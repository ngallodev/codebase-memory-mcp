#include "operations/operation.h"

#include "foundation/compat_fs.h"
#include "foundation/workspace.h"
#include "store/store.h"
#include "yyjson/yyjson.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    SNIPPET_NEIGHBOR_LIMIT = 20,
    SNIPPET_RES_RANK_CALLABLE = 2,
    SNIPPET_RES_RANK_OTHER = 1,
    SNIPPET_RES_RANK_MODULE = 0,
    SNIPPET_RES_LABEL_WEIGHT = 1000000,
};

static char *dup_text(const char *text) {
    if (!text) return NULL;
    size_t len = strlen(text);
    char *copy = malloc(len + 1U);
    if (copy) memcpy(copy, text, len + 1U);
    return copy;
}

static char *string_arg(const char *args, const char *name) {
    yyjson_doc *doc = args ? yyjson_read(args, strlen(args), 0) : NULL;
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    char *copy = value && yyjson_is_str(value) ? dup_text(yyjson_get_str(value)) : NULL;
    if (doc) yyjson_doc_free(doc);
    return copy;
}

static bool bool_arg(const char *args, const char *name) {
    yyjson_doc *doc = args ? yyjson_read(args, strlen(args), 0) : NULL;
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    bool result = value && yyjson_is_bool(value) && yyjson_get_bool(value);
    if (doc) yyjson_doc_free(doc);
    return result;
}

static cbm_operation_result_t json_result(yyjson_mut_doc *doc, bool error) {
    if (!doc) return cbm_operation_result_copy("{\"error\":\"result allocation failed\"}", true);
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json ? cbm_operation_result_take(json, error)
                : cbm_operation_result_copy("{\"error\":\"result encoding failed\"}", true);
}

static cbm_operation_result_t error_result(const char *message, const char *hint) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) {
        if (doc) yyjson_mut_doc_free(doc);
        return cbm_operation_result_copy(message ? message : "snippet failed", true);
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "error", message ? message : "snippet failed");
    if (hint) yyjson_mut_obj_add_strcpy(doc, root, "hint", hint);
    return json_result(doc, true);
}

static long resolution_score(const cbm_node_t *node) {
    long rank = SNIPPET_RES_RANK_MODULE;
    if (node->label) {
        if (strcmp(node->label, "Function") == 0 || strcmp(node->label, "Method") == 0) {
            rank = SNIPPET_RES_RANK_CALLABLE;
        } else if (strcmp(node->label, "Module") != 0 && strcmp(node->label, "File") != 0) {
            rank = SNIPPET_RES_RANK_OTHER;
        }
    }
    long span = (long)node->end_line - (long)node->start_line;
    if (span < 0) span = 0;
    return rank * SNIPPET_RES_LABEL_WEIGHT + span;
}

static bool real_callable(const cbm_node_t *node) {
    return node->label &&
           (strcmp(node->label, "Function") == 0 || strcmp(node->label, "Method") == 0) &&
           node->end_line > node->start_line;
}

static int resolved_node(const cbm_node_t *nodes, int count, bool *ambiguous) {
    *ambiguous = false;
    if (count <= 1) return 0;
    int best = 0;
    long score = resolution_score(&nodes[0]);
    for (int i = 1; i < count; ++i) {
        long candidate = resolution_score(&nodes[i]);
        if (candidate > score) {
            score = candidate;
            best = i;
        }
    }
    int top_count = 0;
    int real_count = 0;
    for (int i = 0; i < count; ++i) {
        if (resolution_score(&nodes[i]) == score) ++top_count;
        if (real_callable(&nodes[i])) ++real_count;
    }
    *ambiguous = top_count > 1 || real_count > 1;
    return best;
}

static char *read_lines(const char *path, int start_line, int end_line) {
    FILE *file = cbm_fopen(path, "rb");
    if (!file) return NULL;
    size_t capacity = 4096U;
    size_t length = 0U;
    char *buffer = malloc(capacity);
    if (!buffer) {
        fclose(file);
        return NULL;
    }
    buffer[0] = '\0';
    char line[2048];
    int line_number = 0;
    while (fgets(line, sizeof(line), file)) {
        ++line_number;
        if (line_number < start_line) continue;
        if (line_number > end_line) break;
        size_t line_length = strlen(line);
        if (length + line_length + 1U > capacity) {
            size_t next = capacity;
            while (length + line_length + 1U > next) next *= 2U;
            char *grown = realloc(buffer, next);
            if (!grown) {
                free(buffer);
                fclose(file);
                return NULL;
            }
            buffer = grown;
            capacity = next;
        }
        memcpy(buffer + length, line, line_length);
        length += line_length;
        buffer[length] = '\0';
    }
    fclose(file);
    if (length == 0U) {
        free(buffer);
        return NULL;
    }
    return buffer;
}

static cbm_operation_result_t ambiguous_result(const char *input, const cbm_node_t *nodes, int count) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    yyjson_mut_val *suggestions = doc ? yyjson_mut_arr(doc) : NULL;
    if (!doc || !root || !suggestions) {
        if (doc) yyjson_mut_doc_free(doc);
        return error_result("result allocation failed", NULL);
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", "ambiguous");
    yyjson_mut_obj_add_strcpy(doc, root, "input", input ? input : "");
    for (int i = 0; i < count; ++i) {
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, item, "qualified_name",
                                  nodes[i].qualified_name ? nodes[i].qualified_name : "");
        yyjson_mut_obj_add_strcpy(doc, item, "name", nodes[i].name ? nodes[i].name : "");
        yyjson_mut_obj_add_strcpy(doc, item, "label", nodes[i].label ? nodes[i].label : "");
        yyjson_mut_obj_add_strcpy(doc, item, "file_path",
                                  nodes[i].file_path ? nodes[i].file_path : "");
        yyjson_mut_arr_add_val(suggestions, item);
    }
    yyjson_mut_obj_add_val(doc, root, "suggestions", suggestions);
    yyjson_mut_obj_add_str(doc, root, "hint",
                           "Choose an exact qualified_name from suggestions, or narrow with search.");
    return json_result(doc, false);
}

static cbm_operation_result_t node_result(cbm_store_t *store, const char *project,
                                          const cbm_node_t *node, const char *match,
                                          bool include_neighbors) {
    cbm_project_t project_info = {0};
    if (cbm_store_get_project(store, project, &project_info) != CBM_STORE_OK ||
        !project_info.root_path || !node->file_path) {
        cbm_project_free_fields(&project_info);
        return error_result("indexed source location is unavailable", NULL);
    }
    char abs_path[4096];
    int n = snprintf(abs_path, sizeof(abs_path), "%s%s%s", project_info.root_path,
                     project_info.root_path[strlen(project_info.root_path) - 1U] == '/' ? "" : "/",
                     node->file_path);
    if (n < 0 || (size_t)n >= sizeof(abs_path) ||
        !cbm_path_within_root(project_info.root_path, abs_path)) {
        cbm_project_free_fields(&project_info);
        return error_result("indexed source path escapes project root", NULL);
    }
    int start_line = node->start_line > 0 ? node->start_line : 1;
    int end_line = node->end_line >= start_line ? node->end_line : start_line;
    char *source = read_lines(abs_path, start_line, end_line);
    if (!source) {
        cbm_project_free_fields(&project_info);
        return error_result("source file could not be read",
                            "The index may be stale. Re-index or inspect the file directly.");
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) {
        if (doc) yyjson_mut_doc_free(doc);
        free(source);
        cbm_project_free_fields(&project_info);
        return error_result("result allocation failed", NULL);
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "project", project);
    yyjson_mut_obj_add_strcpy(doc, root, "qualified_name",
                              node->qualified_name ? node->qualified_name : "");
    yyjson_mut_obj_add_strcpy(doc, root, "name", node->name ? node->name : "");
    yyjson_mut_obj_add_strcpy(doc, root, "label", node->label ? node->label : "");
    yyjson_mut_obj_add_strcpy(doc, root, "file_path", node->file_path ? node->file_path : "");
    yyjson_mut_obj_add_int(doc, root, "start_line", start_line);
    yyjson_mut_obj_add_int(doc, root, "end_line", end_line);
    yyjson_mut_obj_add_strcpy(doc, root, "source", source);
    if (match) yyjson_mut_obj_add_strcpy(doc, root, "match", match);

    /* Preserve the coverage warning promised by the public snippet contract.
     * The graph may contain a callable from a file whose parse included
     * ERROR/MISSING regions; callers need that signal before treating the
     * snippet as complete evidence. */
    cbm_coverage_row_t *coverage_rows = NULL;
    int coverage_count = 0;
    if (cbm_store_coverage_get_path(store, project, node->file_path, &coverage_rows,
                                    &coverage_count) == CBM_STORE_OK) {
        for (int i = 0; i < coverage_count; ++i) {
            if (coverage_rows[i].kind && strcmp(coverage_rows[i].kind, "parse_partial") == 0) {
                yyjson_mut_obj_add_str(
                    doc, root, "coverage_note",
                    "This file was only PARTIALLY indexed; read the source directly when graph "
                    "coverage matters for completeness.");
                break;
            }
        }
    }
    cbm_store_free_coverage(coverage_rows, coverage_count);

    if (include_neighbors) {
        char **callers = NULL;
        char **callees = NULL;
        int caller_count = 0;
        int callee_count = 0;
        if (cbm_store_node_neighbor_names(store, node->id, SNIPPET_NEIGHBOR_LIMIT, &callers,
                                          &caller_count, &callees, &callee_count) == CBM_STORE_OK) {
            yyjson_mut_val *caller_values = yyjson_mut_arr(doc);
            yyjson_mut_val *callee_values = yyjson_mut_arr(doc);
            for (int i = 0; i < caller_count; ++i) {
                yyjson_mut_arr_add_strcpy(doc, caller_values, callers[i] ? callers[i] : "");
                free(callers[i]);
            }
            for (int i = 0; i < callee_count; ++i) {
                yyjson_mut_arr_add_strcpy(doc, callee_values, callees[i] ? callees[i] : "");
                free(callees[i]);
            }
            free(callers);
            free(callees);
            yyjson_mut_obj_add_val(doc, root, "callers", caller_values);
            yyjson_mut_obj_add_val(doc, root, "callees", callee_values);
        }
    }

    free(source);
    cbm_project_free_fields(&project_info);
    return json_result(doc, false);
}

cbm_operation_result_t cbm_snippet_operation_execute(const char *args) {
    char *project = string_arg(args, "project");
    char *qualified_name = string_arg(args, "qualified_name");
    bool include_neighbors = bool_arg(args, "include_neighbors");
    if (!project || !project[0]) {
        free(project);
        free(qualified_name);
        return error_result("project is required", "Run the command from an indexed repository.");
    }
    if (!qualified_name || !qualified_name[0]) {
        free(project);
        free(qualified_name);
        return error_result("qualified_name is required", "Use search first to discover a symbol.");
    }
    cbm_store_t *store = cbm_store_open(project);
    if (!store) {
        free(project);
        free(qualified_name);
        return error_result("project not indexed", "Run 'codebase-memory-cli index .' first.");
    }

    cbm_node_t exact = {0};
    if (cbm_store_find_node_by_qn(store, project, qualified_name, &exact) == CBM_STORE_OK) {
        cbm_operation_result_t result = node_result(store, project, &exact, NULL, include_neighbors);
        cbm_node_free_fields(&exact);
        cbm_store_close(store);
        free(project);
        free(qualified_name);
        return result;
    }

    cbm_node_t *matches = NULL;
    int count = 0;
    (void)cbm_store_find_nodes_by_qn_suffix(store, project, qualified_name, &matches, &count);
    if (count > 0) {
        bool ambiguous = false;
        int selected = resolved_node(matches, count, &ambiguous);
        cbm_operation_result_t result = ambiguous
                                            ? ambiguous_result(qualified_name, matches, count)
                                            : node_result(store, project, &matches[selected], "suffix",
                                                          include_neighbors);
        cbm_store_free_nodes(matches, count);
        cbm_store_close(store);
        free(project);
        free(qualified_name);
        return result;
    }
    cbm_store_free_nodes(matches, count);
    cbm_store_close(store);
    free(project);
    free(qualified_name);
    return error_result("symbol not found",
                        "Use 'codebase-memory-cli search <term>' first, then pass an exact qualified_name.");
}
