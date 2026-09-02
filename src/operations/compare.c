#include "operations/compare.h"

#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/platform.h"
#include "foundation/str_util.h"
#include "store/store.h"
#include "yyjson/yyjson.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    COMPARE_DEFAULT_LIMIT = 200,
    COMPARE_MAX_LIMIT = 1000,
    COMPARE_DEFAULT_SCAN_LIMIT = 2000000,
    COMPARE_MAX_SCAN_LIMIT = 10000000,
};

#define COMPARE_SET_BYTE_BUDGET ((size_t)512U * 1024U)

typedef struct {
    yyjson_mut_val *items;
    size_t returned;
    size_t encoded_bytes;
    bool budget_exhausted;
} compare_result_set_t;

typedef struct {
    const cbm_operation_runtime_t *runtime;
    yyjson_mut_doc *doc;
    size_t limit;
    compare_result_set_t nodes_added;
    compare_result_set_t nodes_removed;
    compare_result_set_t edges_added;
    compare_result_set_t edges_removed;
} compare_response_t;

static bool compare_cancelled(const cbm_operation_runtime_t *runtime) {
    return runtime && runtime->cancelled && runtime->cancelled(runtime->cancelled_context);
}

static char *compare_strdup(const char *text) {
    if (!text) return NULL;
    size_t len = strlen(text);
    char *copy = malloc(len + 1U);
    if (copy) memcpy(copy, text, len + 1U);
    return copy;
}

static bool compare_project_db_file(const char *name) {
    if (!name) return false;
    size_t len = strlen(name);
    return len > 3U && strcmp(name + len - 3U, ".db") == 0 && name[0] != '_' &&
           strncmp(name, ":memory:", strlen(":memory:")) != 0;
}

static bool compare_primary_project_name(cbm_store_t *store, char *out, size_t out_size) {
    cbm_project_t *projects = NULL;
    int count = 0;
    if (!store || !out || out_size == 0U ||
        cbm_store_list_projects(store, &projects, &count) != CBM_STORE_OK) {
        return false;
    }
    int primary = -1;
    int primary_count = 0;
    for (int i = 0; i < count; ++i) {
        if (projects[i].name && projects[i].name[0] && !strstr(projects[i].name, "::")) {
            primary = i;
            ++primary_count;
        }
    }
    bool ok = primary_count == 1;
    if (ok) (void)snprintf(out, out_size, "%s", projects[primary].name);
    cbm_store_free_projects(projects, count);
    return ok;
}

static cbm_store_t *compare_open_project_store(const char *project) {
    if (!project || !cbm_validate_project_name(project)) return NULL;
    const char *cache_dir = cbm_resolve_cache_dir();
    if (!cache_dir) return NULL;

    char path[CBM_SZ_2K];
    if (snprintf(path, sizeof(path), "%s/%s.db", cache_dir, project) < (int)sizeof(path)) {
        cbm_store_t *store = cbm_store_open_path_query(path);
        if (store) {
            cbm_project_t row = {0};
            if (cbm_store_get_project(store, project, &row) == CBM_STORE_OK) {
                cbm_project_free_fields(&row);
                return store;
            }
            cbm_store_close(store);
        }
    }

    cbm_dir_t *dir = cbm_opendir(cache_dir);
    if (!dir) return NULL;
    cbm_store_t *found = NULL;
    cbm_dirent_t *entry = NULL;
    while ((entry = cbm_readdir(dir)) != NULL) {
        if (!compare_project_db_file(entry->name)) continue;
        if (snprintf(path, sizeof(path), "%s/%s", cache_dir, entry->name) >= (int)sizeof(path)) {
            continue;
        }
        cbm_store_t *candidate = cbm_store_open_path_query(path);
        if (!candidate) continue;
        char internal_name[CBM_SZ_1K];
        if (compare_primary_project_name(candidate, internal_name, sizeof(internal_name)) &&
            strcmp(internal_name, project) == 0) {
            found = candidate;
            break;
        }
        cbm_store_close(candidate);
    }
    cbm_closedir(dir);
    return found;
}

static cbm_operation_result_t compare_error(const char *code, const char *message) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) {
        if (doc) yyjson_mut_doc_free(doc);
        return cbm_operation_result_copy("compare_graphs failed: out of memory", true);
    }
    yyjson_mut_doc_set_root(doc, root);
    if (!yyjson_mut_obj_add_strcpy(doc, root, "error", message) ||
        !yyjson_mut_obj_add_strcpy(doc, root, "code", code)) {
        yyjson_mut_doc_free(doc);
        return cbm_operation_result_copy("compare_graphs failed: out of memory", true);
    }
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    if (!json) return cbm_operation_result_copy("compare_graphs failed: out of memory", true);
    return cbm_operation_result_take(json, true);
}

static bool compare_arg_name_allowed(const char *name) {
    return strcmp(name, "base_project") == 0 || strcmp(name, "target_project") == 0 ||
           strcmp(name, "limit") == 0 || strcmp(name, "scan_limit") == 0;
}

static bool compare_parse_bounded_integer(yyjson_val *root, const char *key, int64_t default_value,
                                          int64_t maximum, uint64_t *out,
                                          const char **error_message) {
    yyjson_val *value = yyjson_obj_get(root, key);
    int64_t parsed = default_value;
    if (value) {
        if (!yyjson_is_int(value)) {
            *error_message = "limit values must be integers";
            return false;
        }
        parsed = yyjson_get_int(value);
    }
    if (parsed < 1 || parsed > maximum) {
        *error_message = strcmp(key, "limit") == 0 ? "limit must be between 1 and 1000"
                                                   : "scan_limit must be between 1 and 10000000";
        return false;
    }
    *out = (uint64_t)parsed;
    return true;
}

static bool compare_parse_arguments(const char *args, char **base_project, char **target_project,
                                    uint64_t *limit, uint64_t *scan_limit,
                                    const char **error_message) {
    *base_project = NULL;
    *target_project = NULL;
    const char *json = args ? args : "{}";
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    if (!doc) {
        *error_message = "arguments must be valid JSON";
        return false;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!yyjson_is_obj(root)) {
        *error_message = "arguments must be an object";
        yyjson_doc_free(doc);
        return false;
    }
    yyjson_obj_iter iterator = yyjson_obj_iter_with(root);
    yyjson_val *key = NULL;
    while ((key = yyjson_obj_iter_next(&iterator)) != NULL) {
        const char *name = yyjson_get_str(key);
        if (!name || !compare_arg_name_allowed(name)) {
            *error_message = "unknown argument";
            yyjson_doc_free(doc);
            return false;
        }
    }
    yyjson_val *base = yyjson_obj_get(root, "base_project");
    yyjson_val *target = yyjson_obj_get(root, "target_project");
    if (!base || !yyjson_is_str(base) || yyjson_get_len(base) == 0U || !target ||
        !yyjson_is_str(target) || yyjson_get_len(target) == 0U) {
        *error_message = "base_project and target_project are required non-empty strings";
        yyjson_doc_free(doc);
        return false;
    }
    if (strcmp(yyjson_get_str(base), yyjson_get_str(target)) == 0) {
        *error_message = "base_project and target_project must be distinct";
        yyjson_doc_free(doc);
        return false;
    }
    if (!compare_parse_bounded_integer(root, "limit", COMPARE_DEFAULT_LIMIT, COMPARE_MAX_LIMIT,
                                       limit, error_message) ||
        !compare_parse_bounded_integer(root, "scan_limit", COMPARE_DEFAULT_SCAN_LIMIT,
                                       COMPARE_MAX_SCAN_LIMIT, scan_limit, error_message)) {
        yyjson_doc_free(doc);
        return false;
    }
    *base_project = compare_strdup(yyjson_get_str(base));
    *target_project = compare_strdup(yyjson_get_str(target));
    yyjson_doc_free(doc);
    if (!*base_project || !*target_project) {
        free(*base_project);
        free(*target_project);
        *base_project = NULL;
        *target_project = NULL;
        *error_message = "out of memory while validating arguments";
        return false;
    }
    return true;
}

static bool compare_utf8_cont(unsigned char c) { return (c & 0xC0U) == 0x80U; }

static char *compare_sanitize_utf8_lossy(const char *s) {
    if (!s) return NULL;
    size_t len = strlen(s);
    if (len > (SIZE_MAX - 1U) / 3U) return NULL;
    char *out = malloc(len * 3U + 1U);
    if (!out) return NULL;
    const unsigned char *p = (const unsigned char *)s;
    const unsigned char *end = p + len;
    unsigned char *dst = (unsigned char *)out;
    while (p < end) {
        unsigned char c = *p;
        size_t n = 0U;
        if (c < 0x80U) n = 1U;
        else if (c >= 0xC2U && c <= 0xDFU && p + 1 < end && compare_utf8_cont(p[1])) n = 2U;
        else if (c == 0xE0U && p + 2 < end && p[1] >= 0xA0U && p[1] <= 0xBFU && compare_utf8_cont(p[2])) n = 3U;
        else if (c >= 0xE1U && c <= 0xECU && p + 2 < end && compare_utf8_cont(p[1]) && compare_utf8_cont(p[2])) n = 3U;
        else if (c == 0xEDU && p + 2 < end && p[1] >= 0x80U && p[1] <= 0x9FU && compare_utf8_cont(p[2])) n = 3U;
        else if (c >= 0xEEU && c <= 0xEFU && p + 2 < end && compare_utf8_cont(p[1]) && compare_utf8_cont(p[2])) n = 3U;
        else if (c == 0xF0U && p + 3 < end && p[1] >= 0x90U && p[1] <= 0xBFU && compare_utf8_cont(p[2]) && compare_utf8_cont(p[3])) n = 4U;
        else if (c >= 0xF1U && c <= 0xF3U && p + 3 < end && compare_utf8_cont(p[1]) && compare_utf8_cont(p[2]) && compare_utf8_cont(p[3])) n = 4U;
        else if (c == 0xF4U && p + 3 < end && p[1] >= 0x80U && p[1] <= 0x8FU && compare_utf8_cont(p[2]) && compare_utf8_cont(p[3])) n = 4U;
        if (n > 0U) {
            memcpy(dst, p, n);
            dst += n;
            p += n;
        } else {
            *dst++ = 0xEFU; *dst++ = 0xBFU; *dst++ = 0xBDU; ++p;
        }
    }
    *dst = '\0';
    return out;
}

static bool compare_add_identity_string(yyjson_mut_doc *doc, yyjson_mut_val *object,
                                        const char *key, const char *value) {
    char *sanitized = compare_sanitize_utf8_lossy(value);
    if (!sanitized) return false;
    bool ok = yyjson_mut_obj_add_strcpy(doc, object, key, sanitized);
    free(sanitized);
    return ok;
}

static yyjson_mut_val *compare_node_json(yyjson_mut_doc *doc,
                                         const cbm_graph_node_identity_t *node) {
    yyjson_mut_val *object = yyjson_mut_obj(doc);
    if (!object || !compare_add_identity_string(doc, object, "qualified_name", node->qualified_name) ||
        !compare_add_identity_string(doc, object, "label", node->label) ||
        !compare_add_identity_string(doc, object, "file_path", node->file_path)) return NULL;
    return object;
}

static bool compare_append_item(compare_response_t *response, compare_result_set_t *set,
                                yyjson_mut_doc *item_doc, yyjson_mut_val *item) {
    if (!item_doc || !item) { yyjson_mut_doc_free(item_doc); return false; }
    char *encoded = yyjson_mut_write(item_doc, 0, NULL);
    if (!encoded) { yyjson_mut_doc_free(item_doc); return false; }
    size_t encoded_len = strlen(encoded);
    size_t separator = set->returned > 0U ? 1U : 0U;
    free(encoded);
    if (set->encoded_bytes > COMPARE_SET_BYTE_BUDGET ||
        separator > COMPARE_SET_BYTE_BUDGET - set->encoded_bytes ||
        encoded_len > COMPARE_SET_BYTE_BUDGET - set->encoded_bytes - separator) {
        set->budget_exhausted = true;
        yyjson_mut_doc_free(item_doc);
        return true;
    }
    yyjson_mut_val *copy = yyjson_mut_val_mut_copy(response->doc, item);
    bool ok = copy && yyjson_mut_arr_add_val(set->items, copy);
    yyjson_mut_doc_free(item_doc);
    if (!ok) return false;
    set->encoded_bytes += separator + encoded_len;
    ++set->returned;
    return true;
}

static bool compare_node_callback(void *context, bool added,
                                  const cbm_graph_node_identity_t *node) {
    compare_response_t *response = context;
    compare_result_set_t *set = added ? &response->nodes_added : &response->nodes_removed;
    if (set->returned >= response->limit || set->budget_exhausted) return true;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *item = doc ? compare_node_json(doc, node) : NULL;
    if (doc && item) yyjson_mut_doc_set_root(doc, item);
    return compare_append_item(response, set, doc, item);
}

static bool compare_edge_callback(void *context, bool added,
                                  const cbm_graph_edge_identity_t *edge) {
    compare_response_t *response = context;
    compare_result_set_t *set = added ? &response->edges_added : &response->edges_removed;
    if (set->returned >= response->limit || set->budget_exhausted) return true;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *item = doc ? yyjson_mut_obj(doc) : NULL;
    yyjson_mut_val *source = doc ? compare_node_json(doc, &edge->source) : NULL;
    yyjson_mut_val *target = doc ? compare_node_json(doc, &edge->target) : NULL;
    bool ok = item && source && target && yyjson_mut_obj_add_val(doc, item, "source", source) &&
              yyjson_mut_obj_add_val(doc, item, "target", target) &&
              compare_add_identity_string(doc, item, "type", edge->type) &&
              compare_add_identity_string(doc, item, "local_name_gen", edge->local_name_gen);
    if (ok) yyjson_mut_doc_set_root(doc, item);
    return compare_append_item(response, set, doc, ok ? item : NULL);
}

static bool compare_cancel_callback(void *context) {
    compare_response_t *response = context;
    return compare_cancelled(response->runtime);
}

static yyjson_mut_val *compare_project_json(yyjson_mut_doc *doc, const char *project,
                                            const cbm_graph_compare_project_t *metadata) {
    yyjson_mut_val *object = yyjson_mut_obj(doc);
    if (!object || !yyjson_mut_obj_add_strcpy(doc, object, "project", project) ||
        !compare_add_identity_string(doc, object, "generation", metadata->generation) ||
        !compare_add_identity_string(doc, object, "index_mode", metadata->index_mode) ||
        !yyjson_mut_obj_add_sint(doc, object, "node_count", metadata->node_count) ||
        !yyjson_mut_obj_add_sint(doc, object, "edge_count", metadata->edge_count)) return NULL;
    return object;
}

static yyjson_mut_val *compare_set_json(yyjson_mut_doc *doc, compare_result_set_t *set,
                                        uint64_t total, size_t limit) {
    yyjson_mut_val *object = yyjson_mut_obj(doc);
    yyjson_mut_val *reasons = yyjson_mut_arr(doc);
    bool truncated = total > (uint64_t)set->returned;
    if (!object || !reasons || !yyjson_mut_obj_add_val(doc, object, "items", set->items) ||
        !yyjson_mut_obj_add_uint(doc, object, "returned", set->returned) ||
        !yyjson_mut_obj_add_uint(doc, object, "total", total) ||
        !yyjson_mut_obj_add_bool(doc, object, "truncated", truncated)) return NULL;
    if (truncated && set->returned >= limit && !yyjson_mut_arr_add_strcpy(doc, reasons, "limit")) return NULL;
    if (truncated && set->budget_exhausted && !yyjson_mut_arr_add_strcpy(doc, reasons, "encoded_byte_budget")) return NULL;
    if (!yyjson_mut_obj_add_val(doc, object, "truncation_reasons", reasons)) return NULL;
    return object;
}

cbm_operation_result_t cbm_compare_operation_execute(const char *args_json,
                                                     const cbm_operation_runtime_t *runtime) {
    char *base_project = NULL;
    char *target_project = NULL;
    uint64_t limit = 0U;
    uint64_t scan_limit = 0U;
    const char *argument_error = NULL;
    if (!compare_parse_arguments(args_json, &base_project, &target_project, &limit, &scan_limit,
                                 &argument_error)) {
        return compare_error("invalid_arguments", argument_error);
    }
    if (compare_cancelled(runtime)) {
        free(base_project); free(target_project);
        return compare_error("cancelled", "compare_graphs cancelled for this request");
    }
    cbm_store_t *base_store = compare_open_project_store(base_project);
    if (!base_store) {
        free(base_project); free(target_project);
        return compare_error("project_not_indexed", "base project is not indexed");
    }
    cbm_store_t *target_store = compare_open_project_store(target_project);
    if (!target_store) {
        cbm_store_close(base_store); free(base_project); free(target_project);
        return compare_error("project_not_indexed", "target project is not indexed");
    }
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    compare_response_t response = {
        .runtime = runtime,
        .doc = doc,
        .limit = (size_t)limit,
        .nodes_added = {.items = doc ? yyjson_mut_arr(doc) : NULL, .encoded_bytes = 2U},
        .nodes_removed = {.items = doc ? yyjson_mut_arr(doc) : NULL, .encoded_bytes = 2U},
        .edges_added = {.items = doc ? yyjson_mut_arr(doc) : NULL, .encoded_bytes = 2U},
        .edges_removed = {.items = doc ? yyjson_mut_arr(doc) : NULL, .encoded_bytes = 2U},
    };
    if (!doc || !root || !response.nodes_added.items || !response.nodes_removed.items ||
        !response.edges_added.items || !response.edges_removed.items) {
        cbm_store_close(target_store); cbm_store_close(base_store); yyjson_mut_doc_free(doc);
        free(base_project); free(target_project);
        return compare_error("allocation_failed", "could not allocate comparison result");
    }
    yyjson_mut_doc_set_root(doc, root);
    cbm_graph_compare_result_t comparison = {0};
    int rc = cbm_store_compare_graphs(base_store, base_project, target_store, target_project,
                                      scan_limit, compare_cancel_callback, compare_node_callback,
                                      compare_edge_callback, &response, &comparison);
    cbm_store_close(target_store); cbm_store_close(base_store);
    if (rc != CBM_STORE_OK) {
        yyjson_mut_doc_free(doc); free(base_project); free(target_project);
        if (rc == CBM_STORE_CANCELLED) return compare_error("cancelled", "compare_graphs cancelled for this request");
        if (rc == CBM_STORE_NOT_FOUND) return compare_error("project_not_indexed", "project is not indexed");
        if (rc == CBM_STORE_SCAN_LIMIT) return compare_error("scan_limit_exceeded", "combined graph rows exceed scan_limit");
        if (rc == CBM_STORE_CALLBACK_ERR) return compare_error("allocation_failed", "could not allocate comparison result");
        return compare_error("query_failed", "graph comparison query failed");
    }
    yyjson_mut_val *base = compare_project_json(doc, base_project, &comparison.base);
    yyjson_mut_val *target = compare_project_json(doc, target_project, &comparison.target);
    yyjson_mut_val *nodes = yyjson_mut_obj(doc);
    yyjson_mut_val *edges = yyjson_mut_obj(doc);
    yyjson_mut_val *limits = yyjson_mut_obj(doc);
    yyjson_mut_val *nodes_added = compare_set_json(doc, &response.nodes_added, comparison.nodes_added_total, response.limit);
    yyjson_mut_val *nodes_removed = compare_set_json(doc, &response.nodes_removed, comparison.nodes_removed_total, response.limit);
    yyjson_mut_val *edges_added = compare_set_json(doc, &response.edges_added, comparison.edges_added_total, response.limit);
    yyjson_mut_val *edges_removed = compare_set_json(doc, &response.edges_removed, comparison.edges_removed_total, response.limit);
    bool built = base && target && nodes && edges && limits && nodes_added && nodes_removed && edges_added && edges_removed &&
        yyjson_mut_obj_add_int(doc, root, "schema_version", 1) &&
        yyjson_mut_obj_add_val(doc, root, "base", base) && yyjson_mut_obj_add_val(doc, root, "target", target) &&
        yyjson_mut_obj_add_val(doc, nodes, "added", nodes_added) && yyjson_mut_obj_add_val(doc, nodes, "removed", nodes_removed) &&
        yyjson_mut_obj_add_val(doc, root, "nodes", nodes) && yyjson_mut_obj_add_val(doc, edges, "added", edges_added) &&
        yyjson_mut_obj_add_val(doc, edges, "removed", edges_removed) && yyjson_mut_obj_add_val(doc, root, "edges", edges) &&
        yyjson_mut_obj_add_uint(doc, limits, "limit", limit) && yyjson_mut_obj_add_uint(doc, limits, "scan_limit", scan_limit) &&
        yyjson_mut_obj_add_uint(doc, limits, "encoded_byte_budget", COMPARE_SET_BYTE_BUDGET) &&
        yyjson_mut_obj_add_val(doc, root, "limits", limits);
    free(base_project); free(target_project);
    if (!built) { yyjson_mut_doc_free(doc); return compare_error("allocation_failed", "could not allocate comparison result"); }
    if (compare_cancelled(runtime)) { yyjson_mut_doc_free(doc); return compare_error("cancelled", "compare_graphs cancelled for this request"); }
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    if (!json) return compare_error("allocation_failed", "could not serialize comparison result");
    return cbm_operation_result_take(json, false);
}
