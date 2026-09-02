#include "operations/read.h"
#include "operations/coverage.h"
#include "operations/snippet.h"
#include "operations/search.h"
#include "operations/trace.h"
#include "operations/trace_ingest.h"
#include "operations/schema.h"
#include "operations/query.h"
#include "operations/architecture.h"
#include "operations/changes.h"
#include "operations/source_search.h"
#include "operations/file_outline.h"
#include "operations/compare.h"

#include "foundation/platform.h"
#include "foundation/compat_fs.h"
#include "git/git_context.h"
#include "store/store.h"
#include "yyjson/yyjson.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define OP_DB_EXT_LEN 3U
#define OP_COVERAGE_FILE_CAP 500

static char *copy_string(const char *text);

static int json_int_arg(const char *args_json, const char *name, int fallback) {
    yyjson_doc *doc = args_json ? yyjson_read(args_json, strlen(args_json), 0) : NULL;
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    int result = value && yyjson_is_int(value) ? (int)yyjson_get_sint(value) : fallback;
    if (doc) {
        yyjson_doc_free(doc);
    }
    return result;
}

static bool json_bool_arg(const char *args_json, const char *name, bool fallback) {
    yyjson_doc *doc = args_json ? yyjson_read(args_json, strlen(args_json), 0) : NULL;
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    bool result = value && yyjson_is_bool(value) ? yyjson_get_bool(value) : fallback;
    if (doc) {
        yyjson_doc_free(doc);
    }
    return result;
}

static char *json_string_arg(const char *args_json, const char *name) {
    yyjson_doc *doc = args_json ? yyjson_read(args_json, strlen(args_json), 0) : NULL;
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    const char *text = value && yyjson_is_str(value) ? yyjson_get_str(value) : NULL;
    char *copy = copy_string(text);
    if (doc) {
        yyjson_doc_free(doc);
    }
    return copy;
}

static cbm_operation_result_t json_doc_result(yyjson_mut_doc *doc, bool is_error) {
    if (!doc) {
        return cbm_operation_result_copy("{\"error\":\"result allocation failed\"}", true);
    }
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    if (!json) {
        return cbm_operation_result_copy("{\"error\":\"result encoding failed\"}", true);
    }
    return cbm_operation_result_take(json, is_error);
}

static cbm_operation_result_t json_error(const char *message, const char *hint) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) {
        if (doc) {
            yyjson_mut_doc_free(doc);
        }
        return cbm_operation_result_copy(message ? message : "operation failed", true);
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "error", message ? message : "operation failed");
    if (hint) {
        yyjson_mut_obj_add_strcpy(doc, root, "hint", hint);
    }
    return json_doc_result(doc, true);
}

static bool project_db_file(const char *name) {
    if (!name) {
        return false;
    }
    size_t len = strlen(name);
    if (len <= OP_DB_EXT_LEN || strcmp(name + len - OP_DB_EXT_LEN, ".db") != 0) {
        return false;
    }
    return name[0] != '_' && strncmp(name, ":memory:", strlen(":memory:")) != 0;
}

static char *copy_string(const char *text) {
    if (!text) {
        return NULL;
    }
    size_t len = strlen(text);
    char *copy = malloc(len + 1U);
    if (copy) {
        memcpy(copy, text, len + 1U);
    }
    return copy;
}

static int string_ptr_cmp(const void *left, const void *right) {
    const char *const *a = left;
    const char *const *b = right;
    return strcmp(*a, *b);
}

static bool primary_project_name(cbm_store_t *store, char *out, size_t out_size) {
    cbm_project_t *projects = NULL;
    int count = 0;
    if (!store || !out || out_size == 0 || cbm_store_list_projects(store, &projects, &count) != CBM_STORE_OK) {
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
    if (ok) {
        (void)snprintf(out, out_size, "%s", projects[primary].name);
    }
    cbm_store_free_projects(projects, count);
    return ok;
}

static void add_project_entry(yyjson_mut_doc *doc, yyjson_mut_val *array, const char *cache_dir,
                              const char *db_name, bool include_details) {
    char db_path[4096];
    if (snprintf(db_path, sizeof(db_path), "%s/%s", cache_dir, db_name) >= (int)sizeof(db_path)) {
        return;
    }
    cbm_store_t *store = cbm_store_open_path_query(db_path);
    if (!store) {
        return;
    }
    char project_name[1024];
    if (!primary_project_name(store, project_name, sizeof(project_name))) {
        cbm_store_close(store);
        return;
    }

    cbm_project_t project = {0};
    char root_path[4096] = "";
    if (cbm_store_get_project(store, project_name, &project) == CBM_STORE_OK) {
        if (project.root_path) {
            (void)snprintf(root_path, sizeof(root_path), "%s", project.root_path);
        }
        cbm_project_free_fields(&project);
    }

    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_strcpy(doc, item, "name", project_name);
    yyjson_mut_obj_add_strcpy(doc, item, "root_path", root_path);
    if (include_details) {
        cbm_git_context_t git = {0};
        if (root_path[0] && cbm_git_context_resolve(root_path, &git) && git.is_git && git.branch) {
            yyjson_mut_obj_add_strcpy(doc, item, "branch", git.branch);
        }
        cbm_git_context_free(&git);
        yyjson_mut_obj_add_int(doc, item, "nodes", cbm_store_count_nodes(store, project_name));
        yyjson_mut_obj_add_int(doc, item, "edges", cbm_store_count_edges(store, project_name));
        int64_t size = cbm_file_size(db_path);
        yyjson_mut_obj_add_int(doc, item, "size_bytes", size < 0 ? 0 : size);
    }
    yyjson_mut_arr_add_val(array, item);
    cbm_store_close(store);
}

static cbm_operation_result_t execute_projects(const char *args_json) {
    int offset = json_int_arg(args_json, "offset", 0);
    int limit = json_int_arg(args_json, "limit", 50);
    bool include_details = json_bool_arg(args_json, "include_details", false);
    if (json_bool_arg(args_json, "metadata_only", false)) {
        include_details = false;
    }
    if (offset < 0) {
        offset = 0;
    }
    if (limit < 1) {
        limit = 1;
    } else if (limit > 100) {
        limit = 100;
    }

    const char *cache_dir = cbm_resolve_cache_dir();
    cbm_dir_t *dir = cache_dir ? cbm_opendir(cache_dir) : NULL;
    if (!dir) {
        return json_error("cannot read cache directory",
                          "Check directory permissions or run 'codebase-memory-cli index .' first.");
    }

    char **names = NULL;
    size_t count = 0;
    size_t capacity = 0;
    bool oom = false;
    cbm_dirent_t *entry = NULL;
    while ((entry = cbm_readdir(dir)) != NULL) {
        if (!project_db_file(entry->name)) {
            continue;
        }
        if (count == capacity) {
            size_t next = capacity ? capacity * 2U : 32U;
            char **grown = realloc(names, next * sizeof(*names));
            if (!grown) {
                oom = true;
                break;
            }
            names = grown;
            capacity = next;
        }
        names[count] = copy_string(entry->name);
        if (!names[count]) {
            oom = true;
            break;
        }
        ++count;
    }
    cbm_closedir(dir);
    if (oom) {
        for (size_t i = 0; i < count; ++i) {
            free(names[i]);
        }
        free(names);
        return json_error("out of memory while collecting indexed projects", NULL);
    }
    if (count > 1) {
        qsort(names, count, sizeof(*names), string_ptr_cmp);
    }

    size_t start = (size_t)offset < count ? (size_t)offset : count;
    size_t end = start + (size_t)limit;
    if (end > count) {
        end = count;
    }
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    yyjson_mut_val *projects = doc ? yyjson_mut_arr(doc) : NULL;
    if (!doc || !root || !projects) {
        for (size_t i = 0; i < count; ++i) {
            free(names[i]);
        }
        free(names);
        if (doc) {
            yyjson_mut_doc_free(doc);
        }
        return json_error("result allocation failed", NULL);
    }
    yyjson_mut_doc_set_root(doc, root);
    for (size_t i = start; i < end; ++i) {
        add_project_entry(doc, projects, cache_dir, names[i], include_details);
    }
    for (size_t i = 0; i < count; ++i) {
        free(names[i]);
    }
    free(names);

    yyjson_mut_obj_add_val(doc, root, "projects", projects);
    yyjson_mut_obj_add_uint(doc, root, "total", count);
    yyjson_mut_obj_add_int(doc, root, "offset", offset);
    yyjson_mut_obj_add_int(doc, root, "limit", limit);
    yyjson_mut_obj_add_uint(doc, root, "returned", yyjson_mut_arr_size(projects));
    yyjson_mut_obj_add_bool(doc, root, "has_more", end < count);
    if (yyjson_mut_arr_size(projects) == 0) {
        yyjson_mut_obj_add_str(doc, root, "hint",
                               "No projects indexed. Run 'codebase-memory-cli index .' first.");
    }
    return json_doc_result(doc, false);
}


static void add_status_coverage(yyjson_mut_doc *doc, yyjson_mut_val *root, cbm_store_t *store,
                                const char *project) {
    cbm_coverage_row_t *rows = NULL;
    int count = 0;
    (void)cbm_store_coverage_get(store, project, &rows, &count);

    yyjson_mut_val *partial_files = yyjson_mut_arr(doc);
    yyjson_mut_val *skipped_files = yyjson_mut_arr(doc);
    yyjson_mut_val *excluded_dirs = yyjson_mut_arr(doc);
    yyjson_mut_val *excluded_files = yyjson_mut_arr(doc);
    int partial_count = 0;
    int skipped_count = 0;
    int excluded_dir_count = 0;
    int excluded_file_count = 0;
    for (int i = 0; i < count; ++i) {
        const char *kind = rows[i].kind ? rows[i].kind : "";
        if (strcmp(kind, "parse_partial") == 0) {
            if (partial_count < OP_COVERAGE_FILE_CAP) {
                yyjson_mut_val *entry = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, entry, "path", rows[i].rel_path ? rows[i].rel_path : "");
                yyjson_mut_obj_add_strcpy(doc, entry, "error_ranges",
                                          rows[i].detail ? rows[i].detail : "");
                yyjson_mut_arr_add_val(partial_files, entry);
            }
            ++partial_count;
        } else if (strcmp(kind, "not_indexed_dir") == 0) {
            if (excluded_dir_count < OP_COVERAGE_FILE_CAP) {
                yyjson_mut_arr_add_strcpy(doc, excluded_dirs, rows[i].rel_path ? rows[i].rel_path : "");
            }
            ++excluded_dir_count;
        } else if (strcmp(kind, "not_indexed_file") == 0) {
            if (excluded_file_count < OP_COVERAGE_FILE_CAP) {
                yyjson_mut_val *entry = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, entry, "path", rows[i].rel_path ? rows[i].rel_path : "");
                yyjson_mut_obj_add_strcpy(doc, entry, "reason", rows[i].detail ? rows[i].detail : "");
                yyjson_mut_arr_add_val(excluded_files, entry);
            }
            ++excluded_file_count;
        } else {
            if (skipped_count < OP_COVERAGE_FILE_CAP) {
                yyjson_mut_val *entry = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, entry, "path", rows[i].rel_path ? rows[i].rel_path : "");
                yyjson_mut_obj_add_strcpy(doc, entry, "reason", rows[i].detail ? rows[i].detail : "");
                yyjson_mut_obj_add_strcpy(doc, entry, "phase", kind);
                yyjson_mut_arr_add_val(skipped_files, entry);
            }
            ++skipped_count;
        }
    }
    cbm_store_free_coverage(rows, count);

    yyjson_mut_val *partial = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, partial, "files", partial_files);
    yyjson_mut_obj_add_int(doc, partial, "count", partial_count);
    yyjson_mut_obj_add_bool(doc, partial, "truncated", partial_count > OP_COVERAGE_FILE_CAP);
    yyjson_mut_obj_add_val(doc, root, "parse_partial", partial);

    yyjson_mut_val *skipped = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, skipped, "files", skipped_files);
    yyjson_mut_obj_add_int(doc, skipped, "count", skipped_count);
    yyjson_mut_obj_add_bool(doc, skipped, "truncated", skipped_count > OP_COVERAGE_FILE_CAP);
    yyjson_mut_obj_add_val(doc, root, "skipped", skipped);

    yyjson_mut_val *excluded = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, excluded, "dirs", excluded_dirs);
    yyjson_mut_obj_add_int(doc, excluded, "dirs_count", excluded_dir_count);
    yyjson_mut_obj_add_val(doc, excluded, "files", excluded_files);
    yyjson_mut_obj_add_int(doc, excluded, "files_count", excluded_file_count);
    yyjson_mut_obj_add_bool(doc, excluded, "truncated",
                            excluded_dir_count > OP_COVERAGE_FILE_CAP ||
                                excluded_file_count > OP_COVERAGE_FILE_CAP);
    if (excluded_dir_count > 0 || excluded_file_count > 0) {
        yyjson_mut_obj_add_str(doc, excluded, "note",
                               "Purposely not indexed — excluded by ignore rules. Change the "
                               "ignore rules and re-index to include them.");
    }
    yyjson_mut_obj_add_val(doc, root, "not_indexed", excluded);

    if (partial_count > 0 || skipped_count > 0) {
        yyjson_mut_obj_add_str(doc, root, "coverage_note",
                               "Best-effort signal, not a completeness guarantee. Read flagged "
                               "source directly when graph coverage is partial or skipped.");
    }
}

static cbm_operation_result_t execute_status(const char *args_json) {
    char *project = json_string_arg(args_json, "project");
    if (!project || !project[0]) {
        free(project);
        yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
        yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
        if (!doc || !root) {
            if (doc) yyjson_mut_doc_free(doc);
            return json_error("result allocation failed", NULL);
        }
        yyjson_mut_doc_set_root(doc, root);
        yyjson_mut_obj_add_str(doc, root, "status", "no_project");
        return json_doc_result(doc, false);
    }

    cbm_store_t *store = cbm_store_open(project);
    if (!store) {
        cbm_operation_result_t error = json_error(
            "project not indexed",
            "Run 'codebase-memory-cli index .' in the repository or specify an indexed --project.");
        free(project);
        return error;
    }
    int nodes = cbm_store_count_nodes(store, project);
    int edges = cbm_store_count_edges(store, project);
    bool verbose = json_bool_arg(args_json, "verbose", false);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) {
        if (doc) yyjson_mut_doc_free(doc);
        cbm_store_close(store);
        free(project);
        return json_error("result allocation failed", NULL);
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "project", project);
    yyjson_mut_obj_add_int(doc, root, "nodes", nodes);
    yyjson_mut_obj_add_int(doc, root, "edges", edges);
    yyjson_mut_obj_add_str(doc, root, "status", nodes > 0 ? "ready" : "empty");

    cbm_project_t info = {0};
    if (cbm_store_get_project(store, project, &info) == CBM_STORE_OK) {
        const char *root_path = info.root_path ? info.root_path : "";
        yyjson_mut_obj_add_strcpy(doc, root, "root_path", root_path);
        if (verbose && root_path[0]) {
            cbm_git_context_t git = {0};
            if (cbm_git_context_resolve(root_path, &git) && git.is_git) {
                yyjson_mut_val *git_obj = yyjson_mut_obj(doc);
                if (git.branch) yyjson_mut_obj_add_strcpy(doc, git_obj, "branch", git.branch);
                if (git.head_sha) yyjson_mut_obj_add_strcpy(doc, git_obj, "head_sha", git.head_sha);
                yyjson_mut_obj_add_val(doc, root, "git", git_obj);
            }
            cbm_git_context_free(&git);
        }
        cbm_project_free_fields(&info);
    }
    add_status_coverage(doc, root, store, project);
    if (nodes == 0) {
        yyjson_mut_obj_add_str(doc, root, "hint",
                               "Project is empty. Re-run 'codebase-memory-cli index .' to populate.");
    }
    cbm_store_close(store);
    free(project);
    return json_doc_result(doc, false);
}

bool cbm_read_operation_supported(cbm_operation_id_t operation) {
    return operation == CBM_OPERATION_PROJECTS || operation == CBM_OPERATION_STATUS ||
           operation == CBM_OPERATION_COVERAGE || operation == CBM_OPERATION_SEARCH ||
           operation == CBM_OPERATION_SNIPPET || operation == CBM_OPERATION_TRACE ||
           operation == CBM_OPERATION_SCHEMA || operation == CBM_OPERATION_QUERY ||
           operation == CBM_OPERATION_ARCHITECTURE || operation == CBM_OPERATION_CHANGES ||
           operation == CBM_OPERATION_SOURCE_SEARCH || operation == CBM_OPERATION_FILE_OUTLINE ||
           operation == CBM_OPERATION_COMPARE || operation == CBM_OPERATION_INGEST_TRACES;
}

cbm_operation_result_t cbm_read_operation_execute(cbm_operation_id_t operation,
                                                  const char *args_json,
                                                  const cbm_operation_runtime_t *runtime) {
    (void)runtime;
    if (operation == CBM_OPERATION_PROJECTS) {
        return execute_projects(args_json);
    }
    if (operation == CBM_OPERATION_STATUS) {
        return execute_status(args_json);
    }
    if (operation == CBM_OPERATION_COVERAGE) {
        return cbm_coverage_operation_execute(args_json);
    }
    if (operation == CBM_OPERATION_SEARCH) {
        return cbm_search_operation_execute(args_json);
    }
    if (operation == CBM_OPERATION_SNIPPET) {
        return cbm_snippet_operation_execute(args_json);
    }
    if (operation == CBM_OPERATION_TRACE) {
        return cbm_trace_operation_execute(args_json);
    }
    if (operation == CBM_OPERATION_SCHEMA) {
        return cbm_schema_operation_execute(args_json);
    }
    if (operation == CBM_OPERATION_QUERY) {
        return cbm_query_operation_execute(args_json);
    }
    if (operation == CBM_OPERATION_ARCHITECTURE) {
        return cbm_architecture_operation_execute(args_json);
    }
    if (operation == CBM_OPERATION_CHANGES) {
        return cbm_changes_operation_execute(args_json, runtime);
    }
    if (operation == CBM_OPERATION_SOURCE_SEARCH) {
        return cbm_source_search_operation_execute(args_json, runtime);
    }
    if (operation == CBM_OPERATION_FILE_OUTLINE) {
        return cbm_file_outline_operation_execute(args_json, runtime);
    }
    if (operation == CBM_OPERATION_COMPARE) {
        return cbm_compare_operation_execute(args_json, runtime);
    }
    return cbm_operation_result_copy("native read operation not implemented", true);
}
