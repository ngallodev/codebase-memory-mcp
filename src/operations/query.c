#include "operations/query.h"

#include "cypher/cypher.h"
#include "store/store.h"
#include "operations/compact_out.h"
#include "yyjson/yyjson.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

static char *copy_string(const char *text) {
    if (!text) return NULL;
    size_t len = strlen(text);
    char *copy = malloc(len + 1U);
    if (copy) memcpy(copy, text, len + 1U);
    return copy;
}

static yyjson_doc *read_args(const char *args_json) {
    return yyjson_read(args_json ? args_json : "{}", strlen(args_json ? args_json : "{}"), 0);
}

static char *string_arg(const char *args_json, const char *name) {
    yyjson_doc *doc = read_args(args_json);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    char *result = value && yyjson_is_str(value) ? copy_string(yyjson_get_str(value)) : NULL;
    if (doc) yyjson_doc_free(doc);
    return result;
}

static char *project_arg(const char *args_json) {
    static const char *const names[] = {"project", "project_name", "project_id", "projectName"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i) {
        char *value = string_arg(args_json, names[i]);
        if (value) return value;
    }
    return NULL;
}

static int int_arg(const char *args_json, const char *name, int fallback) {
    yyjson_doc *doc = read_args(args_json);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    int result = value && yyjson_is_int(value) ? (int)yyjson_get_sint(value) : fallback;
    if (doc) yyjson_doc_free(doc);
    return result;
}

static cbm_operation_result_t error_result(const char *message) {
    return cbm_operation_result_copy(message ? message : "query execution failed", true);
}

cbm_operation_result_t cbm_query_operation_execute(const char *args_json) {
    char *query = string_arg(args_json, "query");
    char *project = project_arg(args_json);
    int max_rows = int_arg(args_json, "max_rows", 0);
    char *graph = string_arg(args_json, "graph");
    bool missed_graph = graph && strcmp(graph, "missed") == 0;
    free(graph);

    if (!query) {
        free(project);
        return error_result("query is required");
    }
    if (missed_graph && !project) {
        free(query);
        return error_result("project is required when graph=\"missed\"");
    }
    if (!project || !project[0]) {
        free(query);
        free(project);
        return error_result("project is required");
    }

    cbm_store_t *store = cbm_store_open(project);
    if (!store) {
        free(query);
        free(project);
        return error_result("project not found or not indexed");
    }
    if (cbm_store_count_nodes(store, project) <= 0) {
        cbm_store_close(store);
        free(query);
        free(project);
        return error_result("project not indexed or index is empty");
    }

    char coverage_project[512];
    const char *cypher_project = project;
    if (missed_graph) {
        cbm_store_coverage_shadow_project(coverage_project, sizeof(coverage_project), project);
        cypher_project = coverage_project;
    }

    cbm_cypher_result_t result = {0};
    int rc = cbm_cypher_execute(store, query, cypher_project, max_rows, &result);
    if (rc < 0) {
        cbm_operation_result_t error = error_result(result.error ? result.error : "query execution failed");
        cbm_cypher_result_free(&result);
        cbm_store_close(store);
        free(query);
        free(project);
        return error;
    }

    char *format = string_arg(args_json, "format");
    bool legacy_json = format && strcmp(format, "json") == 0;
    free(format);

    char *payload = NULL;
    if (!legacy_json) {
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        cbm_tree_table_header(&sb, "rows", result.row_count, (const char *const *)result.columns,
                              result.col_count);
        for (int r = 0; r < result.row_count; ++r) {
            cbm_tree_row_begin(&sb);
            for (int c = 0; c < result.col_count; ++c) {
                cbm_tree_cell_str(&sb, result.rows[r][c], c == 0);
            }
            cbm_tree_row_end(&sb);
        }
        cbm_tree_scalar_int(&sb, "total", result.row_count);
        if (result.warning) cbm_tree_scalar_str(&sb, "warning", result.warning);
        if (result.row_count == 0) {
            cbm_tree_scalar_str(&sb, "hint",
                                "Query returned no results. Use get_graph_schema() to see available labels and edge types.");
        }
        payload = cbm_sb_finish(&sb);
    } else {
        yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
        yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
        if (doc && root) {
            yyjson_mut_doc_set_root(doc, root);
            yyjson_mut_val *columns = yyjson_mut_arr(doc);
            yyjson_mut_val *rows = yyjson_mut_arr(doc);
            for (int c = 0; c < result.col_count; ++c) yyjson_mut_arr_add_str(doc, columns, result.columns[c]);
            for (int r = 0; r < result.row_count; ++r) {
                yyjson_mut_val *row = yyjson_mut_arr(doc);
                for (int c = 0; c < result.col_count; ++c) yyjson_mut_arr_add_str(doc, row, result.rows[r][c]);
                yyjson_mut_arr_add_val(rows, row);
            }
            yyjson_mut_obj_add_val(doc, root, "columns", columns);
            yyjson_mut_obj_add_val(doc, root, "rows", rows);
            yyjson_mut_obj_add_int(doc, root, "total", result.row_count);
            if (result.warning) yyjson_mut_obj_add_str(doc, root, "warning", result.warning);
            if (result.row_count == 0) {
                yyjson_mut_obj_add_str(doc, root, "hint",
                    "Query returned no results. Use get_graph_schema() to see available labels and edge types.");
            }
            payload = yyjson_mut_write(doc, 0, NULL);
        }
        if (doc) yyjson_mut_doc_free(doc);
    }

    cbm_cypher_result_free(&result);
    cbm_store_close(store);
    free(query);
    free(project);
    return payload ? cbm_operation_result_take(payload, false) : error_result("out of memory");
}
