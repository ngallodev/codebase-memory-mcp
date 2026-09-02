#include "operations/file_outline.h"

#include "foundation/constants.h"
#include "operations/compact_out.h"
#include "operations/command_runner.h"
#include "store/store.h"
#include "yyjson/yyjson.h"

#include <ctype.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FILE_OUTLINE_OUTPUT_MAX ((size_t)2U * 1024U * 1024U)

typedef enum outline_path_result {
    OUTLINE_PATH_OK = 0,
    OUTLINE_PATH_OUTSIDE,
    OUTLINE_PATH_INVALID,
} outline_path_result_t;

static cbm_operation_result_t outline_error(const char *message) {
    return cbm_operation_result_copy(message ? message : "get_file_outline failed", true);
}

static char *outline_strdup(const char *text) {
    if (!text) return NULL;
    size_t len = strlen(text);
    char *copy = malloc(len + 1U);
    if (copy) memcpy(copy, text, len + 1U);
    return copy;
}

static char *outline_project_arg(yyjson_val *root) {
    static const char *const names[] = {"project", "project_name", "project_id", "projectName"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i) {
        yyjson_val *value = yyjson_obj_get(root, names[i]);
        if (value && yyjson_is_str(value)) return outline_strdup(yyjson_get_str(value));
    }
    return NULL;
}

static outline_path_result_t outline_normalize_rel(const char *input, char *out, size_t out_size) {
    if (!input || !out || out_size == 0U) return OUTLINE_PATH_INVALID;
    out[0] = '\0';
    size_t len = strlen(input);
    if (len == 0U || len >= out_size || input[0] == '/' || input[0] == '\\' ||
        (len >= 2U && isalpha((unsigned char)input[0]) && input[1] == ':')) {
        return OUTLINE_PATH_OUTSIDE;
    }
    size_t in = 0U;
    size_t written = 0U;
    while (in < len) {
        while (in < len && (input[in] == '/' || input[in] == '\\')) ++in;
        if (in >= len) break;
        size_t start = in;
        while (in < len && input[in] != '/' && input[in] != '\\') {
            if ((unsigned char)input[in] < 0x20U) return OUTLINE_PATH_INVALID;
            ++in;
        }
        size_t part_len = in - start;
        if (part_len == 1U && input[start] == '.') continue;
        if (part_len == 2U && input[start] == '.' && input[start + 1U] == '.') {
            return OUTLINE_PATH_OUTSIDE;
        }
        if (written > 0U) {
            if (written + 1U >= out_size) return OUTLINE_PATH_INVALID;
            out[written++] = '/';
        }
        if (written + part_len >= out_size) return OUTLINE_PATH_INVALID;
        memcpy(out + written, input + start, part_len);
        written += part_len;
    }
    out[written] = '\0';
    return written > 0U ? OUTLINE_PATH_OK : OUTLINE_PATH_INVALID;
}

static bool outline_cancelled(void *context) {
    return cbm_operation_runtime_cancelled((const cbm_operation_runtime_t *)context);
}

static char *outline_json_payload(const char *file_path, cbm_file_outline_row_t *rows, int row_count,
                                  int total, int offset, int limit) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *object = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !object) {
        if (doc) yyjson_mut_doc_free(doc);
        return NULL;
    }
    yyjson_mut_doc_set_root(doc, object);
    yyjson_mut_obj_add_strcpy(doc, object, "file_path", file_path);
    yyjson_mut_val *columns = yyjson_mut_arr(doc);
    static const char *const names[] = {"name", "label", "lines", "qn"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i) yyjson_mut_arr_add_str(doc, columns, names[i]);
    yyjson_mut_obj_add_val(doc, object, "cols", columns);
    yyjson_mut_val *json_rows = yyjson_mut_arr(doc);
    for (int i = 0; i < row_count; ++i) {
        char lines[CBM_SZ_32];
        if (rows[i].start_line > 0) {
            snprintf(lines, sizeof(lines), "%d-%d", rows[i].start_line,
                     rows[i].end_line > rows[i].start_line ? rows[i].end_line : rows[i].start_line);
        } else {
            lines[0] = '\0';
        }
        yyjson_mut_val *row = yyjson_mut_arr(doc);
        yyjson_mut_arr_add_strcpy(doc, row, rows[i].name ? rows[i].name : "");
        yyjson_mut_arr_add_strcpy(doc, row, rows[i].label ? rows[i].label : "");
        yyjson_mut_arr_add_strcpy(doc, row, lines);
        yyjson_mut_arr_add_strcpy(doc, row, rows[i].qualified_name ? rows[i].qualified_name : "");
        yyjson_mut_arr_add_val(json_rows, row);
    }
    yyjson_mut_obj_add_val(doc, object, "rows", json_rows);
    yyjson_mut_obj_add_int(doc, object, "total", total);
    yyjson_mut_obj_add_int(doc, object, "offset", offset);
    yyjson_mut_obj_add_int(doc, object, "limit", limit);
    yyjson_mut_obj_add_int(doc, object, "returned", row_count);
    yyjson_mut_obj_add_bool(doc, object, "has_more", (int64_t)offset + row_count < total);
    char *payload = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return payload;
}

static char *outline_tree_payload(const char *file_path, cbm_file_outline_row_t *rows, int row_count,
                                  int total, int offset, int limit) {
    cbm_sb_t sb;
    cbm_sb_init(&sb);
    cbm_tree_scalar_str(&sb, "file_path", file_path);
    static const char *const columns[] = {"name", "label", "lines", "qn"};
    cbm_tree_table_header(&sb, "results", row_count, columns, 4);
    for (int i = 0; i < row_count; ++i) {
        char lines[CBM_SZ_32];
        if (rows[i].start_line > 0) {
            snprintf(lines, sizeof(lines), "%d-%d", rows[i].start_line,
                     rows[i].end_line > rows[i].start_line ? rows[i].end_line : rows[i].start_line);
        } else {
            lines[0] = '\0';
        }
        cbm_tree_row_begin(&sb);
        cbm_tree_cell_str(&sb, rows[i].name, true);
        cbm_tree_cell_str(&sb, rows[i].label, false);
        cbm_tree_cell_str(&sb, lines, false);
        cbm_tree_cell_str(&sb, rows[i].qualified_name, false);
        cbm_tree_row_end(&sb);
    }
    cbm_tree_scalar_int(&sb, "total", total);
    cbm_tree_scalar_int(&sb, "offset", offset);
    cbm_tree_scalar_int(&sb, "limit", limit);
    cbm_tree_scalar_int(&sb, "returned", row_count);
    cbm_tree_scalar_bool(&sb, "has_more", (int64_t)offset + row_count < total);
    return cbm_sb_finish(&sb);
}

cbm_operation_result_t cbm_file_outline_operation_execute(const char *args_json,
                                                           const cbm_operation_runtime_t *runtime) {
    const char *args_text = args_json ? args_json : "{}";
    yyjson_doc *doc = yyjson_read(args_text, strlen(args_text), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    if (!root || !yyjson_is_obj(root)) {
        if (doc) yyjson_doc_free(doc);
        return outline_error("get_file_outline arguments must be a JSON object");
    }
    char *project = outline_project_arg(root);
    yyjson_val *file_value = yyjson_obj_get(root, "file_path");
    const char *file_path = file_value && yyjson_is_str(file_value) ? yyjson_get_str(file_value) : NULL;
    if (!project) {
        yyjson_doc_free(doc);
        return outline_error("project is required");
    }
    if (!file_path || !file_path[0]) {
        free(project);
        yyjson_doc_free(doc);
        return outline_error("file_path is required");
    }
    char normalized[CBM_SZ_4K];
    if (outline_normalize_rel(file_path, normalized, sizeof(normalized)) != OUTLINE_PATH_OK) {
        free(project);
        yyjson_doc_free(doc);
        return outline_error("file_path must be a repository-relative path without '..'");
    }
    int limit = 100;
    yyjson_val *limit_value = yyjson_obj_get(root, "limit");
    if (limit_value) {
        if (!yyjson_is_int(limit_value)) {
            free(project); yyjson_doc_free(doc); return outline_error("limit must be an integer");
        }
        int64_t parsed = yyjson_get_sint(limit_value);
        if (parsed < 1 || parsed > CBM_STORE_FILE_OUTLINE_MAX_LIMIT) {
            free(project); yyjson_doc_free(doc); return outline_error("limit must be between 1 and 200");
        }
        limit = (int)parsed;
    }
    int offset = 0;
    yyjson_val *offset_value = yyjson_obj_get(root, "offset");
    if (offset_value) {
        if (!yyjson_is_int(offset_value)) {
            free(project); yyjson_doc_free(doc); return outline_error("offset must be a non-negative integer");
        }
        int64_t parsed = yyjson_get_sint(offset_value);
        if (parsed < 0 || parsed > INT_MAX) {
            free(project); yyjson_doc_free(doc); return outline_error("offset must be a non-negative integer");
        }
        offset = (int)parsed;
    }
    bool json_format = false;
    yyjson_val *format_value = yyjson_obj_get(root, "format");
    if (format_value) {
        const char *format = yyjson_is_str(format_value) ? yyjson_get_str(format_value) : NULL;
        if (!format || (strcmp(format, "tree") != 0 && strcmp(format, "json") != 0)) {
            free(project); yyjson_doc_free(doc); return outline_error("format must be either 'tree' or 'json'");
        }
        json_format = strcmp(format, "json") == 0;
    }
    const char *labels[CBM_STORE_FILE_OUTLINE_MAX_LABELS];
    int label_count = 0;
    yyjson_val *labels_value = yyjson_obj_get(root, "labels");
    if (labels_value) {
        if (!yyjson_is_arr(labels_value) || yyjson_arr_size(labels_value) > CBM_STORE_FILE_OUTLINE_MAX_LABELS) {
            free(project); yyjson_doc_free(doc); return outline_error("labels must be an array of at most 16 strings");
        }
        size_t index = 0, max = 0;
        yyjson_val *item = NULL;
        yyjson_arr_foreach(labels_value, index, max, item) {
            const char *label = yyjson_is_str(item) ? yyjson_get_str(item) : NULL;
            if (!label || !label[0] || strlen(label) >= CBM_SZ_128) {
                free(project); yyjson_doc_free(doc); return outline_error("labels must contain only non-empty bounded strings");
            }
            labels[label_count++] = label;
        }
    }
    cbm_store_t *store = cbm_store_open(project);
    if (!store) {
        free(project); yyjson_doc_free(doc); return outline_error("project not found or not indexed");
    }
    cbm_project_t info = {0};
    if (cbm_store_get_project(store, project, &info) != CBM_STORE_OK) {
        cbm_store_close(store); free(project); yyjson_doc_free(doc); return outline_error("project not found or not indexed");
    }
    cbm_project_free_fields(&info);
    cbm_file_outline_row_t *rows = NULL;
    int row_count = 0, total = 0;
    int rc = cbm_store_get_file_outline(store, project, normalized, labels, label_count, limit, offset,
                                        outline_cancelled, (void *)runtime, &rows, &row_count, &total);
    cbm_store_close(store);
    if (rc != CBM_STORE_OK) {
        free(project); yyjson_doc_free(doc);
        if (rc == CBM_STORE_CANCELLED) return outline_error("get_file_outline cancelled for this request");
        if (rc == CBM_STORE_SCAN_LIMIT) return outline_error("get_file_outline exceeded its fixed output safety limit");
        return outline_error("get_file_outline query failed");
    }
    char *payload = json_format ? outline_json_payload(normalized, rows, row_count, total, offset, limit)
                                : outline_tree_payload(normalized, rows, row_count, total, offset, limit);
    cbm_store_free_file_outline(rows, row_count);
    free(project);
    yyjson_doc_free(doc);
    if (!payload) return outline_error("get_file_outline output allocation failed");
    if (strlen(payload) > FILE_OUTLINE_OUTPUT_MAX) {
        free(payload);
        return outline_error("get_file_outline exceeded its fixed output safety limit");
    }
    return cbm_operation_result_take(payload, false);
}
