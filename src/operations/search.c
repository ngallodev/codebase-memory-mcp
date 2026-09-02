#include "operations/search.h"

#include "foundation/constants.h"
#include "sqlite3/sqlite3.h"
#include "store/store.h"
#include "yyjson/yyjson.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    SEARCH_DEFAULT_LIMIT = 50,
    SEARCH_MAX_LIMIT = 1000,
    BM25_QUERY_BUFFER = 1024,
    BM25_INNER_LIMIT = 2000,
    BM25_COL_QN = 3,
    BM25_COL_LABEL = 1,
    BM25_COL_FILE = 4,
    BM25_COL_START = 5,
    BM25_COL_END = 6,
    BM25_COL_RANK = 7,
    SEARCH_MAX_FIELDS = 12,
    SEARCH_MAX_SEMANTIC_KEYWORDS = 32,
};

#define BM25_WEIGHTS "bm25(nodes_fts, 1.0, 1.0, 1.0, 1.0, 0.3)"

static sqlite3_destructor_type transient_destructor(void) {
    static const volatile intptr_t raw = -1;
    sqlite3_destructor_type destructor = NULL;
    memcpy(&destructor, (const void *)&raw, sizeof(destructor));
    return destructor;
}

static char *copy_text(const char *text) {
    if (!text) return NULL;
    size_t length = strlen(text);
    char *copy = malloc(length + 1U);
    if (copy) memcpy(copy, text, length + 1U);
    return copy;
}

static yyjson_doc *read_args(const char *args) {
    return args ? yyjson_read(args, strlen(args), 0) : NULL;
}

static char *string_arg(const char *args, const char *name) {
    yyjson_doc *doc = read_args(args);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    char *result = value && yyjson_is_str(value) ? copy_text(yyjson_get_str(value)) : NULL;
    if (doc) yyjson_doc_free(doc);
    return result;
}

static int int_arg(const char *args, const char *name, int fallback) {
    yyjson_doc *doc = read_args(args);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    int result = value && yyjson_is_int(value) ? (int)yyjson_get_sint(value) : fallback;
    if (doc) yyjson_doc_free(doc);
    return result;
}

static bool bool_arg(const char *args, const char *name) {
    yyjson_doc *doc = read_args(args);
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
        return cbm_operation_result_copy(message ? message : "search failed", true);
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "error", message ? message : "search failed");
    if (hint) yyjson_mut_obj_add_strcpy(doc, root, "hint", hint);
    return json_result(doc, true);
}

static bool valid_relationship(const char *value) {
    if (!value || !value[0]) return false;
    for (const unsigned char *p = (const unsigned char *)value; *p; ++p)
        if (!((*p >= 'A' && *p <= 'Z') || *p == '_')) return false;
    return true;
}

static int build_match(const char *query, char *output, size_t output_size) {
    if (!query || !output || output_size < 2U) return 0;
    size_t position = 0U;
    int tokens = 0;
    const char *p = query;
    while (*p) {
        while (*p && !((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
                       (*p >= '0' && *p <= '9') || *p == '_')) ++p;
        if (!*p) break;
        const char *start = p;
        while (*p && ((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
                      (*p >= '0' && *p <= '9') || *p == '_')) ++p;
        size_t length = (size_t)(p - start);
        const char *separator = tokens ? " OR " : "";
        size_t separator_length = strlen(separator);
        if (position + separator_length + length + 1U >= output_size) break;
        memcpy(output + position, separator, separator_length); position += separator_length;
        memcpy(output + position, start, length); position += length;
        ++tokens;
    }
    output[position] = '\0';
    return tokens;
}

static char *file_pattern_like(const char *pattern) {
    if (!pattern) return NULL;
    char *like = cbm_glob_to_like(pattern);
    if (!like || strchr(pattern, '*') || strchr(pattern, '?')) return like;
    size_t length = strlen(like);
    char *contains = malloc(length + 3U);
    if (!contains) return like;
    contains[0] = '%';
    memcpy(contains + 1U, like, length);
    contains[length + 1U] = '%';
    contains[length + 2U] = '\0';
    free(like);
    return contains;
}

static cbm_operation_result_t bm25_search(cbm_store_t *store, const char *project,
                                           const char *query, const char *file_pattern,
                                           int limit, int offset) {
    sqlite3 *db = cbm_store_get_db(store);
    char match[BM25_QUERY_BUFFER];
    if (!db || build_match(query, match, sizeof(match)) == 0)
        return cbm_operation_result_copy("", true);
    char *file_like = file_pattern_like(file_pattern);
    const char *sql =
        "SELECT n.id,n.label,n.name,n.qualified_name,n.file_path,n.start_line,n.end_line,"
        "(fts.base_rank-CASE WHEN n.label IN ('Function','Method') THEN 10.0 "
        "WHEN n.label='Route' THEN 8.0 WHEN n.label IN (" CBM_SQL_TYPE_LIKE_LABELS ") THEN 5.0 "
        "WHEN n.label IN (" CBM_SQL_RELATION_LABELS ") THEN 5.0 ELSE 0.0 END) AS rank "
        "FROM (SELECT rowid," BM25_WEIGHTS " AS base_rank FROM nodes_fts "
        "WHERE nodes_fts MATCH ?1 ORDER BY base_rank LIMIT ?5) fts "
        "JOIN nodes n ON n.id=fts.rowid WHERE n.project=?2 "
        "AND n.label NOT IN ('File','Folder','Variable','Project') "
        "AND (?6 IS NULL OR n.file_path LIKE ?6) ORDER BY rank,n.id LIMIT ?3 OFFSET ?4";
    sqlite3_stmt *statement = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &statement, NULL) != SQLITE_OK) {
        free(file_like);
        return error_result("full-text search unavailable", "Re-index the project or use structural search flags.");
    }
    sqlite3_destructor_type destructor = transient_destructor();
    sqlite3_bind_text(statement, 1, match, -1, destructor);
    sqlite3_bind_text(statement, 2, project, -1, destructor);
    sqlite3_bind_int(statement, 3, limit);
    sqlite3_bind_int(statement, 4, offset);
    sqlite3_bind_int(statement, 5, BM25_INNER_LIMIT);
    if (file_like) sqlite3_bind_text(statement, 6, file_like, -1, destructor);
    else sqlite3_bind_null(statement, 6);

    int total = 0;
    const char *count_sql =
        "SELECT COUNT(*) FROM (SELECT fts.rowid FROM (SELECT rowid FROM nodes_fts "
        "WHERE nodes_fts MATCH ?1 ORDER BY " BM25_WEIGHTS " LIMIT ?3) fts "
        "JOIN nodes n ON n.id=fts.rowid WHERE n.project=?2 "
        "AND n.label NOT IN ('File','Folder','Variable','Project') "
        "AND (?6 IS NULL OR n.file_path LIKE ?6))";
    sqlite3_stmt *counter = NULL;
    if (sqlite3_prepare_v2(db, count_sql, -1, &counter, NULL) == SQLITE_OK) {
        sqlite3_bind_text(counter, 1, match, -1, destructor);
        sqlite3_bind_text(counter, 2, project, -1, destructor);
        sqlite3_bind_int(counter, 3, BM25_INNER_LIMIT);
        if (file_like) sqlite3_bind_text(counter, 6, file_like, -1, destructor);
        else sqlite3_bind_null(counter, 6);
        if (sqlite3_step(counter) == SQLITE_ROW) total = sqlite3_column_int(counter, 0);
        sqlite3_finalize(counter);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    yyjson_mut_val *columns = doc ? yyjson_mut_arr(doc) : NULL;
    yyjson_mut_val *rows = doc ? yyjson_mut_arr(doc) : NULL;
    if (!doc || !root || !columns || !rows) {
        if (doc) yyjson_mut_doc_free(doc);
        sqlite3_finalize(statement); free(file_like);
        return error_result("result allocation failed", NULL);
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_int(doc, root, "total", total);
    yyjson_mut_obj_add_str(doc, root, "search_mode", "bm25");
    static const char *const names[] = {"qn", "label", "file", "lines", "rank"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i) yyjson_mut_arr_add_str(doc, columns, names[i]);
    yyjson_mut_obj_add_val(doc, root, "cols", columns);
    int emitted = 0;
    while (sqlite3_step(statement) == SQLITE_ROW) {
        char lines[32] = "";
        int start = sqlite3_column_int(statement, BM25_COL_START);
        int end = sqlite3_column_int(statement, BM25_COL_END);
        if (start > 0) (void)snprintf(lines, sizeof(lines), "%d-%d", start, end > start ? end : start);
        yyjson_mut_val *row = yyjson_mut_arr(doc);
        const unsigned char *qn = sqlite3_column_text(statement, BM25_COL_QN);
        const unsigned char *label = sqlite3_column_text(statement, BM25_COL_LABEL);
        const unsigned char *file = sqlite3_column_text(statement, BM25_COL_FILE);
        yyjson_mut_arr_add_strcpy(doc, row, qn ? (const char *)qn : "");
        yyjson_mut_arr_add_strcpy(doc, row, label ? (const char *)label : "");
        yyjson_mut_arr_add_strcpy(doc, row, file ? (const char *)file : "");
        yyjson_mut_arr_add_strcpy(doc, row, lines);
        yyjson_mut_arr_add_real(doc, row, sqlite3_column_double(statement, BM25_COL_RANK));
        yyjson_mut_arr_add_val(rows, row);
        ++emitted;
    }
    sqlite3_finalize(statement); free(file_like);
    yyjson_mut_obj_add_val(doc, root, "rows", rows);
    yyjson_mut_obj_add_bool(doc, root, "has_more", total > offset + emitted);
    return json_result(doc, false);
}

static bool blocked_field(const char *field) {
    return !field || !field[0] || strcmp(field, "fp") == 0 || strcmp(field, "sp") == 0 || strcmp(field, "bt") == 0;
}

static bool core_field(const char *field) {
    return strcmp(field, "qn") == 0 || strcmp(field, "qualified_name") == 0 || strcmp(field, "name") == 0 ||
           strcmp(field, "label") == 0 || strcmp(field, "file") == 0 || strcmp(field, "file_path") == 0 ||
           strcmp(field, "path") == 0 || strcmp(field, "lines") == 0 || strcmp(field, "in") == 0 || strcmp(field, "out") == 0;
}

static int parse_fields(const char *args, const char **fields, yyjson_doc **owner, bool *core_requested) {
    *owner = NULL; *core_requested = false;
    yyjson_doc *doc = read_args(args);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *array = yyjson_is_obj(root) ? yyjson_obj_get(root, "fields") : NULL;
    if (!yyjson_is_arr(array)) { if (doc) yyjson_doc_free(doc); return 0; }
    int count = 0;
    size_t index, max; yyjson_val *value;
    yyjson_arr_foreach(array, index, max, value) {
        const char *field = yyjson_is_str(value) ? yyjson_get_str(value) : NULL;
        if (blocked_field(field)) continue;
        if (core_field(field)) { *core_requested = true; continue; }
        if (count < SEARCH_MAX_FIELDS) fields[count++] = field;
    }
    if (count == 0) { yyjson_doc_free(doc); return 0; }
    *owner = doc;
    return count;
}

static int result_qn_cmp(const void *left, const void *right) {
    const cbm_search_result_t *a = left;
    const cbm_search_result_t *b = right;
    const char *aq = a->node.qualified_name ? a->node.qualified_name : "";
    const char *bq = b->node.qualified_name ? b->node.qualified_name : "";
    return strcmp(aq, bq);
}

static size_t prefix_length(const char *qn) {
    const char *last = qn ? strrchr(qn, '.') : NULL;
    return last ? (size_t)(last - qn) : 0U;
}

static void add_property_value(yyjson_mut_doc *doc, yyjson_mut_val *row, yyjson_val *value) {
    yyjson_mut_val *copy = value && !yyjson_is_null(value) ? yyjson_val_mut_copy(doc, value) : NULL;
    if (copy) yyjson_mut_arr_add_val(row, copy); else yyjson_mut_arr_add_null(doc, row);
}

static void emit_structural(yyjson_mut_doc *doc, yyjson_mut_val *root, cbm_search_output_t *output,
                            int offset, const char **fields, int field_count, bool include_connected) {
    yyjson_mut_obj_add_int(doc, root, "total", output->total);
    yyjson_mut_obj_add_int(doc, root, "count", output->count);
    yyjson_mut_val *columns = yyjson_mut_arr(doc);
    static const char *const base[] = {"name", "label", "lines", "in", "out"};
    for (size_t i = 0; i < sizeof(base) / sizeof(base[0]); ++i) yyjson_mut_arr_add_str(doc, columns, base[i]);
    for (int i = 0; i < field_count; ++i) yyjson_mut_arr_add_strcpy(doc, columns, fields[i]);
    if (include_connected) yyjson_mut_arr_add_str(doc, columns, "connected");
    yyjson_mut_obj_add_val(doc, root, "cols", columns);
    if (output->count > 1) qsort(output->results, (size_t)output->count, sizeof(*output->results), result_qn_cmp);
    yyjson_mut_val *groups = yyjson_mut_arr(doc);
    yyjson_mut_val *rows = NULL;
    char current[2048] = "";
    for (int i = 0; i < output->count; ++i) {
        cbm_search_result_t *search = &output->results[i];
        const char *qn = search->node.qualified_name ? search->node.qualified_name : "";
        const char *file = search->node.file_path ? search->node.file_path : "";
        size_t prefix = prefix_length(qn);
        char key[2048];
        (void)snprintf(key, sizeof(key), "%.*s|%s", (int)prefix, qn, file);
        if (!rows || strcmp(key, current) != 0) {
            (void)snprintf(current, sizeof(current), "%s", key);
            yyjson_mut_val *group = yyjson_mut_obj(doc);
            char prefix_text[1024];
            (void)snprintf(prefix_text, sizeof(prefix_text), "%.*s", (int)prefix, qn);
            yyjson_mut_obj_add_strcpy(doc, group, "qn_prefix", prefix_text);
            yyjson_mut_obj_add_strcpy(doc, group, "file", file);
            rows = yyjson_mut_arr(doc);
            yyjson_mut_obj_add_val(doc, group, "rows", rows);
            yyjson_mut_arr_add_val(groups, group);
        }
        char lines[32] = "";
        if (search->node.start_line > 0)
            (void)snprintf(lines, sizeof(lines), "%d-%d", search->node.start_line,
                           search->node.end_line > search->node.start_line ? search->node.end_line : search->node.start_line);
        yyjson_mut_val *row = yyjson_mut_arr(doc);
        yyjson_mut_arr_add_strcpy(doc, row, prefix ? qn + prefix + 1U : qn);
        yyjson_mut_arr_add_strcpy(doc, row, search->node.label ? search->node.label : "");
        yyjson_mut_arr_add_strcpy(doc, row, lines);
        yyjson_mut_arr_add_int(doc, row, search->in_degree);
        yyjson_mut_arr_add_int(doc, row, search->out_degree);
        if (field_count > 0) {
            yyjson_doc *properties = search->node.properties_json ? yyjson_read(search->node.properties_json, strlen(search->node.properties_json), 0) : NULL;
            yyjson_val *property_root = properties ? yyjson_doc_get_root(properties) : NULL;
            for (int field = 0; field < field_count; ++field)
                add_property_value(doc, row, yyjson_is_obj(property_root) ? yyjson_obj_get(property_root, fields[field]) : NULL);
            if (properties) yyjson_doc_free(properties);
        }
        if (include_connected) {
            yyjson_mut_val *connected = yyjson_mut_arr(doc);
            for (int c = 0; c < search->connected_count; ++c)
                yyjson_mut_arr_add_strcpy(doc, connected, search->connected_names[c] ? search->connected_names[c] : "");
            yyjson_mut_arr_add_val(row, connected);
        }
        yyjson_mut_arr_add_val(rows, row);
    }
    yyjson_mut_obj_add_val(doc, root, "groups", groups);
    yyjson_mut_obj_add_bool(doc, root, "has_more", output->total > offset + output->count);
}

static bool semantic_query(const char *args, cbm_store_t *store, const char *project, int limit,
                           cbm_vector_result_t **results, int *count, bool *present) {
    *results = NULL; *count = 0; *present = false;
    yyjson_doc *doc = read_args(args);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *array = yyjson_is_obj(root) ? yyjson_obj_get(root, "semantic_query") : NULL;
    if (!array) { if (doc) yyjson_doc_free(doc); return true; }
    *present = true;
    if (!yyjson_is_arr(array)) { yyjson_doc_free(doc); return false; }
    const char *keywords[SEARCH_MAX_SEMANTIC_KEYWORDS];
    int keyword_count = 0;
    size_t index, max; yyjson_val *value;
    yyjson_arr_foreach(array, index, max, value) {
        if (keyword_count < SEARCH_MAX_SEMANTIC_KEYWORDS && yyjson_is_str(value)) keywords[keyword_count++] = yyjson_get_str(value);
    }
    if (keyword_count > 0)
        (void)cbm_store_vector_search(store, project, keywords, keyword_count, limit, results, count);
    yyjson_doc_free(doc);
    return true;
}

static void emit_semantic(yyjson_mut_doc *doc, yyjson_mut_val *root,
                          const cbm_vector_result_t *results, int count) {
    yyjson_mut_val *semantic = yyjson_mut_obj(doc);
    yyjson_mut_val *columns = yyjson_mut_arr(doc);
    static const char *const names[] = {"qn", "label", "file", "score"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i) yyjson_mut_arr_add_str(doc, columns, names[i]);
    yyjson_mut_obj_add_val(doc, semantic, "cols", columns);
    yyjson_mut_val *rows = yyjson_mut_arr(doc);
    for (int i = 0; i < count; ++i) {
        yyjson_mut_val *row = yyjson_mut_arr(doc);
        yyjson_mut_arr_add_strcpy(doc, row, results[i].qualified_name ? results[i].qualified_name : "");
        yyjson_mut_arr_add_strcpy(doc, row, results[i].label ? results[i].label : "");
        yyjson_mut_arr_add_strcpy(doc, row, results[i].file_path ? results[i].file_path : "");
        yyjson_mut_arr_add_real(doc, row, results[i].score);
        yyjson_mut_arr_add_val(rows, row);
    }
    yyjson_mut_obj_add_val(doc, semantic, "rows", rows);
    yyjson_mut_obj_add_val(doc, root, "semantic", semantic);
}

cbm_operation_result_t cbm_search_operation_execute(const char *args) {
    char *project = string_arg(args, "project");
    char *query = string_arg(args, "query");
    char *label = string_arg(args, "label");
    char *name_pattern = string_arg(args, "name_pattern");
    char *qn_pattern = string_arg(args, "qn_pattern");
    char *file_pattern = string_arg(args, "file_pattern");
    char *relationship = string_arg(args, "relationship");
    int limit = int_arg(args, "limit", SEARCH_DEFAULT_LIMIT);
    int offset = int_arg(args, "offset", 0);
    int min_degree = int_arg(args, "min_degree", -1);
    int max_degree = int_arg(args, "max_degree", -1);
    bool exclude_entry_points = bool_arg(args, "exclude_entry_points");
    bool include_connected = bool_arg(args, "include_connected");
    if (limit < 1) limit = 1; else if (limit > SEARCH_MAX_LIMIT) limit = SEARCH_MAX_LIMIT;
    if (offset < 0) offset = 0;

    cbm_operation_result_t result = {0};
    cbm_store_t *store = NULL;
    cbm_search_output_t output = {0};
    cbm_vector_result_t *vectors = NULL;
    int vector_count = 0;
    yyjson_doc *fields_owner = NULL;

    if (!project || !project[0]) { result = error_result("project is required", "Run the command from an indexed repository."); goto done; }
    if (relationship && !valid_relationship(relationship)) { result = error_result("relationship must be uppercase letters and underscores", NULL); goto done; }
    store = cbm_store_open(project);
    if (!store) { result = error_result("project not indexed", "Run 'codebase-memory-cli index .' first."); goto done; }

    if (query && query[0]) {
        result = bm25_search(store, project, query, file_pattern, limit, offset);
        if (result.payload && result.payload[0]) goto done;
        cbm_operation_result_dispose(&result);
    }

    bool semantic_present = false;
    if (!semantic_query(args, store, project, limit, &vectors, &vector_count, &semantic_present)) {
        result = error_result("semantic_query must be an array of keyword strings",
                              "Example: --semantic-query '[\"send\",\"publish\"]'.");
        goto done;
    }
    bool structural = label || name_pattern || qn_pattern || file_pattern || relationship ||
                      exclude_entry_points || min_degree != -1 || max_degree != -1;
    bool semantic_only = semantic_present && !structural;
    cbm_search_params_t params = {.project = project, .label = label, .name_pattern = name_pattern,
                                  .qn_pattern = qn_pattern, .file_pattern = file_pattern,
                                  .relationship = relationship, .exclude_entry_points = exclude_entry_points,
                                  .include_connected = include_connected, .limit = limit, .offset = offset,
                                  .min_degree = min_degree, .max_degree = max_degree};
    if (!semantic_only && cbm_store_search(store, &params, &output) != CBM_STORE_OK) {
        result = error_result("graph search failed", NULL); goto done;
    }

    const char *fields[SEARCH_MAX_FIELDS];
    bool core_requested = false;
    int field_count = parse_fields(args, fields, &fields_owner, &core_requested);
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) { if (doc) yyjson_mut_doc_free(doc); result = error_result("result allocation failed", NULL); goto done; }
    yyjson_mut_doc_set_root(doc, root);
    if (!semantic_only) emit_structural(doc, root, &output, offset, fields, field_count, include_connected);
    if (core_requested) yyjson_mut_obj_add_str(doc, root, "fields_hint", "Core fields are already included and were not duplicated as extra property columns.");
    if (output.total == 0 && !semantic_only) {
        if (name_pattern && label) yyjson_mut_obj_add_str(doc, root, "hint", "No results. Remove the label filter or broaden name_pattern.");
        else if (name_pattern) yyjson_mut_obj_add_str(doc, root, "hint", "No nodes match this pattern. Check spelling or broaden the regex.");
        else if (label) yyjson_mut_obj_add_str(doc, root, "hint", "No nodes have this label. Use architecture/schema discovery to inspect available labels.");
    }
    if (semantic_present) {
        emit_semantic(doc, root, vectors, vector_count);
        if (semantic_only && vector_count == 0) yyjson_mut_obj_add_str(doc, root, "hint", "No semantic matches. Re-index at moderate/full semantic depth or broaden the keywords.");
    }
    result = json_result(doc, false);

done:
    if (fields_owner) yyjson_doc_free(fields_owner);
    if (vectors) cbm_store_free_vector_results(vectors, vector_count);
    cbm_store_search_free(&output);
    if (store) cbm_store_close(store);
    free(project); free(query); free(label); free(name_pattern); free(qn_pattern); free(file_pattern); free(relationship);
    return result;
}
