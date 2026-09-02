#include "operations/operation.h"

#include "foundation/platform.h"
#include "foundation/workspace.h"
#include "store/store.h"
#include "yyjson/yyjson.h"

#include <ctype.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

#ifndef CBM_NSEC_PER_SEC
#define CBM_NSEC_PER_SEC 1000000000LL
#endif

enum {
    OP_COVERAGE_PATH_MAX = 128,
    OP_COVERAGE_SCOPE_MAX = 32,
    OP_COVERAGE_SCOPE_DEFAULT_LIMIT = 200,
    OP_COVERAGE_SCOPE_MAX_LIMIT = 1000,
    OP_COVERAGE_RANGE_MAX = 128,
};

typedef enum op_coverage_path_result {
    OP_COVERAGE_PATH_OK = 0,
    OP_COVERAGE_PATH_OUTSIDE,
    OP_COVERAGE_PATH_INVALID,
} op_coverage_path_result_t;

static char *op_copy_string(const char *text) {
    if (!text) return NULL;
    size_t len = strlen(text);
    char *copy = malloc(len + 1U);
    if (copy) memcpy(copy, text, len + 1U);
    return copy;
}

static char *op_string_arg(const char *args, const char *name) {
    yyjson_doc *doc = args ? yyjson_read(args, strlen(args), 0) : NULL;
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    char *copy = value && yyjson_is_str(value) ? op_copy_string(yyjson_get_str(value)) : NULL;
    if (doc) yyjson_doc_free(doc);
    return copy;
}

static int op_int_arg(const char *args, const char *name, int fallback) {
    yyjson_doc *doc = args ? yyjson_read(args, strlen(args), 0) : NULL;
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    int result = value && yyjson_is_int(value) ? (int)yyjson_get_sint(value) : fallback;
    if (doc) yyjson_doc_free(doc);
    return result;
}

static cbm_operation_result_t op_json_result(yyjson_mut_doc *doc, bool error) {
    if (!doc) return cbm_operation_result_copy("{\"error\":\"result allocation failed\"}", true);
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    return json ? cbm_operation_result_take(json, error)
                : cbm_operation_result_copy("{\"error\":\"result encoding failed\"}", true);
}

static cbm_operation_result_t op_error(const char *message, const char *hint) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) {
        if (doc) yyjson_mut_doc_free(doc);
        return cbm_operation_result_copy(message ? message : "coverage failed", true);
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "error", message ? message : "coverage failed");
    if (hint) yyjson_mut_obj_add_strcpy(doc, root, "hint", hint);
    return op_json_result(doc, true);
}

static op_coverage_path_result_t normalize_rel(const char *input, bool allow_root, char *out,
                                               size_t out_size) {
    if (!input || !out || out_size == 0U) return OP_COVERAGE_PATH_INVALID;
    out[0] = '\0';
    size_t len = strlen(input);
    if (len == 0U || len >= out_size || input[0] == '/' || input[0] == '\\' ||
        (len >= 2U && isalpha((unsigned char)input[0]) && input[1] == ':')) {
        return OP_COVERAGE_PATH_OUTSIDE;
    }
    size_t in = 0U;
    size_t written = 0U;
    while (in < len) {
        while (in < len && (input[in] == '/' || input[in] == '\\')) ++in;
        if (in >= len) break;
        size_t start = in;
        while (in < len && input[in] != '/' && input[in] != '\\') {
            if ((unsigned char)input[in] < 0x20U) return OP_COVERAGE_PATH_INVALID;
            ++in;
        }
        size_t part_len = in - start;
        if (part_len == 1U && input[start] == '.') continue;
        if (part_len == 2U && input[start] == '.' && input[start + 1U] == '.') {
            return OP_COVERAGE_PATH_OUTSIDE;
        }
        if (written > 0U) {
            if (written + 1U >= out_size) return OP_COVERAGE_PATH_INVALID;
            out[written++] = '/';
        }
        if (written + part_len >= out_size) return OP_COVERAGE_PATH_INVALID;
        memcpy(out + written, input + start, part_len);
        written += part_len;
    }
    out[written] = '\0';
    return written > 0U || allow_root ? OP_COVERAGE_PATH_OK : OP_COVERAGE_PATH_INVALID;
}

static int64_t stat_mtime_ns(const struct stat *st) {
#ifdef __APPLE__
    return ((int64_t)st->st_mtimespec.tv_sec * CBM_NSEC_PER_SEC) + (int64_t)st->st_mtimespec.tv_nsec;
#elif defined(_WIN32)
    return (int64_t)st->st_mtime * CBM_NSEC_PER_SEC;
#else
    return ((int64_t)st->st_mtim.tv_sec * CBM_NSEC_PER_SEC) + (int64_t)st->st_mtim.tv_nsec;
#endif
}

static const char *path_freshness(cbm_store_t *store, const char *project, const char *root_path,
                                  const char *rel_path, bool *outside) {
    *outside = false;
    if (!root_path || !root_path[0]) return "unavailable";
    char abs_path[4096];
    int n = snprintf(abs_path, sizeof(abs_path), "%s%s%s", root_path,
                     root_path[strlen(root_path) - 1U] == '/' ? "" : "/", rel_path);
    if (n < 0 || (size_t)n >= sizeof(abs_path)) return "unavailable";
    struct stat st;
    if (stat(abs_path, &st) != 0) return "missing";
    if (!cbm_path_within_root(root_path, abs_path)) {
        *outside = true;
        return "outside_project";
    }
    cbm_file_hash_t hash = {0};
    int rc = cbm_store_get_file_hash(store, project, rel_path, &hash);
    if (rc == CBM_STORE_NOT_FOUND) return "not_tracked";
    if (rc != CBM_STORE_OK) return "unavailable";
    bool matches = hash.mtime_ns == stat_mtime_ns(&st) && hash.size == st.st_size;
    cbm_store_clear_file_hash(&hash);
    return matches ? "metadata_match" : "metadata_changed";
}

static void add_ranges(yyjson_mut_doc *doc, yyjson_mut_val *row, const char *detail) {
    if (!detail || !detail[0]) return;
    yyjson_mut_val *ranges = yyjson_mut_arr(doc);
    const char *p = detail;
    int emitted = 0;
    while (*p && emitted < OP_COVERAGE_RANGE_MAX) {
        while (*p == ' ' || *p == ',') ++p;
        if (!isdigit((unsigned char)*p)) break;
        char *endptr = NULL;
        long start = strtol(p, &endptr, 10);
        if (endptr == p || start <= 0 || start > INT32_MAX) break;
        p = endptr;
        long end = start;
        if (*p == '-') {
            ++p;
            long parsed = strtol(p, &endptr, 10);
            if (endptr == p || parsed < start || parsed > INT32_MAX) break;
            end = parsed;
            p = endptr;
        }
        yyjson_mut_val *range = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_int(doc, range, "start", start);
        yyjson_mut_obj_add_int(doc, range, "end", end);
        yyjson_mut_arr_add_val(ranges, range);
        ++emitted;
        while (*p == ' ') ++p;
        if (*p && *p != ',') break;
    }
    if (emitted > 0) yyjson_mut_obj_add_val(doc, row, "ranges", ranges);
}

static void add_row(yyjson_mut_doc *doc, yyjson_mut_val *array, const cbm_coverage_row_t *row,
                    const char *requested_path) {
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_strcpy(doc, item, "path", row->rel_path ? row->rel_path : "");
    yyjson_mut_obj_add_strcpy(doc, item, "kind", row->kind ? row->kind : "");
    yyjson_mut_obj_add_strcpy(doc, item, "detail", row->detail ? row->detail : "");
    if (requested_path) {
        yyjson_mut_obj_add_str(doc, item, "match",
                               row->rel_path && strcmp(row->rel_path, requested_path) == 0
                                   ? "exact"
                                   : "ancestor");
    }
    if (row->kind && strcmp(row->kind, "parse_partial") == 0) add_ranges(doc, item, row->detail);
    yyjson_mut_arr_add_val(array, item);
}

static const char *coverage_status(const cbm_coverage_row_t *rows, int count,
                                   const char *requested_path, const char *recording_status,
                                   bool generation_matches, bool lookup_ok,
                                   bool exact_path_verified) {
    if (!lookup_ok) return "coverage_unavailable";
    bool exact = false;
    for (int i = 0; i < count; ++i) {
        if (rows[i].rel_path && strcmp(rows[i].rel_path, requested_path) == 0) {
            exact = true;
            break;
        }
    }
    for (int pass = 0; pass < 3; ++pass) {
        for (int i = 0; i < count; ++i) {
            if (exact && (!rows[i].rel_path || strcmp(rows[i].rel_path, requested_path) != 0)) continue;
            const char *kind = rows[i].kind ? rows[i].kind : "";
            if (pass == 0 && strcmp(kind, "parse_partial") == 0) return "partial";
            if (pass == 1 && strncmp(kind, "not_indexed", 11) == 0) return "excluded";
            if (pass == 2 && kind[0]) return "skipped";
        }
    }
    bool complete = recording_status && strcmp(recording_status, "complete") == 0;
    bool truncated_exact = exact_path_verified && recording_status &&
                           strcmp(recording_status, "truncated") == 0;
    return generation_matches && (complete || truncated_exact) ? "no_recorded_issue"
                                                                : "coverage_unavailable";
}

static const char *recommended_action(const char *status, const char *freshness) {
    if (!freshness || strcmp(freshness, "metadata_match") != 0) return "read_source_and_reindex";
    if (strcmp(status, "partial") == 0) return "read_ranges_and_verify_scope";
    if (strcmp(status, "skipped") == 0) return "read_source_directly";
    if (strcmp(status, "excluded") == 0) return "read_source_or_change_ignore_rules";
    if (strcmp(status, "no_recorded_issue") == 0) return "use_graph_with_best_effort_caveat";
    return "read_source_and_reindex";
}

cbm_operation_result_t cbm_coverage_operation_execute(const char *args) {
    char *project = op_string_arg(args, "project");
    if (!project || !project[0]) {
        free(project);
        return op_error("project is required", "Run the command from an indexed repository.");
    }
    cbm_store_t *store = cbm_store_open(project);
    if (!store) {
        cbm_operation_result_t result = op_error("project not indexed", "Run 'codebase-memory-cli index .' first.");
        free(project);
        return result;
    }

    yyjson_doc *adoc = args ? yyjson_read(args, strlen(args), 0) : NULL;
    yyjson_val *aroot = adoc ? yyjson_doc_get_root(adoc) : NULL;
    yyjson_val *paths = yyjson_is_obj(aroot) ? yyjson_obj_get(aroot, "paths") : NULL;
    yyjson_val *scopes = yyjson_is_obj(aroot) ? yyjson_obj_get(aroot, "scopes") : NULL;
    size_t path_count = paths && yyjson_is_arr(paths) ? yyjson_arr_size(paths) : 0U;
    size_t scope_count = scopes && yyjson_is_arr(scopes) ? yyjson_arr_size(scopes) : 0U;
    if (!aroot || (paths && !yyjson_is_arr(paths)) || (scopes && !yyjson_is_arr(scopes)) ||
        (path_count == 0U && scope_count == 0U) || path_count > OP_COVERAGE_PATH_MAX ||
        scope_count > OP_COVERAGE_SCOPE_MAX) {
        if (adoc) yyjson_doc_free(adoc);
        cbm_store_close(store);
        free(project);
        return op_error("paths or scopes is required (arrays; max 128 paths and 32 scopes)", NULL);
    }

    cbm_project_t proj = {0};
    bool have_project = cbm_store_get_project(store, project, &proj) == CBM_STORE_OK;
    cbm_coverage_meta_t meta = {0};
    bool have_meta = cbm_store_coverage_meta_get(store, project, &meta) == CBM_STORE_OK;
    bool generation_matches = have_project && have_meta && proj.indexed_at && meta.generation &&
                              strcmp(proj.indexed_at, meta.generation) == 0;
    const char *recording_status = have_meta && meta.recording_status ? meta.recording_status : "unknown";

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) {
        if (doc) yyjson_mut_doc_free(doc);
        yyjson_doc_free(adoc);
        if (have_meta) cbm_store_coverage_meta_clear(&meta);
        if (have_project) cbm_project_free_fields(&proj);
        cbm_store_close(store);
        free(project);
        return op_error("result allocation failed", NULL);
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "project", project);
    yyjson_mut_obj_add_str(doc, root, "signal", "best_effort");
    yyjson_mut_obj_add_strcpy(doc, root, "indexed_at", have_project && proj.indexed_at ? proj.indexed_at : "");

    yyjson_mut_val *meta_obj = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_strcpy(doc, meta_obj, "generation", have_meta && meta.generation ? meta.generation : "");
    yyjson_mut_obj_add_strcpy(doc, meta_obj, "index_mode", have_meta && meta.index_mode ? meta.index_mode : "unknown");
    yyjson_mut_obj_add_strcpy(doc, meta_obj, "recorded_at", have_meta && meta.recorded_at ? meta.recorded_at : "");
    yyjson_mut_obj_add_strcpy(doc, meta_obj, "recording_status", recording_status);
    yyjson_mut_obj_add_int(doc, meta_obj, "ignored_files_stored", have_meta ? meta.ignored_files_stored : 0);
    yyjson_mut_obj_add_int(doc, meta_obj, "ignored_files_total", have_meta ? meta.ignored_files_total : 0);
    yyjson_mut_obj_add_bool(doc, meta_obj, "hash_records_complete", have_meta && meta.hash_records_complete);
    yyjson_mut_obj_add_int(doc, meta_obj, "coverage_version", have_meta ? meta.coverage_version : 0);
    yyjson_mut_obj_add_bool(doc, meta_obj, "generation_matches", generation_matches);
    yyjson_mut_obj_add_val(doc, root, "metadata", meta_obj);

    size_t idx, max;
    yyjson_val *value;
    yyjson_mut_val *path_results = yyjson_mut_arr(doc);
    if (paths) {
        yyjson_arr_foreach(paths, idx, max, value) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            const char *input = yyjson_is_str(value) ? yyjson_get_str(value) : NULL;
            yyjson_mut_obj_add_strcpy(doc, item, "requested_path", input ? input : "");
            char rel[4096];
            op_coverage_path_result_t normalized = normalize_rel(input, false, rel, sizeof(rel));
            if (normalized != OP_COVERAGE_PATH_OK) {
                yyjson_mut_obj_add_str(doc, item, "status",
                                       normalized == OP_COVERAGE_PATH_OUTSIDE ? "outside_project" : "invalid_path");
                yyjson_mut_obj_add_str(doc, item, "freshness", "unavailable");
                yyjson_mut_obj_add_str(doc, item, "recommended_action", "use_project_relative_path");
                yyjson_mut_arr_add_val(path_results, item);
                continue;
            }
            yyjson_mut_obj_add_strcpy(doc, item, "path", rel);
            cbm_coverage_row_t *rows = NULL;
            int row_count = 0;
            int rc = cbm_store_coverage_get_path(store, project, rel, &rows, &row_count);
            bool lookup_ok = rc == CBM_STORE_OK || rc == CBM_STORE_NOT_FOUND;
            if (!lookup_ok) {
                row_count = 0;
                yyjson_mut_obj_add_str(doc, item, "coverage_lookup", "error");
            }
            bool outside = false;
            const char *freshness = path_freshness(store, project, have_project ? proj.root_path : NULL, rel, &outside);
            bool exact_verified = have_meta && meta.hash_records_complete && strcmp(freshness, "metadata_match") == 0;
            const char *status = outside ? "outside_project"
                                         : coverage_status(rows, row_count, rel, recording_status,
                                                           generation_matches, lookup_ok, exact_verified);
            yyjson_mut_obj_add_strcpy(doc, item, "status", status);
            yyjson_mut_obj_add_strcpy(doc, item, "freshness", freshness);
            yyjson_mut_obj_add_strcpy(doc, item, "recommended_action", recommended_action(status, freshness));
            yyjson_mut_val *coverage = yyjson_mut_arr(doc);
            for (int i = 0; i < row_count; ++i) add_row(doc, coverage, &rows[i], rel);
            yyjson_mut_obj_add_val(doc, item, "coverage", coverage);
            cbm_store_free_coverage(rows, row_count);
            yyjson_mut_arr_add_val(path_results, item);
        }
    }
    yyjson_mut_obj_add_val(doc, root, "paths", path_results);

    int scope_limit = op_int_arg(args, "scope_limit", OP_COVERAGE_SCOPE_DEFAULT_LIMIT);
    int scope_offset = op_int_arg(args, "scope_offset", 0);
    if (scope_limit < 1) scope_limit = 1;
    else if (scope_limit > OP_COVERAGE_SCOPE_MAX_LIMIT) scope_limit = OP_COVERAGE_SCOPE_MAX_LIMIT;
    if (scope_offset < 0) scope_offset = 0;
    yyjson_mut_val *scope_results = yyjson_mut_arr(doc);
    if (scopes) {
        yyjson_arr_foreach(scopes, idx, max, value) {
            yyjson_mut_val *item = yyjson_mut_obj(doc);
            const char *input = yyjson_is_str(value) ? yyjson_get_str(value) : NULL;
            yyjson_mut_obj_add_strcpy(doc, item, "requested_scope", input ? input : "");
            char scope[4096];
            op_coverage_path_result_t normalized = normalize_rel(input, true, scope, sizeof(scope));
            if (normalized != OP_COVERAGE_PATH_OK) {
                yyjson_mut_obj_add_str(doc, item, "status",
                                       normalized == OP_COVERAGE_PATH_OUTSIDE ? "outside_project" : "invalid_path");
                yyjson_mut_arr_add_val(scope_results, item);
                continue;
            }
            yyjson_mut_obj_add_strcpy(doc, item, "scope", scope[0] ? scope : ".");
            cbm_coverage_row_t *rows = NULL;
            int row_count = 0;
            int rc = cbm_store_coverage_get_scope(store, project, scope, &rows, &row_count);
            bool lookup_ok = rc == CBM_STORE_OK || rc == CBM_STORE_NOT_FOUND;
            if (!lookup_ok) {
                row_count = 0;
                yyjson_mut_obj_add_str(doc, item, "coverage_lookup", "error");
            }
            yyjson_mut_obj_add_int(doc, item, "total", row_count);
            int start = scope_offset < row_count ? scope_offset : row_count;
            int end = start + scope_limit < row_count ? start + scope_limit : row_count;
            yyjson_mut_obj_add_bool(doc, item, "has_more", end < row_count);
            if (end < row_count) yyjson_mut_obj_add_int(doc, item, "next_offset", end);
            yyjson_mut_val *entries = yyjson_mut_arr(doc);
            for (int i = start; i < end; ++i) add_row(doc, entries, &rows[i], NULL);
            yyjson_mut_obj_add_val(doc, item, "entries", entries);
            const char *scope_status = !lookup_ok || !generation_matches
                                           ? "coverage_unavailable"
                                           : row_count > 0 ? "known_gaps"
                                                           : strcmp(recording_status, "complete") == 0
                                                                 ? "no_recorded_issue"
                                                                 : "coverage_unavailable";
            yyjson_mut_obj_add_str(doc, item, "status", scope_status);
            cbm_store_free_coverage(rows, row_count);
            yyjson_mut_arr_add_val(scope_results, item);
        }
    }
    yyjson_mut_obj_add_val(doc, root, "scopes", scope_results);
    yyjson_mut_obj_add_str(doc, root, "caveat",
                           "Best-effort signal only. No recorded issue does not prove graph or source completeness; read flagged source and qualify claims when metadata is changed or unavailable.");

    yyjson_doc_free(adoc);
    if (have_meta) cbm_store_coverage_meta_clear(&meta);
    if (have_project) cbm_project_free_fields(&proj);
    cbm_store_close(store);
    free(project);
    return op_json_result(doc, false);
}
