#include "operations/source_search.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_regex.h"
#include "foundation/constants.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/workspace.h"
#include "operations/command_runner.h"
#include "operations/compact_out.h"
#include "store/store.h"
#include "yyjson/yyjson.h"

#ifdef _WIN32
#include <io.h>
#define source_fdopen _fdopen
#define source_close _close
#else
#include <unistd.h>
#define source_fdopen fdopen
#define source_close close
#endif

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define SOURCE_PS_UTF8_PRELUDE "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; "

enum {
    SOURCE_DEFAULT_LIMIT = 10,
    SOURCE_RETURN_FILES = 2,
    SOURCE_PAIR_LEN = 2,
    SOURCE_SKIP_ONE = 1,
};

#define SOURCE_SEARCH_OUTPUT_MAX ((size_t)64U * 1024U * 1024U)
#define SOURCE_SEARCH_SCAN_TIMEOUT_MS ((uint64_t)30000U)

static char *source_strdup(const char *text) {
    if (!text) return NULL;
    size_t len = strlen(text);
    char *copy = malloc(len + 1U);
    if (copy) memcpy(copy, text, len + 1U);
    return copy;
}

static yyjson_doc *source_args_doc(const char *args) {
    const char *json = args ? args : "{}";
    return yyjson_read(json, strlen(json), 0);
}

static char *source_string_arg(const char *args, const char *name) {
    yyjson_doc *doc = source_args_doc(args);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    char *result = value && yyjson_is_str(value) ? source_strdup(yyjson_get_str(value)) : NULL;
    if (doc) yyjson_doc_free(doc);
    return result;
}

static char *source_project_arg(const char *args) {
    static const char *const names[] = {"project", "project_name", "project_id", "projectName"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i) {
        char *value = source_string_arg(args, names[i]);
        if (value) return value;
    }
    return NULL;
}

static int source_int_arg(const char *args, const char *name, int fallback) {
    yyjson_doc *doc = source_args_doc(args);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    int result = value && yyjson_is_int(value) ? (int)yyjson_get_sint(value) : fallback;
    if (doc) yyjson_doc_free(doc);
    return result;
}

static bool source_bool_arg(const char *args, const char *name, bool fallback) {
    yyjson_doc *doc = source_args_doc(args);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    bool result = value && yyjson_is_bool(value) ? yyjson_get_bool(value) : fallback;
    if (doc) yyjson_doc_free(doc);
    return result;
}

static char *source_doc_to_str(yyjson_mut_doc *doc) {
    return yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INVALID_UNICODE, NULL);
}

static cbm_operation_result_t source_error(const char *message) {
    return cbm_operation_result_copy(message ? message : "source search failed", true);
}

static cbm_operation_result_t source_project_error(const char *project) {
    if (!project) {
        return source_error("{\"error\":\"missing required argument: project\",\"hint\":\"Pass the project argument. Run projects to see indexed projects.\"}");
    }
    return source_error("{\"error\":\"project not found or not indexed\",\"hint\":\"Run projects to see indexed projects.\"}");
}

static cbm_store_t *source_open_store_and_root(const char *project, char **root_path_out) {
    *root_path_out = NULL;
    if (!project || !project[0]) return NULL;
    cbm_store_t *store = cbm_store_open(project);
    if (!store) return NULL;
    cbm_project_t info = {0};
    if (cbm_store_get_project(store, project, &info) != CBM_STORE_OK || !info.root_path || !info.root_path[0]) {
        cbm_project_free_fields(&info);
        cbm_store_close(store);
        return NULL;
    }
    *root_path_out = source_strdup(info.root_path);
    cbm_project_free_fields(&info);
    if (!*root_path_out) {
        cbm_store_close(store);
        return NULL;
    }
    return store;
}

static char *source_read_file_lines(const char *path, int start, int end) {
    FILE *fp = cbm_fopen(path, "r");
    if (!fp) return NULL;
    size_t cap = CBM_SZ_4K;
    char *buf = malloc(cap);
    if (!buf) { (void)fclose(fp); return NULL; }
    size_t len = 0;
    buf[0] = '\0';
    char line[CBM_SZ_2K];
    int lineno = 0;
    while (fgets(line, sizeof(line), fp)) {
        lineno++;
        if (lineno < start) continue;
        if (lineno > end) break;
        size_t ll = strlen(line);
        while (len + ll + 1U > cap) {
            cap *= 2U;
            buf = safe_realloc(buf, cap);
        }
        memcpy(buf + len, line, ll);
        len += ll;
        buf[len] = '\0';
    }
    (void)fclose(fp);
    if (len == 0) { free(buf); return NULL; }
    return buf;
}

static bool source_utf8_is_cont(unsigned char c) { return (c & 0xC0) == 0x80; }

static char *source_sanitize_utf8_lossy(const char *s) {
    enum { REP_LEN = 3, THREE = 3, FOUR = 4, FOURTH = 3 };
    if (!s) return NULL;
    size_t len = strlen(s);
    if (len > (((size_t)-1) - 1U) / REP_LEN) return NULL;
    char *out = malloc(len * REP_LEN + 1U);
    if (!out) return NULL;
    const unsigned char *p = (const unsigned char *)s;
    const unsigned char *end = p + len;
    unsigned char *dst = (unsigned char *)out;
    while (p < end) {
        unsigned char c = *p;
        size_t n = 0;
        if (c < 0x80) n = 1;
        else if (c >= 0xC2 && c <= 0xDF && p + 1 < end && source_utf8_is_cont(p[1])) n = 2;
        else if (c == 0xE0 && p + 2 < end && p[1] >= 0xA0 && p[1] <= 0xBF && source_utf8_is_cont(p[2])) n = THREE;
        else if (c >= 0xE1 && c <= 0xEC && p + 2 < end && source_utf8_is_cont(p[1]) && source_utf8_is_cont(p[2])) n = THREE;
        else if (c == 0xED && p + 2 < end && p[1] >= 0x80 && p[1] <= 0x9F && source_utf8_is_cont(p[2])) n = THREE;
        else if (c >= 0xEE && c <= 0xEF && p + 2 < end && source_utf8_is_cont(p[1]) && source_utf8_is_cont(p[2])) n = THREE;
        else if (c == 0xF0 && p + FOURTH < end && p[1] >= 0x90 && p[1] <= 0xBF && source_utf8_is_cont(p[2]) && source_utf8_is_cont(p[FOURTH])) n = FOUR;
        else if (c >= 0xF1 && c <= 0xF3 && p + FOURTH < end && source_utf8_is_cont(p[1]) && source_utf8_is_cont(p[2]) && source_utf8_is_cont(p[FOURTH])) n = FOUR;
        else if (c == 0xF4 && p + FOURTH < end && p[1] >= 0x80 && p[1] <= 0x8F && source_utf8_is_cont(p[2]) && source_utf8_is_cont(p[FOURTH])) n = FOUR;
        if (n > 0) { memcpy(dst, p, n); dst += n; p += n; }
        else { *dst++ = 0xEF; *dst++ = 0xBF; *dst++ = 0xBD; p++; }
    }
    *dst = '\0';
    return out;
}
/* ── search_code v2: graph-augmented code search ─────────────── */

/* Intermediate grep match */
typedef struct {
    char file[CBM_SZ_512];
    int line;
    char content[CBM_SZ_1K];
} grep_match_t;

/* Deduped result: one per containing graph node */
typedef struct {
    int64_t node_id; /* 0 = raw match (no containing node) */
    char node_name[CBM_SZ_256];
    char qualified_name[CBM_SZ_512];
    char label[CBM_SZ_64];
    char file[CBM_SZ_512];
    int start_line;
    int end_line;
    int in_degree;
    int out_degree;
    int score;
    int match_lines[CBM_SZ_64];
    int match_count;
} search_result_t;

typedef struct {
    uint64_t scope_ms;
    uint64_t scan_ms;
    uint64_t enrich_ms;
    uint64_t elapsed_ms;
    bool include_phase_timings;
} search_metrics_t;

/* Score a result for ranking: project source first, vendored last, tests lowest */
enum { SCORE_FUNC = 10, SCORE_ROUTE = 15, SCORE_VENDORED = -50, SCORE_TEST = -5 };
enum { MAX_LINE_SPAN = 999999 };

static int compute_search_score(const search_result_t *r) {
    int score = r->in_degree;
    if (strcmp(r->label, "Function") == 0 || strcmp(r->label, "Method") == 0) {
        score += SCORE_FUNC;
    }
    if (strcmp(r->label, "Route") == 0) {
        score += SCORE_ROUTE;
    }
    if (strstr(r->file, "vendored/") || strstr(r->file, "vendor/") ||
        strstr(r->file, "node_modules/")) {
        score += SCORE_VENDORED;
    }
    /* Penalize test files */
    if (strstr(r->file, "test") || strstr(r->file, "spec") || strstr(r->file, "_test.")) {
        score += SCORE_TEST;
    }
    return score;
}

static int search_result_cmp(const void *a, const void *b) {
    const search_result_t *ra = (const search_result_t *)a;
    const search_result_t *rb = (const search_result_t *)b;
    return rb->score - ra->score; /* descending */
}

/* Moving an arbitrary file_pattern ahead of Select-String is not generally results-preserving:
 * the current Windows path applies PowerShell -like to the full MatchInfo.Path, while POSIX
 * delegates glob semantics to grep --include. Restrict the Windows optimization to plain suffix
 * globs whose meaning cannot depend on path separator normalization or directory components. The
 * original post-scan filter remains in place as a second guard. */
bool cbm_search_code_file_pattern_can_prefilter(const char *file_pattern) {
    if (!file_pattern || file_pattern[0] != '*' || file_pattern[1] != '.' ||
        file_pattern[2] == '\0') {
        return false;
    }
    for (const unsigned char *p = (const unsigned char *)file_pattern + 2; *p; p++) {
        if (!((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') || (*p >= '0' && *p <= '9') ||
              *p == '.' || *p == '_' || *p == '-')) {
            return false;
        }
    }
    return true;
}

/* Build the grep/search command string based on scoped vs recursive mode.
 * On Windows, uses PowerShell Select-String with tab-delimited output.
 * On POSIX, uses grep with colon-delimited output. */
/* Windows PowerShell 5.1 encodes stdout for a native-process pipe in the
 * console OEM codepage, so any character the inherited CP cannot carry
 * (Cyrillic under CP437/850, etc.) degrades to '?' before it ever reaches
 * collect_grep_matches — and WHETHER it degrades depends on the console the
 * server happened to inherit. Pin the pipe to UTF-8 inside every generated
 * command so raw search content is codepage-independent. (The read side is
 * already safe: Select-String decodes BOM-less UTF-8 via .NET StreamReader
 * defaults.) */

void cbm_search_code_build_grep_cmd(char *cmd, size_t cmd_sz, bool use_regex, bool scoped,
                                    const char *file_pattern, const char *tmpfile,
                                    const char *filelist, const char *root_path) {
#ifdef _WIN32
    const char *sm = use_regex ? "" : " -SimpleMatch";
    if (scoped) {
        if (file_pattern) {
            if (cbm_search_code_file_pattern_can_prefilter(file_pattern)) {
                snprintf(
                    cmd, cmd_sz,
                    "powershell -Command \"" SOURCE_PS_UTF8_PRELUDE
                    "$pat = Get-Content -Encoding UTF8 -LiteralPath '%s'; "
                    "Get-Content -Encoding UTF8 -LiteralPath '%s'"
                    " | Where-Object { $_ -like '%s' }"
                    " | ForEach-Object { Select-String -LiteralPath $_ -Pattern $pat%s "
                    "-ErrorAction SilentlyContinue }"
                    " | Where-Object { $_.Path -like '*%s' }"
                    " | ForEach-Object { $_.Path + [char]9 + $_.LineNumber + [char]9 + $_.Line }\"",
                    tmpfile, filelist, file_pattern, sm, file_pattern);
            } else {
                snprintf(
                    cmd, cmd_sz,
                    "powershell -Command \"" SOURCE_PS_UTF8_PRELUDE
                    "$pat = Get-Content -Encoding UTF8 -LiteralPath '%s'; "
                    "Get-Content -Encoding UTF8 -LiteralPath '%s' | ForEach-Object { Select-String "
                    "-LiteralPath $_ -Pattern $pat%s "
                    "-ErrorAction SilentlyContinue }"
                    " | Where-Object { $_.Path -like '*%s' }"
                    " | ForEach-Object { $_.Path + [char]9 + $_.LineNumber + [char]9 + $_.Line }\"",
                    tmpfile, filelist, sm, file_pattern);
            }
        } else {
            snprintf(
                cmd, cmd_sz,
                "powershell -Command \"" SOURCE_PS_UTF8_PRELUDE
                "$pat = Get-Content -Encoding UTF8 -LiteralPath '%s'; "
                "Get-Content -Encoding UTF8 -LiteralPath '%s' | ForEach-Object { Select-String "
                "-LiteralPath $_ -Pattern $pat%s "
                "-ErrorAction SilentlyContinue }"
                " | ForEach-Object { $_.Path + [char]9 + $_.LineNumber + [char]9 + $_.Line }\"",
                tmpfile, filelist, sm);
        }
    } else {
        if (file_pattern) {
            snprintf(
                cmd, cmd_sz,
                "powershell -Command \"" SOURCE_PS_UTF8_PRELUDE
                "Get-ChildItem -Recurse -Path '%s\\*' -Include '%s' -File "
                "-ErrorAction SilentlyContinue"
                " | Select-String -Pattern (Get-Content -Encoding UTF8 -LiteralPath '%s')%s "
                "-ErrorAction SilentlyContinue"
                " | ForEach-Object { $_.Path + [char]9 + $_.LineNumber + [char]9 + $_.Line }\"",
                root_path, file_pattern, tmpfile, sm);
        } else {
            snprintf(
                cmd, cmd_sz,
                "powershell -Command \"" SOURCE_PS_UTF8_PRELUDE
                "Get-ChildItem -Recurse -Path '%s\\*' -File -ErrorAction "
                "SilentlyContinue"
                " | Select-String -Pattern (Get-Content -Encoding UTF8 -LiteralPath '%s')%s "
                "-ErrorAction SilentlyContinue"
                " | ForEach-Object { $_.Path + [char]9 + $_.LineNumber + [char]9 + $_.Line }\"",
                root_path, tmpfile, sm);
        }
    }
#else
    const char *flag = use_regex ? "-E" : "-F";
    if (scoped) {
        if (file_pattern) {
            /* -0: read NUL-separated paths from the filelist so paths containing
             * spaces stay one argument (issue #687). Pairs with the NUL separator
             * written by write_scoped_filelist. */
            snprintf(cmd, cmd_sz,
                     "xargs -0 sh -c 'grep -Hn -d skip %s --include=\"%s\" -f \"%s\" \"$@\"; "
                     "status=$?; [ \"$status\" -eq 0 ] || [ \"$status\" -eq 1 ]' sh < '%s' "
                     "2>/dev/null",
                     flag, file_pattern, tmpfile, filelist);
        } else {
            snprintf(cmd, cmd_sz,
                     "xargs -0 sh -c 'grep -Hn -d skip %s -f \"%s\" \"$@\"; status=$?; "
                     "[ \"$status\" -eq 0 ] || [ \"$status\" -eq 1 ]' sh < '%s' 2>/dev/null",
                     flag, tmpfile, filelist);
        }
    } else {
        if (file_pattern) {
            snprintf(cmd, cmd_sz, "grep -rn %s --include='%s' -f '%s' '%s' 2>/dev/null", flag,
                     file_pattern, tmpfile, root_path);
        } else {
            snprintf(cmd, cmd_sz, "grep -rn %s -f '%s' '%s' 2>/dev/null", flag, tmpfile, root_path);
        }
    }
#endif
}

/* Build deduplicated file list from search results + raw matches. */
static yyjson_mut_val *build_dedup_files_array(yyjson_mut_doc *doc, search_result_t *sr,
                                               int output_count, grep_match_t *raw, int raw_count) {
    yyjson_mut_val *files_arr = yyjson_mut_arr(doc);
    char *seen_files[CBM_SZ_512];
    int seen_count = 0;
    for (int fi = 0; fi < output_count; fi++) {
        bool dup = false;
        for (int j = 0; j < seen_count; j++) {
            if (strcmp(seen_files[j], sr[fi].file) == 0) {
                dup = true;
                break;
            }
        }
        if (!dup && seen_count < CBM_SZ_512) {
            seen_files[seen_count++] = sr[fi].file;
            yyjson_mut_arr_add_str(doc, files_arr, sr[fi].file);
        }
    }
    for (int fi = 0; fi < raw_count && seen_count < CBM_SZ_512; fi++) {
        bool dup = false;
        for (int j = 0; j < seen_count; j++) {
            if (strcmp(seen_files[j], raw[fi].file) == 0) {
                dup = true;
                break;
            }
        }
        if (!dup) {
            seen_files[seen_count++] = raw[fi].file;
            yyjson_mut_arr_add_str(doc, files_arr, raw[fi].file);
        }
    }
    return files_arr;
}

/* Attach source or context lines to a search result JSON item. */
static void attach_result_source(yyjson_mut_doc *doc, yyjson_mut_val *item, search_result_t *r,
                                 int mode, int context_lines, const char *root_path) {
    enum { MODE_FULL = 1 };
    if (r->start_line <= 0 || r->end_line <= 0) {
        return;
    }
    char abs_path[CBM_SZ_1K];
    snprintf(abs_path, sizeof(abs_path), "%s/%s", root_path, r->file);

    /* Containment: a search result whose indexed path resolves outside the
     * project root (a `..` segment, or a symlink/junction that discovery
     * followed) must not be read back into the response. Same guard the
     * snippet path already uses. */
    if (!cbm_path_within_root(root_path, abs_path)) {
        return;
    }

    if (mode == MODE_FULL) {
        /* Cap each hit's source at a match-anchored window: uncapped
         * whole-symbol dumps ran to 5.7KB × N hits (142KB responses). The
         * complete symbol stays one get_code_snippet call away;
         * source_start/source_truncated make the cut explicit. */
        enum { SC_FULL_MAX_LINES = 60, SC_FULL_LEAD = 5 };
        int s = r->start_line;
        int e = r->end_line;
        bool truncated = false;
        if (e - s + 1 > SC_FULL_MAX_LINES) {
            if (r->match_count > 0 && r->match_lines[0] - SC_FULL_LEAD > s) {
                s = r->match_lines[0] - SC_FULL_LEAD;
            }
            e = s + SC_FULL_MAX_LINES - 1;
            if (e > r->end_line) {
                e = r->end_line;
            }
            truncated = true;
        }
        char *source = source_read_file_lines(abs_path, s, e);
        if (source) {
            char *safe_source = source_sanitize_utf8_lossy(source);
            if (safe_source) {
                yyjson_mut_obj_add_strcpy(doc, item, "source", safe_source);
                free(safe_source);
            }
            free(source);
            if (truncated) {
                yyjson_mut_obj_add_int(doc, item, "source_start", s);
                yyjson_mut_obj_add_bool(doc, item, "source_truncated", true);
            }
        }
    } else if (context_lines > 0 && r->match_count > 0) {
        int ctx_start = r->match_lines[0] - context_lines;
        int ctx_end = r->match_lines[r->match_count - SOURCE_SKIP_ONE] + context_lines;
        if (ctx_start < SOURCE_SKIP_ONE) {
            ctx_start = SOURCE_SKIP_ONE;
        }
        char *ctx = source_read_file_lines(abs_path, ctx_start, ctx_end);
        if (ctx) {
            char *safe_context = source_sanitize_utf8_lossy(ctx);
            if (safe_context) {
                yyjson_mut_obj_add_strcpy(doc, item, "context", safe_context);
                free(safe_context);
            }
            yyjson_mut_obj_add_int(doc, item, "context_start", ctx_start);
            free(ctx);
        }
    }
}

/* Build directory distribution object from search results (top-level dir → count). */
/* Aggregate hits by top-level directory. Shared by the JSON object and the
 * TOON table emission. Returns the number of distinct directories. */
static int aggregate_search_dirs(search_result_t *sr, int sr_count, char dir_names[][CBM_SZ_128],
                                 int *dir_counts, int max_dirs) {
    int dir_n = 0;
    for (int di = 0; di < sr_count; di++) {
        char top[CBM_SZ_128] = "";
        const char *slash = strchr(sr[di].file, '/');
        if (slash) {
            size_t dlen = (size_t)(slash - sr[di].file + SOURCE_SKIP_ONE);
            if (dlen >= sizeof(top)) {
                dlen = sizeof(top) - SOURCE_SKIP_ONE;
            }
            memcpy(top, sr[di].file, dlen);
            top[dlen] = '\0';
        } else {
            snprintf(top, sizeof(top), "%s", sr[di].file);
        }
        int found = CBM_NOT_FOUND;
        for (int d = 0; d < dir_n; d++) {
            if (strcmp(dir_names[d], top) == 0) {
                found = d;
                break;
            }
        }
        if (found >= 0) {
            dir_counts[found]++;
        } else if (dir_n < max_dirs) {
            snprintf(dir_names[dir_n], CBM_SZ_128, "%s", top);
            dir_counts[dir_n] = SOURCE_SKIP_ONE;
            dir_n++;
        }
    }
    return dir_n;
}

static yyjson_mut_val *build_dir_distribution(yyjson_mut_doc *doc, search_result_t *sr,
                                              int sr_count) {
    yyjson_mut_val *dirs = yyjson_mut_obj(doc);
    char dir_names[CBM_SZ_64][CBM_SZ_128];
    int dir_counts[CBM_SZ_64];
    int dir_n = aggregate_search_dirs(sr, sr_count, dir_names, dir_counts, CBM_SZ_64);
    for (int d = 0; d < dir_n; d++) {
        yyjson_mut_val *key = yyjson_mut_strcpy(doc, dir_names[d]);
        yyjson_mut_val *val = yyjson_mut_int(doc, dir_counts[d]);
        yyjson_mut_obj_add(dirs, key, val);
    }
    return dirs;
}

/* TOON emission for compact-mode search results: one row per hit
 * (qn/label/file/lines/matches/degrees — `node` dropped, it duplicates the
 * qn's last segment), a raw[] table for uncorrelated matches, a dirs[]
 * distribution table, and the summary scalars. */
static char *assemble_search_output_toon(search_result_t *sr, int sr_count, grep_match_t *raw,
                                         int raw_count, int gm_count, int limit,
                                         bool warn_literal_pipe, const search_metrics_t *metrics) {
    enum { MAX_RAW = 20, SEARCH_SLOW_MS = 5000 };
    cbm_sb_t sb;
    cbm_sb_init(&sb);

    int output_count = sr_count < limit ? sr_count : limit;
    static const char *const cols[] = {"qn", "label", "file", "lines", "matches", "in", "out"};
    cbm_tree_table_header(&sb, "results", output_count, cols, 7);
    for (int ri = 0; ri < output_count; ri++) {
        search_result_t *r = &sr[ri];
        char lines[CBM_SZ_32];
        if (r->start_line > 0) {
            snprintf(lines, sizeof(lines), "%d-%d", r->start_line,
                     r->end_line > r->start_line ? r->end_line : r->start_line);
        } else {
            lines[0] = '\0';
        }
        /* match line numbers ';'-joined (no comma → no cell quoting) */
        char matches[CBM_SZ_256];
        size_t mpos = 0;
        matches[0] = '\0';
        for (int j = 0; j < r->match_count && mpos + 12 < sizeof(matches); j++) {
            int n = snprintf(matches + mpos, sizeof(matches) - mpos, "%s%d", j > 0 ? ";" : "",
                             r->match_lines[j]);
            if (n < 0) {
                break;
            }
            mpos += (size_t)n;
        }
        cbm_tree_row_begin(&sb);
        cbm_tree_cell_str(&sb, r->qualified_name, true);
        cbm_tree_cell_str(&sb, r->label, false);
        cbm_tree_cell_str(&sb, r->file, false);
        cbm_tree_cell_str(&sb, lines, false);
        cbm_tree_cell_str(&sb, matches, false);
        cbm_tree_cell_int(&sb, r->in_degree, false);
        cbm_tree_cell_int(&sb, r->out_degree, false);
        cbm_tree_row_end(&sb);
    }

    int raw_output = raw_count < MAX_RAW ? raw_count : MAX_RAW;
    if (raw_output > 0) {
        static const char *const rcols[] = {"file", "line", "content"};
        cbm_tree_table_header(&sb, "raw", raw_output, rcols, 3);
        for (int ri = 0; ri < raw_output; ri++) {
            cbm_tree_row_begin(&sb);
            cbm_tree_cell_str(&sb, raw[ri].file, true);
            cbm_tree_cell_int(&sb, raw[ri].line, false);
            cbm_tree_cell_str(&sb, raw[ri].content, false);
            cbm_tree_row_end(&sb);
        }
    }

    char dir_names[CBM_SZ_64][CBM_SZ_128];
    int dir_counts[CBM_SZ_64];
    int dir_n = aggregate_search_dirs(sr, sr_count, dir_names, dir_counts, CBM_SZ_64);
    if (dir_n > 0) {
        static const char *const dcols[] = {"dir", "hits"};
        cbm_tree_table_header(&sb, "dirs", dir_n, dcols, 2);
        for (int d = 0; d < dir_n; d++) {
            cbm_tree_row_begin(&sb);
            cbm_tree_cell_str(&sb, dir_names[d], true);
            cbm_tree_cell_int(&sb, dir_counts[d], false);
            cbm_tree_row_end(&sb);
        }
    }

    cbm_tree_scalar_int(&sb, "total_grep_matches", gm_count);
    cbm_tree_scalar_int(&sb, "total_results", sr_count);
    cbm_tree_scalar_int(&sb, "raw_match_count", raw_count);
    if (metrics->include_phase_timings) {
        cbm_tree_scalar_int(&sb, "scope_ms", (long long)metrics->scope_ms);
        cbm_tree_scalar_int(&sb, "scan_ms", (long long)metrics->scan_ms);
        cbm_tree_scalar_int(&sb, "enrich_ms", (long long)metrics->enrich_ms);
    }
    cbm_tree_scalar_int(&sb, "elapsed_ms", (long long)metrics->elapsed_ms);
    if (warn_literal_pipe) {
        cbm_tree_scalar_str(&sb, "warning",
                            "pattern contains '|' but regex=false, so it is matched literally "
                            "(not as alternation). Pass regex=true for 'foo|bar' to mean "
                            "'foo OR bar'.");
    }
    if (metrics->elapsed_ms >= SEARCH_SLOW_MS) {
        cbm_tree_scalar_str(&sb, "warning_slow",
                            "search was slow; narrow file_pattern/path_filter or use a more "
                            "specific pattern");
    }
    return cbm_sb_finish(&sb);
}

/* Phase 4: assemble JSON output from search results */
static char *assemble_search_output(search_result_t *sr, int sr_count, grep_match_t *raw,
                                    int raw_count, int gm_count, int limit, int mode,
                                    int context_lines, const char *root_path,
                                    bool warn_literal_pipe, const search_metrics_t *metrics) {
    enum { MODE_COMPACT = 0, MODE_FULL = 1, MODE_FILES = 2, SEARCH_SLOW_MS = 5000 };

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    int output_count = sr_count < limit ? sr_count : limit;

    if (mode == MODE_FILES) {
        yyjson_mut_obj_add_val(doc, root_obj, "files",
                               build_dedup_files_array(doc, sr, output_count, raw, raw_count));
    } else {
        /* json-stringified tree: cols + column-ordered row arrays. FULL mode
         * appends a per-row object cell with the (guarded, windowed) source;
         * context requests append the corresponding context object. */
        bool attach_context = context_lines > 0 && mode != MODE_FULL;
        yyjson_mut_val *jcols = yyjson_mut_arr(doc);
        static const char *const sc_cols[] = {"qn",      "label", "file", "lines",
                                              "matches", "in",    "out"};
        for (size_t ci = 0; ci < sizeof(sc_cols) / sizeof(sc_cols[0]); ci++) {
            yyjson_mut_arr_add_str(doc, jcols, sc_cols[ci]);
        }
        if (mode == MODE_FULL || attach_context) {
            yyjson_mut_arr_add_str(doc, jcols, mode == MODE_FULL ? "source" : "context");
        }
        yyjson_mut_obj_add_val(doc, root_obj, "cols", jcols);

        yyjson_mut_val *results_arr = yyjson_mut_arr(doc);
        for (int ri = 0; ri < output_count; ri++) {
            search_result_t *r = &sr[ri];
            char lines[CBM_SZ_32];
            if (r->start_line > 0) {
                snprintf(lines, sizeof(lines), "%d-%d", r->start_line,
                         r->end_line > r->start_line ? r->end_line : r->start_line);
            } else {
                lines[0] = '\0';
            }
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row, r->qualified_name);
            yyjson_mut_arr_add_strcpy(doc, row, r->label);
            yyjson_mut_arr_add_strcpy(doc, row, r->file);
            yyjson_mut_arr_add_strcpy(doc, row, lines);
            yyjson_mut_val *ml = yyjson_mut_arr(doc);
            for (int j = 0; j < r->match_count; j++) {
                yyjson_mut_arr_add_int(doc, ml, r->match_lines[j]);
            }
            yyjson_mut_arr_add_val(row, ml);
            yyjson_mut_arr_add_int(doc, row, r->in_degree);
            yyjson_mut_arr_add_int(doc, row, r->out_degree);
            if (mode == MODE_FULL || attach_context) {
                yyjson_mut_val *src = yyjson_mut_obj(doc);
                attach_result_source(doc, src, r, mode, context_lines, root_path);
                yyjson_mut_arr_add_val(row, src);
            }
            yyjson_mut_arr_add_val(results_arr, row);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "rows", results_arr);

        enum { MAX_RAW = 20 };
        yyjson_mut_val *raw_obj = yyjson_mut_obj(doc);
        yyjson_mut_val *rcols = yyjson_mut_arr(doc);
        yyjson_mut_arr_add_str(doc, rcols, "file");
        yyjson_mut_arr_add_str(doc, rcols, "line");
        yyjson_mut_arr_add_str(doc, rcols, "content");
        yyjson_mut_obj_add_val(doc, raw_obj, "cols", rcols);
        yyjson_mut_val *raw_arr = yyjson_mut_arr(doc);
        int raw_output = raw_count < MAX_RAW ? raw_count : MAX_RAW;
        for (int ri = 0; ri < raw_output; ri++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_str(doc, row, raw[ri].file);
            yyjson_mut_arr_add_int(doc, row, raw[ri].line);
            yyjson_mut_arr_add_str(doc, row, raw[ri].content);
            yyjson_mut_arr_add_val(raw_arr, row);
        }
        yyjson_mut_obj_add_val(doc, raw_obj, "rows", raw_arr);
        yyjson_mut_obj_add_val(doc, root_obj, "raw_matches", raw_obj);
    }

    yyjson_mut_obj_add_val(doc, root_obj, "directories", build_dir_distribution(doc, sr, sr_count));

    /* Summary stats */
    yyjson_mut_obj_add_int(doc, root_obj, "total_grep_matches", gm_count);
    yyjson_mut_obj_add_int(doc, root_obj, "total_results", sr_count);
    yyjson_mut_obj_add_int(doc, root_obj, "raw_match_count", raw_count);
    if (metrics->include_phase_timings) {
        yyjson_mut_obj_add_uint(doc, root_obj, "scope_ms", metrics->scope_ms);
        yyjson_mut_obj_add_uint(doc, root_obj, "scan_ms", metrics->scan_ms);
        yyjson_mut_obj_add_uint(doc, root_obj, "enrich_ms", metrics->enrich_ms);
    }
    yyjson_mut_obj_add_uint(doc, root_obj, "elapsed_ms", metrics->elapsed_ms);
    if (sr_count > 0 && gm_count > 0) {
        char ratio[CBM_SZ_32];
        snprintf(ratio, sizeof(ratio), "%.1fx", (double)gm_count / (double)(sr_count + raw_count));
        yyjson_mut_obj_add_strcpy(doc, root_obj, "dedup_ratio", ratio);
    }

    /* Warnings: surface common foot-guns instead of leaving them silent. */
    yyjson_mut_val *warnings = yyjson_mut_arr(doc);
    if (warn_literal_pipe) {
        yyjson_mut_arr_add_strcpy(
            doc, warnings,
            "pattern contains '|' but regex=false, so it is matched literally (not as "
            "alternation). Pass regex=true for 'foo|bar' to mean 'foo OR bar'.");
    }
    if (metrics->elapsed_ms >= SEARCH_SLOW_MS) {
        char slow[CBM_SZ_128];
        snprintf(slow, sizeof(slow),
                 "search took %dms (>%ds); narrow file_pattern/path_filter or use a more "
                 "specific pattern",
                 (int)metrics->elapsed_ms, SEARCH_SLOW_MS / 1000);
        yyjson_mut_arr_add_strcpy(doc, warnings, slow);
        char ems[CBM_SZ_32];
        snprintf(ems, sizeof(ems), "%d", (int)metrics->elapsed_ms);
        cbm_log_warn("search.slow", "elapsed_ms", ems); /* visibility in logs */
    }
    if (yyjson_mut_arr_size(warnings) > 0) {
        yyjson_mut_obj_add_val(doc, root_obj, "warnings", warnings);
    }

    char *json = source_doc_to_str(doc);
    if (json) {
        char *safe_json = source_sanitize_utf8_lossy(json);
        if (safe_json) {
            free(json);
            json = safe_json;
        }
    }
    yyjson_mut_doc_free(doc);

    return json;
}

/* Read grep output from fp, parse file:line:content format, apply path filter,
 * and return a dynamically-allocated grep_match_t array. */
/* Strip root path prefix from a file path. */
static const char *strip_root_prefix(const char *path, const char *root, size_t root_len) {
    if (strncmp(path, root, root_len) != 0) {
        return path;
    }
    const char *p = path + root_len;
    if (*p == '/') {
        p++;
    }
    return p;
}

static grep_match_t *collect_grep_matches(FILE *fp, const char *root_path, size_t root_len,
                                          bool has_path_filter, cbm_regex_t *path_regex,
                                          int grep_limit, int *out_count) {
    int gm_cap = CBM_SZ_64;
    int gm_count = 0;
    grep_match_t *gm = malloc(gm_cap * sizeof(grep_match_t));
    char line[CBM_SZ_2K];

    while (fgets(line, sizeof(line), fp) && gm_count < grep_limit) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - SOURCE_SKIP_ONE] == '\n' || line[len - SOURCE_SKIP_ONE] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

        /* PowerShell output uses tab as delimiter (paths may contain colons
         * on Windows, e.g. C:\dir\file). Unix grep uses colon. */
#ifdef _WIN32
        char sep = '\t';
#else
        char sep = ':';
#endif
        char *sep1 = strchr(line, (unsigned char)sep);
        if (!sep1) {
            continue;
        }
        char *sep2 = strchr(sep1 + SOURCE_SKIP_ONE, (unsigned char)sep);
        if (!sep2) {
            continue;
        }
        *sep1 = '\0';
        *sep2 = '\0';

#ifdef _WIN32
        cbm_normalize_path_sep(line);
#endif
        const char *path = line;
        const char *file = strip_root_prefix(path, root_path, root_len);

        if (has_path_filter && cbm_regexec(path_regex, file, 0, NULL, 0) != CBM_REG_OK) {
            continue;
        }

        safe_grow(gm, gm_count, gm_cap, SOURCE_PAIR_LEN);
        snprintf(gm[gm_count].file, sizeof(gm[0].file), "%s", file);
        gm[gm_count].line = (int)strtol(sep1 + SOURCE_SKIP_ONE, NULL, CBM_DECIMAL_BASE);
        char *safe_content = source_sanitize_utf8_lossy(sep2 + SOURCE_SKIP_ONE);
        snprintf(gm[gm_count].content, sizeof(gm[0].content), "%s",
                 safe_content ? safe_content : sep2 + SOURCE_SKIP_ONE);
        free(safe_content);
        gm_count++;
    }

    *out_count = gm_count;
    return gm;
}

/* Find the tightest node containing a line in a file. Returns index or -1. */
static int find_tightest_node(cbm_node_t *nodes, int count, int line) {
    int best = CBM_NOT_FOUND;
    int best_span = MAX_LINE_SPAN;
    for (int j = 0; j < count; j++) {
        if (nodes[j].start_line <= line && nodes[j].end_line >= line) {
            int span = nodes[j].end_line - nodes[j].start_line;
            if (span < best_span) {
                best = j;
                best_span = span;
            }
        }
    }
    return best;
}

/* Add a grep hit to the search result set (merge into existing or create new). */
static void add_to_search_results(search_result_t **sr, int *sr_count, int *sr_cap, cbm_node_t *n,
                                  int line) {
    for (int j = 0; j < *sr_count; j++) {
        if ((*sr)[j].node_id == n->id) {
            if ((*sr)[j].match_count < CBM_SZ_64) {
                (*sr)[j].match_lines[(*sr)[j].match_count++] = line;
            }
            return;
        }
    }
    if (*sr_count >= *sr_cap) {
        *sr_cap *= SOURCE_PAIR_LEN;
        *sr = safe_realloc(*sr, *sr_cap * sizeof(search_result_t));
        memset(&(*sr)[*sr_count], 0, (*sr_cap - *sr_count) * sizeof(search_result_t));
    }
    search_result_t *r = &(*sr)[*sr_count];
    r->node_id = n->id;
    snprintf(r->node_name, sizeof(r->node_name), "%s", n->name ? n->name : "");
    snprintf(r->qualified_name, sizeof(r->qualified_name), "%s",
             n->qualified_name ? n->qualified_name : "");
    snprintf(r->label, sizeof(r->label), "%s", n->label ? n->label : "");
    snprintf(r->file, sizeof(r->file), "%s", n->file_path ? n->file_path : "");
    r->start_line = n->start_line;
    r->end_line = n->end_line;
    r->match_lines[0] = line;
    r->match_count = SOURCE_SKIP_ONE;
    (*sr_count)++;
}

/* Match a single grep hit to the tightest containing node, then add to sr or raw. */
static void classify_grep_hit(grep_match_t *hit, cbm_node_t *file_nodes, int file_node_count,
                              search_result_t **sr, int *sr_count, int *sr_cap, grep_match_t **raw,
                              int *raw_count, int *raw_cap) {
    int best = find_tightest_node(file_nodes, file_node_count, hit->line);
    if (best >= 0) {
        add_to_search_results(sr, sr_count, sr_cap, &file_nodes[best], hit->line);
    } else {
        if (*raw_count >= *raw_cap) {
            *raw_cap = (*raw_cap == 0) ? CBM_SZ_32 : *raw_cap * SOURCE_PAIR_LEN;
            *raw = safe_realloc(*raw, *raw_cap * sizeof(grep_match_t));
        }
        if (*raw) {
            (*raw)[(*raw_count)++] = *hit;
        }
    }
}

/* Free a file_nodes array returned from cbm_store_find_nodes_by_file. */
static void free_file_nodes(cbm_node_t *nodes, int count) {
    for (int j = 0; j < count; j++) {
        safe_str_free(&nodes[j].project);
        safe_str_free(&nodes[j].label);
        safe_str_free(&nodes[j].name);
        safe_str_free(&nodes[j].qualified_name);
        safe_str_free(&nodes[j].file_path);
        safe_str_free(&nodes[j].properties_json);
    }
    free(nodes);
}

/* Classify all grep matches file-by-file into search results and raw hits. */
static void classify_all_grep_hits(grep_match_t *gm, int gm_count, cbm_store_t *store,
                                   const char *project, search_result_t **sr, int *sr_count,
                                   int *sr_cap, grep_match_t **raw, int *raw_count, int *raw_cap) {
    qsort(gm, gm_count, sizeof(grep_match_t), (int (*)(const void *, const void *))strcmp);
    int i = 0;
    while (i < gm_count) {
        const char *cur_file = gm[i].file;
        int file_start = i;
        while (i < gm_count && strcmp(gm[i].file, cur_file) == 0) {
            i++;
        }
        cbm_node_t *file_nodes = NULL;
        int file_node_count = 0;
        if (store) {
            cbm_store_find_nodes_by_file(store, project, cur_file, &file_nodes, &file_node_count);
        }
        for (int mi = file_start; mi < i; mi++) {
            classify_grep_hit(&gm[mi], file_nodes, file_node_count, sr, sr_count, sr_cap, raw,
                              raw_count, raw_cap);
        }
        free_file_nodes(file_nodes, file_node_count);
    }
}

/* Write indexed file list for scoped grep. Returns true if scoped.
 * When a path_filter is provided, apply it here — before grep — so large
 * indexed projects do not scan files only for collect_grep_matches to discard
 * them later. The predicate is IDENTICAL to the post-grep filter: the same
 * compiled regex run against the same root-relative path (separators
 * normalized on Windows first), so prefiltering can only skip files whose
 * hits would be dropped anyway — results-preserving by construction.
 * *out_written receives the number of records written (0 = the filter
 * excluded every indexed file).
 *
 * `fl` is the caller's already-open binary stream on the descriptor cbm_mkstemp
 * created inside the private scratch directory; this function never opens or
 * closes it, so the list is never reachable through a predictable pathname. */
static bool write_scoped_filelist(cbm_store_t *pre_store, const char *project, const char *root_path,
                                  FILE *fl, bool has_path_filter, cbm_regex_t *path_regex,
                                  int *out_written) {
    *out_written = 0;
    if (!pre_store) {
        return false;
    }
    char **indexed_files = NULL;
    int indexed_count = 0;
    int list_rc = cbm_store_list_files(pre_store, project, &indexed_files, &indexed_count);
    if (list_rc != CBM_STORE_OK || indexed_count == 0) {
        for (int fi = 0; fi < indexed_count; fi++) {
            free(indexed_files[fi]);
        }
        free(indexed_files);
        return false;
    }
    bool ok = false;
    int written = 0;
    if (fl) {
        ok = true;
        for (int fi = 0; fi < indexed_count; fi++) {
            /* A source path never legitimately contains a newline or carriage
             * return. Those bytes are exactly the record separator on the
             * Windows filelist (and would split naive line readers elsewhere),
             * so a crafted indexed path with an embedded newline could inject
             * an extra entry into the scan set. Skip such paths entirely. */
            if (strpbrk(indexed_files[fi], "\r\n") != NULL) {
                continue;
            }
            if (has_path_filter && path_regex) {
#ifdef _WIN32
                cbm_normalize_path_sep(indexed_files[fi]);
#endif
                if (cbm_regexec(path_regex, indexed_files[fi], 0, NULL, 0) != CBM_REG_OK) {
                    continue;
                }
            }
            size_t root_len = strlen(root_path);
            size_t file_len = strlen(indexed_files[fi]);
            if (root_len > SIZE_MAX - file_len - 2) {
                continue;
            }
            size_t scan_path_len = root_len + 1 + file_len;
            char *scan_path = malloc(scan_path_len + 1);
            if (!scan_path) {
                ok = false;
                break;
            }
            memcpy(scan_path, root_path, root_len);
            scan_path[root_len] = '/';
            memcpy(scan_path + root_len + 1, indexed_files[fi], file_len + 1);

            /* Incremental stores can retain structural directory nodes and
             * briefly stale deleted-file paths. Neither is a content-scan
             * operand. Filter them before spawning so an expected stale entry
             * cannot turn otherwise valid matches into grep status 2. This
             * deliberately does not follow symlinks/reparse points. */
            cbm_path_info_t path_info;
            if (cbm_path_info_utf8(scan_path, &path_info) != 0 || !path_info.is_regular) {
                free(scan_path);
                continue;
            }
            /* Write "<root>/<file>" piece-by-piece (no fixed-size buffer, so an
             * arbitrarily long absolute path cannot overflow). Forward slash join
             * so xargs doesn't treat Windows backslashes as escapes; binary mode
             * (wb) prevents CRLF translation. Record separator differs by platform:
             *   - Unix: NUL, consumed by `xargs -0` — handles spaces in paths (a
             *     newline separator would split plain xargs on the space).
             *   - Windows: newline, consumed by PowerShell `Get-Content |
             *     Select-String -LiteralPath` (NUL bytes break Get-Content). */
            (void)fwrite(scan_path, 1, scan_path_len, fl);
            free(scan_path);
#ifdef _WIN32
            (void)fputc('\n', fl);
#else
            (void)fputc('\0', fl);
#endif
            written++;
        }
        /* The stream stays open — the caller owns it and closes it (flushing
         * these records to disk) before the grep subprocess reads the list. */
    }
    for (int fi = 0; fi < indexed_count; fi++) {
        free(indexed_files[fi]);
    }
    free(indexed_files);
    *out_written = written;
    return ok;
}

/* Parse search mode string (0=compact, 1=full, 2=files). */
static int parse_search_mode(const char *mode_str) {
    if (!mode_str) {
        return 0;
    }
    if (strcmp(mode_str, "full") == 0) {
        return SOURCE_SKIP_ONE;
    }
    if (strcmp(mode_str, "files") == 0) {
        return SOURCE_RETURN_FILES;
    }
    return 0;
}

/* Validate shell-safe arguments for search. */
/* Search/grep paths and globs are ALWAYS single-quoted (POSIX sh) or
 * double-/single-quoted (Windows cmd/PowerShell) on the command line, which
 * neutralises '&' — a very common character in real paths (R&D, "Foo & Bar",
 * OneDrive). Accept '&' here while still rejecting every metacharacter that
 * could break out of the quoting (#272). */
static bool validate_search_path_arg(const char *s) {
    if (!s) {
        return false;
    }
    for (const char *p = s; *p; p++) {
        switch (*p) {
        case '\'':
        case '"':
        case ';':
        case '|':
        case '$':
        case '`':
        case '<':
        case '>':
        case '\n':
        case '\r':
#ifndef _WIN32
        case '\\':
#endif
            return false;
        default:
            break;
        }
    }
    return true;
}

/* These characters retain command-language meaning inside quoted cmd.exe
 * arguments: percent expands environment variables, exclamation can expand
 * delayed variables, and caret changes parsing. Never interpolate them from a
 * stored project root or request branch into the Windows detect_changes payload.
 * /V:OFF is defense in depth for exclamation; validation remains the boundary. */
static __attribute__((unused)) bool validate_windows_cmd_interpolation_arg(const char *s) {
#ifdef _WIN32
    return s && strpbrk(s, "%!^") == NULL;
#else
    return s != NULL;
#endif
}

static bool validate_search_args(const char *root_path, const char *file_pattern) {
    if (!validate_search_path_arg(root_path)) {
        return false;
    }
    if (file_pattern && !validate_search_path_arg(file_pattern)) {
        return false;
    }
    return true;
}

/* Private scratch for one search_code scan: the grep -f pattern file and the
 * scoped file list.
 *
 * Both used to be fixed, guessable paths derived from the pid —
 * "<tmp>/cbm_search_<pid>.pat" and its ".files" companion — opened with a plain
 * fopen. Another local user could pre-plant a symlink at either name and
 * redirect the write; two searches in the same process could also collide on
 * them. Now both live inside a directory created by cbm_mkdtemp (0700 on POSIX,
 * an explicit owner-only DACL on Windows) under an unguessable XXXXXX suffix,
 * and each file is created by cbm_mkstemp — O_CREAT|O_EXCL at mode 0600, so the
 * create fails rather than following anything already at the name. Every write
 * goes through the descriptor cbm_mkstemp returned; neither path is ever
 * reopened by name.
 *
 * Sizing: cbm_mkdtemp copies its expanded result back into `dir`, and its own
 * internal buffer is CBM_SZ_512, so `dir` must be at least that big to receive
 * it. The two file paths are `dir` plus a short basename. */
typedef struct {
    char dir[CBM_SZ_512];
    char pattern_path[CBM_SZ_1K];
    char filelist_path[CBM_SZ_1K];
    FILE *filelist; /* held open for write_scoped_filelist; closed by the caller */
} search_scratch_t;


/* Create <scratch>/<basename>-XXXXXX exclusively and return a stream on the
 * descriptor. On failure `path_out` is emptied so cleanup skips it. */
static FILE *search_scratch_file(const char *dir, const char *basename, char *path_out,
                                 size_t path_sz) {
    path_out[0] = '\0';
    int written = snprintf(path_out, path_sz, "%s/%s-XXXXXX", dir, basename);
    if (written <= 0 || (size_t)written >= path_sz) {
        path_out[0] = '\0';
        return NULL;
    }
    int descriptor = cbm_mkstemp(path_out);
    if (descriptor < 0) {
        path_out[0] = '\0';
        return NULL;
    }
    /* Binary mode: the file list uses an explicit per-platform record separator
     * (NUL for xargs -0, newline for PowerShell) that CRLF translation would
     * corrupt — the same reason the previous code opened it "wb". */
    FILE *stream = source_fdopen(descriptor, "wb");
    if (!stream) {
        (void)source_close(descriptor);
        (void)cbm_unlink(path_out);
        path_out[0] = '\0';
    }
    return stream;
}

/* Anchored cleanup: removes both scratch files and the private directory. Safe
 * to call more than once and on any partially-initialised scratch, so every
 * exit from handle_search_code can call it unconditionally. rmdir succeeding is
 * itself the proof nothing was left inside. */
static void search_scratch_close(search_scratch_t *scratch) {
    if (scratch->filelist) {
        (void)fclose(scratch->filelist);
        scratch->filelist = NULL;
    }
    if (scratch->pattern_path[0] != '\0') {
        (void)cbm_unlink(scratch->pattern_path);
        scratch->pattern_path[0] = '\0';
    }
    if (scratch->filelist_path[0] != '\0') {
        (void)cbm_unlink(scratch->filelist_path);
        scratch->filelist_path[0] = '\0';
    }
    if (scratch->dir[0] != '\0') {
        (void)cbm_rmdir(scratch->dir);
        scratch->dir[0] = '\0';
    }
}

/* Open the scratch directory, write `pattern` to the grep -f file, and leave the
 * file list open for write_scoped_filelist. Returns true on success; on failure
 * everything already created is removed before returning. */
static bool search_scratch_open(search_scratch_t *scratch, const char *pattern) {
    scratch->dir[0] = '\0';
    scratch->pattern_path[0] = '\0';
    scratch->filelist_path[0] = '\0';
    scratch->filelist = NULL;

    int written =
        snprintf(scratch->dir, sizeof(scratch->dir), "%s/cbm-search-XXXXXX", cbm_tmpdir());
    if (written <= 0 || (size_t)written >= sizeof(scratch->dir) || !cbm_mkdtemp(scratch->dir)) {
        scratch->dir[0] = '\0';
        return false;
    }

    FILE *pattern_file = search_scratch_file(scratch->dir, "pat", scratch->pattern_path,
                                             sizeof(scratch->pattern_path));
    if (!pattern_file) {
        search_scratch_close(scratch);
        return false;
    }
    bool ok = fprintf(pattern_file, "%s\n", pattern) >= 0;
    ok = fclose(pattern_file) == 0 && ok;
    if (!ok) {
        search_scratch_close(scratch);
        return false;
    }

    scratch->filelist = search_scratch_file(scratch->dir, "files", scratch->filelist_path,
                                            sizeof(scratch->filelist_path));
    if (!scratch->filelist) {
        search_scratch_close(scratch);
        return false;
    }
    return true;
}

/* Compile a path filter regex. Returns true if compiled successfully. */
static bool compile_path_filter(const char *filter, cbm_regex_t *re) {
    if (!filter || !filter[0]) {
        return false;
    }
    return cbm_regcomp(re, filter, CBM_REG_EXTENDED | CBM_REG_NOSUB) == CBM_REG_OK;
}

static char *search_code_timeout_payload(void) {
    static const char fallback[] =
        "{\"code\":\"request_timeout\",\"message\":\"search_code scan exceeded its execution deadline\"}";
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) {
        if (doc) yyjson_mut_doc_free(doc);
        return source_strdup(fallback);
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "code", "request_timeout");
    yyjson_mut_obj_add_str(doc, root, "message",
                           "search_code scan exceeded its execution deadline");
    char *result = source_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return result ? result : source_strdup(fallback);
}

static cbm_operation_result_t search_code_scan_error(
    search_scratch_t *scratch, const char *output_path, bool has_path_filter,
    cbm_regex_t *path_regex, cbm_store_t *store, char *root_path, char *pattern,
    char *project, char *file_pattern, cbm_operation_command_cause_t cause,
    const char *message) {
    if (output_path && output_path[0]) {
        (void)cbm_unlink(output_path);
    }
    search_scratch_close(scratch);
    if (has_path_filter) {
        cbm_regfree(path_regex);
    }
    if (store) cbm_store_close(store);
    free(root_path);
    free(pattern);
    free(project);
    free(file_pattern);
    if (cause == CBM_OPERATION_COMMAND_DEADLINE) {
        char *payload = search_code_timeout_payload();
        return cbm_operation_result_take(payload, true);
    }
    return source_error(message);
}

cbm_operation_result_t cbm_source_search_operation_execute(
    const char *args, const cbm_operation_runtime_t *runtime) {
    char *pattern = source_string_arg(args, "pattern");
    char *project = source_project_arg(args);
    char *file_pattern = source_string_arg(args, "file_pattern");
    char *path_filter = source_string_arg(args, "path_filter");
    char *mode_str = source_string_arg(args, "mode");
    int limit = source_int_arg(args, "limit", SOURCE_DEFAULT_LIMIT);
    /* #1511: a negative limit flowed straight into the result cap and came back
     * as the reported count ("results: -5"), which reads to an agent as a real
     * answer rather than a rejected argument. The schema now declares
     * minimum:1, but a schema is a request to the client, never a guarantee to
     * the server — clamp here too. */
    if (limit < 1) {
        limit = SOURCE_DEFAULT_LIMIT;
    }
    int context_lines = source_int_arg(args, "context", 0);
    bool use_regex = source_bool_arg(args, "regex", false);
    uint64_t search_t0 = cbm_now_ms();
    search_metrics_t metrics = {0};
    metrics.include_phase_timings = source_bool_arg(args, "debug", false);
    /* In literal (non-regex) mode a '|' is matched as a byte, not alternation —
     * a common silent 0-match trap; flagged in the result warnings (#282). */
    bool pat_has_pipe = pattern && strchr(pattern, '|') != NULL;

    int mode = parse_search_mode(mode_str);
    free(mode_str);

    cbm_regex_t path_regex;
    bool has_path_filter = compile_path_filter(path_filter, &path_regex);
    free(path_filter);
    path_filter = NULL;

    if (!pattern) {
        free(project);
        free(file_pattern);
        return source_error("pattern is required");
    }

    /* Project is required */
    if (!project) {
        free(pattern);
        free(file_pattern);
        return source_project_error(NULL);
    }

    char *root_path = NULL;
    cbm_store_t *store = source_open_store_and_root(project, &root_path);
    if (!store) {
        cbm_operation_result_t error = source_project_error(project);
        free(pattern);
        free(project);
        free(file_pattern);
        return error;
    }

    if (!validate_search_args(root_path, file_pattern)) {
        if (has_path_filter) {
            cbm_regfree(&path_regex);
        }
        cbm_store_close(store);
        free(root_path);
        free(pattern);
        free(project);
        free(file_pattern);
        return source_error("path or file_pattern contains invalid characters");
    }

    /* issue #283: when regex=true, a syntactically invalid pattern (e.g. an
     * unclosed group) makes the underlying grep fail, which the handler would
     * otherwise report as an empty result set — indistinguishable from a
     * legitimate no-match. Validate the user's regex up front and return an
     * explicit error so callers can tell "broken pattern" from "no matches". */
    if (use_regex) {
        cbm_regex_t probe;
        if (cbm_regcomp(&probe, pattern, CBM_REG_EXTENDED | CBM_REG_NOSUB) != CBM_REG_OK) {
            if (has_path_filter) {
                cbm_regfree(&path_regex);
            }
            cbm_store_close(store);
            free(root_path);
            free(pattern);
            free(project);
            free(file_pattern);
            return source_error(
                "invalid regex pattern (regex=true): check for unbalanced (), [], or {}");
        }
        cbm_regfree(&probe);
    }

    /* ── Phase 0.5: Multi-word → regex conversion ───────────── */
    /* If pattern contains whitespace and is not already a regex, convert to a
     * regex that matches all words in order: "foo bar baz" → "foo.*bar.*baz".
     * This avoids requiring the exact phrase as a contiguous substring. */
    if (!use_regex && strchr(pattern, ' ')) {
        size_t plen = strlen(pattern);
        /* Worst case: every char is a space → ".*" between each char */
        char *regex_pat = malloc(plen * 3 + 1);
        if (regex_pat) {
            char *dst = regex_pat;
            const char *src = pattern;
            bool in_space = false;
            while (*src) {
                if (*src == ' ' || *src == '\t') {
                    if (!in_space) {
                        *dst++ = '.';
                        *dst++ = '*';
                        in_space = true;
                    }
                } else {
                    /* Escape regex metacharacters from user input */
                    if (strchr("\\^$.|?*+()[]{}", *src)) {
                        *dst++ = '\\';
                    }
                    *dst++ = *src;
                    in_space = false;
                }
                src++;
            }
            *dst = '\0';
            free(pattern);
            pattern = regex_pat;
            use_regex = true;
        }
    }

    /* ── Phase 1: Grep scan ──────────────────────────────────── */
    uint64_t scan_budget_ms = runtime && runtime->command_timeout_override_set
                                  ? (uint64_t)runtime->command_timeout_override_ms
                                  : SOURCE_SEARCH_SCAN_TIMEOUT_MS;
    uint64_t scan_started_ms = cbm_now_ms();
    uint64_t scan_deadline_ms = UINT64_MAX - scan_started_ms < scan_budget_ms
                                    ? UINT64_MAX
                                    : scan_started_ms + scan_budget_ms;
    bool scan_deadline_latched = false;
    search_scratch_t scratch;
    if (!search_scratch_open(&scratch, pattern)) {
        bool scan_cancelled = cbm_operation_runtime_cancelled(runtime);
        bool scan_timed_out = cbm_now_ms() >= scan_deadline_ms;
        char errmsg[CBM_SZ_256];
        snprintf(errmsg, sizeof(errmsg), "search failed: cannot create temp file (%s)",
                 strerror(errno));
        cbm_store_close(store);
        free(root_path);
        free(pattern);
        free(project);
        free(file_pattern);
        if (scan_cancelled) {
            return source_error("search_code cancelled for this request");
        }
        if (scan_timed_out) {
            char *payload = search_code_timeout_payload();
            return cbm_operation_result_take(payload, true);
        }
        return source_error(errmsg);
    }
    scan_deadline_latched = cbm_now_ms() >= scan_deadline_ms;
    bool scan_cancellation_latched = cbm_operation_runtime_cancelled(runtime);
    const char *tmpfile = scratch.pattern_path;
    const char *filelist = scratch.filelist_path;

    /* No grep-level match limit — let grep find all matches, then dedup and
     * cap in our code. The -m flag caused results from large vendored files
     * to exhaust the quota before reaching project source files. */
    enum { GREP_MAX_MATCHES = 500 };
    int grep_limit = GREP_MAX_MATCHES;

    /* Scope grep to indexed files only — avoids scanning vendored/generated code.
     * Query the graph for distinct file paths, write them to a temp file,
     * then use xargs to pass them to grep. Falls back to recursive grep if
     * no indexed files found (project not fully indexed). */
    bool scoped = false;
    int scoped_written = 0;

    uint64_t scope_t0 = metrics.include_phase_timings ? cbm_now_ms() : 0;
    if (!scan_cancellation_latched && !scan_deadline_latched) {
        scoped = write_scoped_filelist(store, project, root_path, scratch.filelist, has_path_filter,
                                       has_path_filter ? &path_regex : NULL, &scoped_written);
    }
    /* Close before grep runs: this is what flushes the records the helper wrote
     * through the descriptor. Clearing the field hands ownership to
     * search_scratch_close, which still unlinks the file itself. */
    (void)fclose(scratch.filelist);
    scratch.filelist = NULL;
    scan_cancellation_latched = scan_cancellation_latched || cbm_operation_runtime_cancelled(runtime);
    scan_deadline_latched = scan_deadline_latched || cbm_now_ms() >= scan_deadline_ms;
    if (metrics.include_phase_timings) {
        metrics.scope_ms = cbm_now_ms() - scope_t0;
    }

    /* Collect grep matches into array */
    int gm_count = 0;
    grep_match_t *gm = NULL;
    uint64_t scan_t0 = metrics.include_phase_timings ? cbm_now_ms() : 0;
    if (scoped && scoped_written == 0 && !scan_cancellation_latched && !scan_deadline_latched) {
        /* The path_filter excluded every indexed file — nothing to scan.
         * Skip the grep subprocess: xargs on an empty filelist is
         * platform-dependent (GNU execs grep once with no operands, BSD
         * skips), and the post-grep filter would drop every hit anyway. */
        gm = malloc(sizeof(grep_match_t)); /* empty set; freed below */
        search_scratch_close(&scratch);
    } else {
        char cmd[CBM_SZ_4K];
        cbm_search_code_build_grep_cmd(cmd, sizeof(cmd), use_regex, scoped, file_pattern, tmpfile,
                                       filelist, root_path);

        char output_path[CBM_SZ_2K] = {0};
        cbm_proc_result_t scan_result = {0};
        size_t scan_output_limit = runtime && runtime->command_output_limit_override
                                       ? runtime->command_output_limit_override
                                       : SOURCE_SEARCH_OUTPUT_MAX;
        const char *scan_command =
            runtime && runtime->command_override ? runtime->command_override : cmd;
        cbm_operation_command_cause_t scan_cause = cbm_operation_run_shell_command_bounded(
            runtime, scan_command, output_path, scan_output_limit, scan_deadline_ms, true,
            scan_deadline_latched, !scoped, &scan_result);
        if (scan_cause == CBM_OPERATION_COMMAND_SUPERVISION_FAILURE) {
            return search_code_scan_error(&scratch, output_path, has_path_filter, &path_regex, store,
                                          root_path, pattern, project, file_pattern, scan_cause,
                                          "search failed: process supervision could not quiesce");
        }
        if (scan_cause == CBM_OPERATION_COMMAND_CANCELLED) {
            return search_code_scan_error(&scratch, output_path, has_path_filter, &path_regex, store,
                                          root_path, pattern, project, file_pattern, scan_cause,
                                          "search_code cancelled for this request");
        }
        if (scan_cause == CBM_OPERATION_COMMAND_DEADLINE) {
            return search_code_scan_error(&scratch, output_path, has_path_filter, &path_regex, store,
                                          root_path, pattern, project, file_pattern, scan_cause,
                                          NULL);
        }
        if (scan_cause == CBM_OPERATION_COMMAND_OUTPUT_LIMIT) {
            char message[CBM_SZ_128];
            snprintf(message, sizeof(message),
                     "search failed: output exceeded the %zu-byte safety limit", scan_output_limit);
            return search_code_scan_error(&scratch, output_path, has_path_filter, &path_regex, store,
                                          root_path, pattern, project, file_pattern, scan_cause,
                                          message);
        }
        if (scan_cause == CBM_OPERATION_COMMAND_FAILURE ||
            scan_cause == CBM_OPERATION_COMMAND_CONTAINED_FAILURE) {
            return search_code_scan_error(
                &scratch, output_path, has_path_filter, &path_regex, store, root_path, pattern, project,
                file_pattern, scan_cause,
                "search failed: the contained command could not complete");
        }
        FILE *fp = cbm_fopen(output_path, "rb");
        if (!fp) {
            return search_code_scan_error(&scratch, output_path, has_path_filter, &path_regex, store,
                                          root_path, pattern, project, file_pattern,
                                          CBM_OPERATION_COMMAND_FAILURE,
                                          "search failed: contained output could not be read");
        }
        gm = collect_grep_matches(fp, root_path, strlen(root_path), has_path_filter, &path_regex,
                                  grep_limit, &gm_count);
        (void)fclose(fp);
        (void)cbm_unlink(output_path);
        /* Both scratch files and the private directory go here — unlike the old
         * code, the file list is removed even when the scan was not scoped. */
        search_scratch_close(&scratch);
    }
    if (metrics.include_phase_timings) {
        metrics.scan_ms = cbm_now_ms() - scan_t0;
    }

    /* ── Phase 2+3: Block expansion + graph ranking ──────────── */
    /* Sort grep matches by file for contiguous processing.
     * Then: one SQL query per unique file for nodes, one batch query for all degrees. */

    uint64_t enrich_t0 = metrics.include_phase_timings ? cbm_now_ms() : 0;

    int sr_cap = CBM_SZ_32;
    int sr_count = 0;
    search_result_t *sr = calloc(sr_cap, sizeof(search_result_t));

    int raw_cap = CBM_SZ_32;
    int raw_count = 0;
    grep_match_t *raw = malloc(raw_cap * sizeof(grep_match_t));

    /* Sort matches by file path for contiguous per-file processing */
    qsort(gm, gm_count, sizeof(grep_match_t), (int (*)(const void *, const void *))strcmp);

    classify_all_grep_hits(gm, gm_count, store, project, &sr, &sr_count, &sr_cap, &raw, &raw_count,
                           &raw_cap);

    /* Phase 3: batch degree query — ONE query for all results instead of 2×N */
    if (store && sr_count > 0) {
        int64_t *ids = malloc(sr_count * sizeof(int64_t));
        int *in_degs = malloc(sr_count * sizeof(int));
        int *out_degs = malloc(sr_count * sizeof(int));
        for (int j = 0; j < sr_count; j++) {
            ids[j] = sr[j].node_id;
        }
        if (cbm_store_batch_count_degrees(store, ids, sr_count, "CALLS", in_degs, out_degs) ==
            CBM_STORE_OK) {
            for (int j = 0; j < sr_count; j++) {
                sr[j].in_degree = in_degs[j];
                sr[j].out_degree = out_degs[j];
            }
        }
        free(ids);
        free(in_degs);
        free(out_degs);
    }

    /* Compute scores and sort */
    for (int j = 0; j < sr_count; j++) {
        sr[j].score = compute_search_score(&sr[j]);
    }
    if (sr_count > SOURCE_SKIP_ONE) {
        qsort(sr, sr_count, sizeof(search_result_t), search_result_cmp);
    }
    if (metrics.include_phase_timings) {
        metrics.enrich_ms = cbm_now_ms() - enrich_t0;
    }
    metrics.elapsed_ms = cbm_now_ms() - search_t0;

    /* ── Phase 4: Context assembly (extracted helper) ─────────── */

    /* compact mode (default) emits tree tables; format:"json" emits the
     * same model as structured JSON ({cols, rows}; full adds a per-row
     * source cell; files is a plain list). */
    char *sc_format = source_string_arg(args, "format");
    bool sc_legacy_json = sc_format && strcmp(sc_format, "json") == 0;
    free(sc_format);

    char *result = NULL;
    bool result_error = false;
    if (mode == 0 && !sc_legacy_json) {
        result = assemble_search_output_toon(sr, sr_count, raw, raw_count, gm_count, limit,
                                             pat_has_pipe && !use_regex, &metrics);
        result_error = result == NULL;
        if (!result) result = source_strdup("out of memory");
    } else {
        result = assemble_search_output(sr, sr_count, raw, raw_count, gm_count, limit, mode,
                                        context_lines, root_path, pat_has_pipe && !use_regex, &metrics);
        result_error = result == NULL;
        if (!result) result = source_strdup("out of memory");
    }
    free(gm);
    free(sr);
    free(raw);
    free(root_path);
    free(pattern);
    free(project);
    free(file_pattern);
    if (has_path_filter) {
        cbm_regfree(&path_regex);
    }
    cbm_store_close(store);
    return cbm_operation_result_take(result, result_error || result == NULL);
}

/* ── detect_changes ───────────────────────────────────────────── */
