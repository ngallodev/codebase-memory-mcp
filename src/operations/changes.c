#include "operations/changes.h"

#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/limits.h"
#include "foundation/log.h"
#include "foundation/platform.h"
#include "foundation/str_util.h"
#include "operations/command_runner.h"
#include "operations/compact_out.h"
#include "pipeline/pipeline.h"
#include "store/store.h"
#include "yyjson/yyjson.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifdef _WIN32
#include <windows.h>
#endif

enum {
    CHANGES_DEFAULT_BFS_DEPTH = 2,
    CHANGES_BFS_LIMIT_MAX = 5000,
    CHANGES_DEFAULT_IMPACT_LIMIT = 200,
};

#define CHANGES_SKIP_ONE 1
#define CHANGES_PAIR_LEN 2

static char *changes_strdup(const char *text) {
    if (!text) return NULL;
    size_t len = strlen(text);
    char *copy = malloc(len + 1U);
    if (copy) memcpy(copy, text, len + 1U);
    return copy;
}

static void *changes_realloc(void *ptr, size_t size) {
    void *grown = realloc(ptr, size);
    if (!grown && size != 0) {
        free(ptr);
        abort();
    }
    return grown;
}

static yyjson_doc *changes_args_doc(const char *args) {
    const char *json = args ? args : "{}";
    return yyjson_read(json, strlen(json), 0);
}

static char *changes_string_arg(const char *args, const char *name) {
    yyjson_doc *doc = changes_args_doc(args);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    char *result = value && yyjson_is_str(value) ? changes_strdup(yyjson_get_str(value)) : NULL;
    if (doc) yyjson_doc_free(doc);
    return result;
}

static char *changes_project_arg(const char *args) {
    static const char *const names[] = {"project", "project_name", "project_id", "projectName"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i) {
        char *value = changes_string_arg(args, names[i]);
        if (value) return value;
    }
    return NULL;
}

static int changes_int_arg(const char *args, const char *name, int fallback) {
    yyjson_doc *doc = changes_args_doc(args);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    int result = value && yyjson_is_int(value) ? (int)yyjson_get_sint(value) : fallback;
    if (doc) yyjson_doc_free(doc);
    return result;
}

static int changes_clamp_depth(int depth, const char *tool) {
    int cap = cbm_operation_max_depth();
    if (depth > cap) {
        char req_buf[16];
        char cap_buf[16];
        snprintf(req_buf, sizeof(req_buf), "%d", depth);
        snprintf(cap_buf, sizeof(cap_buf), "%d", cap);
        cbm_log_warn("operation.depth_capped", "operation", tool, "requested", req_buf, "cap", cap_buf);
        return cap;
    }
    return depth;
}

static bool changes_validate_search_path_arg(const char *s) {
    if (!s) return false;
    for (const char *p = s; *p; ++p) {
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

static bool changes_validate_windows_cmd_interpolation_arg(const char *s) {
#ifdef _WIN32
    return s && strpbrk(s, "%!^") == NULL;
#else
    return s != NULL;
#endif
}

static char *changes_doc_to_str(yyjson_mut_doc *doc) {
    return yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INVALID_UNICODE, NULL);
}

static cbm_operation_result_t changes_error(const char *message) {
    return cbm_operation_result_copy(message ? message : "detect_changes failed", true);
}

static cbm_operation_result_t changes_project_error(const char *project) {
    if (!project) {
        return changes_error("{\"error\":\"missing required argument: project\",\"hint\":\"Pass the project as the \\\"project\\\" argument. Run projects to see indexed projects.\"}");
    }
    return changes_error("{\"error\":\"project not found or not indexed\",\"hint\":\"Run projects to see indexed projects.\"}");
}

static cbm_store_t *changes_open_store_and_root(const char *project, char **root_path_out) {
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
    *root_path_out = changes_strdup(info.root_path);
    cbm_project_free_fields(&info);
    if (!*root_path_out) {
        cbm_store_close(store);
        return NULL;
    }
    return store;
}

static size_t changes_qn_prefix_len(const char *qn) {
    const char *last = qn ? strrchr(qn, '.') : NULL;
    return last ? (size_t)(last - qn) : 0;
}

static int changes_hop_cmp_qn(const void *pa, const void *pb) {
    const cbm_node_hop_t *a = pa;
    const cbm_node_hop_t *b = pb;
    const char *qa = a->node.qualified_name ? a->node.qualified_name : "";
    const char *qb = b->node.qualified_name ? b->node.qualified_name : "";
    int c = strcmp(qa, qb);
    return c != 0 ? c : a->hop - b->hop;
}

bool cbm_detect_node_in_hunks(const cbm_node_t *node, const cbm_changed_hunk_t *hunks,
                              int hunk_count, const char *file) {
    for (int h = 0; h < hunk_count; ++h) {
        if (strcmp(hunks[h].path, file) == 0 && node->start_line <= hunks[h].end_line &&
            node->end_line >= hunks[h].start_line) {
            return true;
        }
    }
    return false;
}
static bool detect_is_seedable_label(const char *lb) {
    return lb && strcmp(lb, "File") != 0 && strcmp(lb, "Folder") != 0 &&
           strcmp(lb, "Project") != 0 && strcmp(lb, "Module") != 0 && strcmp(lb, "Package") != 0 &&
           strcmp(lb, "Section") != 0;
}

static void detect_collect_seeds(cbm_store_t *store, const char *project, const char *file,
                                 const cbm_changed_hunk_t *hunks, int hunk_count, int64_t **seeds,
                                 int *n, int *cap) {
    cbm_node_t *nodes = NULL;
    int ncount = 0;
    cbm_store_find_nodes_by_file(store, project, file, &nodes, &ncount);
    bool scope_to_hunks = false;
    for (int h = 0; h < hunk_count; h++) {
        if (strcmp(hunks[h].path, file) == 0) {
            scope_to_hunks = true;
            break;
        }
    }
    /* A file can have hunks yet no SEEDABLE definition overlapping any of them:
     * an import-only edit, a module-level constant, or a change above the first
     * definition all land outside every definition's line range. Scoping would
     * then drop the file from the seed set entirely — strictly worse recall
     * than the whole-file behavior this replaces. Probe for an overlap first
     * and keep whole-file seeding for that file when there is none.
     *
     * The probe must apply the same label filter as the seeding loop below:
     * container nodes span the whole file (a Module node is lines 1..EOF), so
     * counting them would report an overlap for every hunk and defeat the
     * fallback entirely. */
    if (scope_to_hunks) {
        bool any_overlap = false;
        for (int i = 0; i < ncount && !any_overlap; i++) {
            any_overlap = detect_is_seedable_label(nodes[i].label) &&
                          cbm_detect_node_in_hunks(&nodes[i], hunks, hunk_count, file);
        }
        scope_to_hunks = any_overlap;
    }
    for (int i = 0; i < ncount; i++) {
        if (detect_is_seedable_label(nodes[i].label)) {
            if (scope_to_hunks && !cbm_detect_node_in_hunks(&nodes[i], hunks, hunk_count, file)) {
                continue;
            }
            if (*n >= *cap) {
                *cap = *cap ? *cap * 2 : 16;
                *seeds = changes_realloc(*seeds, (size_t)*cap * sizeof(int64_t));
            }
            (*seeds)[(*n)++] = nodes[i].id;
        }
    }
    cbm_store_free_nodes(nodes, ncount);
}

/* Module key for the impacted rollup = the first TWO path segments
 * ("src/mcp/mcp.c" -> "src/mcp"), a quotient of the blast radius coarse enough
 * to fit yet specific enough to localize (one segment collapses a whole tree
 * to "src"). Falls back to one segment, then the whole path. */
static void detect_module_of(const char *file, char *out, size_t outsz) {
    if (!file || !file[0]) {
        snprintf(out, outsz, "(root)");
        return;
    }
    const char *s1 = strchr(file, '/');
    if (!s1) {
        snprintf(out, outsz, "%s", file);
        return;
    }
    const char *s2 = strchr(s1 + 1, '/');
    size_t len = s2 ? (size_t)(s2 - file) : strlen(file);
    if (len >= outsz) {
        len = outsz - 1;
    }
    memcpy(out, file, len);
    out[len] = '\0';
}

/* Aggregate the impact set into the 2-segment module rollup. Fills up to
 * DETECT_MODCAP (module, count) pairs; symbols beyond the cap land in
 * *overflow (surfaced as "(other)", never silently dropped). Shared by the
 * tree and json emitters so both encodings carry the same model. */
enum { DETECT_MODCAP = 256 };

static int detect_module_rollup(const cbm_traverse_result_t *impact, char mods[][CBM_SZ_128],
                                int *mcnt, int *overflow) {
    int nmods = 0;
    *overflow = 0;
    for (int i = 0; i < impact->visited_count; i++) {
        char m[CBM_SZ_128];
        detect_module_of(impact->visited[i].node.file_path, m, sizeof(m));
        int j = 0;
        for (; j < nmods; j++) {
            if (strcmp(mods[j], m) == 0) {
                mcnt[j]++;
                break;
            }
        }
        if (j == nmods) {
            if (nmods < DETECT_MODCAP) {
                snprintf(mods[nmods], CBM_SZ_128, "%s", m);
                mcnt[nmods] = 1;
                nmods++;
            } else {
                (*overflow)++;
            }
        }
    }
    return nmods;
}

/* Emit the impacted set as a grouped tree leg: rows grouped under their shared
 * (qn-prefix, file), `name label hop` per row. At most `limit` rows are listed
 * (the visited array is hop-ordered, so the closest — highest-signal — impact
 * shows first); impacted_total always carries the exact full count, and
 * `impacted_shown < impacted_total` is the honest truncation signal. The
 * module rollup (emitted by the caller) stays complete regardless. */
static void detect_emit_impacted_tree(cbm_sb_t *sb, cbm_traverse_result_t *tr, int limit) {
    cbm_tree_scalar_int(sb, "impacted_total", tr->visited_count);
    int shown = tr->visited_count < limit ? tr->visited_count : limit;
    /* qn order for stable grouping, but keep hop-closeness: sort by (hop) is
     * lost under qn sort, so group AFTER selecting the nearest `shown` rows —
     * the visited array is already (hop,id)-ordered from the BFS. */
    char hdr[CBM_SZ_128];
    snprintf(hdr, sizeof(hdr),
             "impacted_shown: %d\nimpacted: %d  (rows: name label hop; qn = group prefix + \".\" "
             "+ name; nearest hops first)\n",
             shown, shown);
    cbm_sb_append(sb, hdr);
    if (shown > 1) {
        qsort(tr->visited, (size_t)shown, sizeof(cbm_node_hop_t), changes_hop_cmp_qn);
    }
    char cur_group[CBM_SZ_1K] = "";
    for (int i = 0; i < shown; i++) {
        const char *qn =
            tr->visited[i].node.qualified_name ? tr->visited[i].node.qualified_name : "";
        const char *file = tr->visited[i].node.file_path ? tr->visited[i].node.file_path : "";
        size_t plen = changes_qn_prefix_len(qn);
        char group[CBM_SZ_1K];
        snprintf(group, sizeof(group), "%.*s (%s)", (int)plen, qn, file);
        if (strcmp(group, cur_group) != 0) {
            snprintf(cur_group, sizeof(cur_group), "%s", group);
            cbm_sb_append(sb, group);
            cbm_sb_append(sb, ":\n");
        }
        char row[CBM_SZ_512];
        snprintf(row, sizeof(row), "  %s %s %d\n", plen ? qn + plen + 1 : qn,
                 tr->visited[i].node.label ? tr->visited[i].node.label : "", tr->visited[i].hop);
        cbm_sb_append(sb, row);
    }
    if (shown < tr->visited_count) {
        char more[CBM_SZ_256];
        snprintf(more, sizeof(more),
                 "impacted_omitted: %d  (see impacted_modules for the full rollup; raise 'limit' "
                 "or lower 'depth' to see specifics)\n",
                 tr->visited_count - shown);
        cbm_sb_append(sb, more);
    }
}

cbm_operation_result_t cbm_changes_operation_execute(const char *args,
                                                        const cbm_operation_runtime_t *runtime) {
    char *project = changes_project_arg(args);
    char *base_branch = changes_string_arg(args, "base_branch");
    char *since = changes_string_arg(args, "since");
    char *scope = changes_string_arg(args, "scope");
    int depth = changes_int_arg(args, "depth", CHANGES_DEFAULT_BFS_DEPTH);
    depth = changes_clamp_depth(depth, "detect_changes");

    /* scope: "files" = just changed files, "symbols" = files + symbols (default) */
    bool want_symbols = !scope || strcmp(scope, "symbols") == 0 || strcmp(scope, "impact") == 0;

    /* `since` (e.g. "HEAD~10", "v0.5.0") is the documented diff base but was
     * previously parsed and never used: it takes precedence over base_branch.
     * Route it through base_branch so the shared shell-arg validation and the
     * existing `<base>...HEAD` (three-dot) diff apply unchanged — `since` thus
     * adopts the same merge-base semantics base_branch already uses. */
    if (since && since[0]) {
        free(base_branch);
        base_branch = since; /* transfer ownership */
        since = NULL;
    }
    free(since); /* no-op after the swap (since is NULL); frees it otherwise */

    if (!base_branch) {
        base_branch = changes_strdup("main");
    }

    /* Reject shell metacharacters, and a leading '-', in the user-supplied
     * branch name. base_branch is spliced into `git diff --name-only
     * "<base>"...HEAD`; a value starting with '-' would be read by git as an
     * option rather than a ref (e.g. `--output=<path>` writes the diff to an
     * arbitrary file). A real git ref never begins with '-'. */
    if (!cbm_validate_shell_arg(base_branch) || base_branch[0] == '-' ||
        !changes_validate_windows_cmd_interpolation_arg(base_branch)) {
        free(project);
        free(base_branch);
        free(scope);
        return changes_error("base_branch contains invalid characters");
    }

    char *root_path = NULL;
    cbm_store_t *store = changes_open_store_and_root(project, &root_path);
    if (!store) {
        cbm_operation_result_t error = changes_project_error(project);
        free(project);
        free(base_branch);
        free(scope);
        return error;
    }

    if (!changes_validate_search_path_arg(root_path) ||
        !changes_validate_windows_cmd_interpolation_arg(root_path)) {
        cbm_store_close(store);
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        return changes_error("project path contains invalid characters");
    }

    /* Get changed files via git (-C avoids cd + quoting issues on Windows).
     * Three sources are merged:
     *   1. committed changes vs base   (diff <base>...HEAD)
     *   2. unstaged tracked changes    (diff)
     *   3. untracked + staged-new files (status --porcelain) — these are
     *      invisible to `git diff` and were silently missed before, so a
     *      brand-new file never appeared until a manual re-index (#520).
     * status --porcelain prefixes each path with a 2-char code + space
     * ("?? path", "A  path"); the prefix is stripped when parsing below. */
    char cmd[CBM_SZ_2K];
#ifdef _WIN32
    snprintf(cmd, sizeof(cmd),
             "git -C \"%s\" diff --name-only \"%s\"...HEAD 2>NUL & "
             "git -C \"%s\" diff --name-only 2>NUL & "
             "git --no-optional-locks -C \"%s\" status --porcelain "
             "--untracked-files=normal 2>NUL",
             root_path, base_branch, root_path, root_path);
#else
    snprintf(cmd, sizeof(cmd),
             "{ git -C '%s' diff --name-only '%s'...HEAD 2>/dev/null; "
             "git -C '%s' diff --name-only 2>/dev/null; "
             "git --no-optional-locks -C '%s' status --porcelain "
             "--untracked-files=normal 2>/dev/null; } | sort -u",
             root_path, base_branch, root_path, root_path);
#endif

    char output_path[CBM_SZ_2K] = {0};
    cbm_proc_result_t git_result = {0};
    int git_run = cbm_operation_run_shell_command(runtime, cmd, output_path, &git_result);
    bool git_cancelled = git_result.cancellation_requested || cbm_operation_runtime_cancelled(runtime);
    if (git_cancelled) {
        (void)cbm_unlink(output_path);
        cbm_store_close(store);
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        return changes_error("detect_changes cancelled for this request");
    }
    if (git_run != 0) {
        (void)cbm_unlink(output_path);
        char errmsg[CBM_SZ_256];
        snprintf(errmsg, sizeof(errmsg),
                 "git diff failed: the contained command could not complete. "
                 "Check that git is installed.");
        cbm_store_close(store);
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        return changes_error(errmsg);
    }
    FILE *fp = cbm_fopen(output_path, "rb");
    if (!fp) {
        (void)cbm_unlink(output_path);
        cbm_store_close(store);
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        return changes_error("git diff failed: contained output could not be read");
    }

    /* Direction of impact. Default inbound = the BLAST RADIUS: the transitive
     * CALLERS of the changed symbols, which may need review. outbound = what
     * the changed code depends on; both = union. */
    char *direction = changes_string_arg(args, "direction");
    if (!direction) {
        direction = changes_strdup("inbound");
    }
    /* Teaching error, same contract as trace_path: never silently correct an
     * unknown direction — the caller would misread the result's semantics. */
    if (strcmp(direction, "inbound") != 0 && strcmp(direction, "outbound") != 0 &&
        strcmp(direction, "both") != 0) {
        char errbuf[CBM_SZ_256];
        snprintf(errbuf, sizeof(errbuf),
                 "invalid direction \"%s\" — use \"inbound\" (blast radius: transitive callers), "
                 "\"outbound\" (dependencies), or \"both\"",
                 direction);
        free(direction);
        free(root_path);
        free(project);
        free(base_branch);
        free(scope);
        (void)fclose(fp);
        (void)cbm_unlink(output_path);
        cbm_store_close(store);
        return changes_error(errbuf);
    }
    char *fmt = changes_string_arg(args, "format");
    bool legacy_json = fmt && strcmp(fmt, "json") == 0;
    free(fmt);

    /* Per-symbol impacted-row display cap (the module rollup stays complete).
     * impacted_total always reports the true count, so this never hides scale. */
    int imp_limit = changes_int_arg(args, "limit", CHANGES_DEFAULT_IMPACT_LIMIT);
    if (imp_limit < 1) {
        imp_limit = 1;
    }
    if (imp_limit > CHANGES_BFS_LIMIT_MAX) {
        imp_limit = CHANGES_BFS_LIMIT_MAX;
    }

    /* Collect changed file paths into a C array (drives seeds, the rollup, and
     * both output encodings). */
    char **files = NULL;
    int file_count = 0;
    int file_cap = 0;
    int64_t *seeds = NULL;
    int seed_count = 0;
    int seed_cap = 0;

    /* Hunk line ranges (unified=0 diff), used to scope seed detection to the
     * actually-changed lines instead of every definition in a changed file
     * (see detect_collect_seeds). Best-effort: any failure here just leaves
     * `hunks` empty and every file falls back to its previous whole-file
     * seeding — this is a precision improvement, not a correctness
     * dependency, so it is never treated as a request-level failure.
     *
     * Coordinate systems: `base...HEAD` hunks carry HEAD-side line numbers,
     * the worktree diff carries worktree-side ones, and node line ranges come
     * from the indexed snapshot. These agree while the index is fresh — the
     * watcher reindexes on HEAD movement and on a dirty tree — but a stale
     * index combined with insertions earlier in the file shifts the node lines
     * relative to the hunks and can mis-scope. The failure is bounded by
     * detect_collect_seeds' zero-overlap fallback: a file whose definitions all
     * miss reverts to whole-file seeding rather than dropping out. */
    cbm_changed_hunk_t *hunks = NULL;
    int hunk_count = 0;
    if (want_symbols) {
        char hunk_cmd[CBM_SZ_2K];
#ifdef _WIN32
        snprintf(hunk_cmd, sizeof(hunk_cmd),
                 "git -C \"%s\" diff --unified=0 \"%s\"...HEAD 2>NUL & "
                 "git -C \"%s\" diff --unified=0 2>NUL",
                 root_path, base_branch, root_path);
#else
        snprintf(hunk_cmd, sizeof(hunk_cmd),
                 "{ git -C '%s' diff --unified=0 '%s'...HEAD 2>/dev/null; "
                 "git -C '%s' diff --unified=0 2>/dev/null; }",
                 root_path, base_branch, root_path);
#endif
        char hunk_output_path[CBM_SZ_2K] = {0};
        cbm_proc_result_t hunk_result = {0};
        int hunk_run =
            cbm_operation_run_shell_command(runtime, hunk_cmd, hunk_output_path, &hunk_result);
        bool hunk_cancelled = hunk_result.cancellation_requested || cbm_operation_runtime_cancelled(runtime);
        FILE *hfp = (!hunk_cancelled && hunk_run == 0) ? cbm_fopen(hunk_output_path, "rb") : NULL;
        if (hfp) {
            (void)fseek(hfp, 0, SEEK_END);
            long hsz = ftell(hfp);
            if (hsz > 0) {
                (void)fseek(hfp, 0, SEEK_SET);
                char *hbuf = malloc((size_t)hsz + CHANGES_SKIP_ONE);
                if (hbuf) {
                    size_t hread = fread(hbuf, CHANGES_SKIP_ONE, (size_t)hsz, hfp);
                    hbuf[hread] = '\0';
                    enum { HUNK_CAP = 4096 };
                    hunks = changes_realloc(NULL, (size_t)HUNK_CAP * sizeof(cbm_changed_hunk_t));
                    hunk_count = cbm_parse_hunks(hbuf, hunks, HUNK_CAP);
                    /* A filled buffer means the diff was truncated: the hunks
                     * past the cap are gone, so files captured only partially
                     * would still look scoped and silently under-seed. Drop
                     * scoping for the whole request rather than under-report a
                     * large refactor — whole-file seeding is the safe side. */
                    if (hunk_count >= HUNK_CAP) {
                        cbm_log_info("detect_changes.hunks", "action", "scoping_disabled", "reason",
                                     "hunk_cap_reached");
                        free(hunks);
                        hunks = NULL;
                        hunk_count = 0;
                    }
                    free(hbuf);
                }
            }
            (void)fclose(hfp);
        }
        if (hunk_output_path[0]) {
            (void)cbm_unlink(hunk_output_path);
        }
    }

    char line[CBM_SZ_1K];
    while (fgets(line, sizeof(line), fp)) {
        size_t len = strlen(line);
        while (len > 0 && (line[len - CHANGES_SKIP_ONE] == '\n' || line[len - CHANGES_SKIP_ONE] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }
        /* Strip the `git status --porcelain` 2-char code + space; for a rename
         * ("R  old -> new") keep the destination path. */
        char *path_line = line;
        if (len > CHANGES_PAIR_LEN && line[CHANGES_PAIR_LEN] == ' ' && strchr(" MADRCU?!", line[0]) &&
            strchr(" MADRCU?!", line[1])) {
            path_line = line + CHANGES_PAIR_LEN + CHANGES_SKIP_ONE;
            char *arrow = strstr(path_line, " -> ");
            if (arrow) {
                enum { ARROW_LEN = 4 };
                path_line = arrow + ARROW_LEN;
            }
        }
        if (path_line[0] == '\0') {
            continue;
        }
        /* Dedup: the three git sources are sorted+unioned on POSIX but not on
         * Windows (separate commands), and a path can repeat. */
        bool dup = false;
        for (int i = 0; i < file_count; i++) {
            if (strcmp(files[i], path_line) == 0) {
                dup = true;
                break;
            }
        }
        if (dup) {
            continue;
        }
        if (file_count >= file_cap) {
            file_cap = file_cap ? file_cap * 2 : 16;
            files = changes_realloc(files, (size_t)file_cap * sizeof(char *));
        }
        files[file_count++] = changes_strdup(path_line);
        if (want_symbols) {
            detect_collect_seeds(store, project, path_line, hunks, hunk_count, &seeds, &seed_count,
                                 &seed_cap);
        }
    }
    (void)fclose(fp);
    (void)cbm_unlink(output_path);
    int git_status = git_result.exit_code;

    /* merge-base SHA: the exact commit the diff is measured against, so the
     * result is reproducible even as base_branch advances. Best-effort. */
    char merge_base[64] = "";
    {
        char mbcmd[CBM_SZ_2K];
#ifdef _WIN32
        snprintf(mbcmd, sizeof(mbcmd), "git -C \"%s\" merge-base \"%s\" HEAD 2>NUL", root_path,
                 base_branch);
#else
        snprintf(mbcmd, sizeof(mbcmd), "git -C '%s' merge-base '%s' HEAD 2>/dev/null", root_path,
                 base_branch);
#endif
        char mb_output_path[CBM_SZ_2K] = {0};
        cbm_proc_result_t mb_result = {0};
        int mb_run = cbm_operation_run_shell_command(runtime, mbcmd, mb_output_path, &mb_result);
        bool mb_cancelled = mb_result.cancellation_requested || cbm_operation_runtime_cancelled(runtime);
        bool mb_containment_failed = mb_run != 0 && mb_output_path[0] != '\0';
        FILE *mbfp =
            mb_run == 0 && mb_result.exit_code == 0 ? cbm_fopen(mb_output_path, "rb") : NULL;
        if (mbfp && !mb_cancelled) {
            if (fgets(merge_base, sizeof(merge_base), mbfp)) {
                size_t l = strlen(merge_base);
                while (l > 0 && (merge_base[l - 1] == '\n' || merge_base[l - 1] == '\r')) {
                    merge_base[--l] = '\0';
                }
            }
        }
        if (mbfp) {
            (void)fclose(mbfp);
        }
        if (mb_output_path[0]) {
            (void)cbm_unlink(mb_output_path);
        }
        if (mb_cancelled || mb_containment_failed) {
            for (int i = 0; i < file_count; i++) {
                free(files[i]);
            }
            free(files);
            free(seeds);
            free(hunks);
            free(direction);
            free(root_path);
            free(project);
            free(base_branch);
            free(scope);
            cbm_store_close(store);
            return changes_error(
                mb_cancelled ? "detect_changes cancelled for this request"
                             : "git merge-base failed: the contained command could not complete");
        }
    }

    /* The impact traversal: ONE multi-source BFS over all seeds. */
    cbm_traverse_result_t impact = {0};
    bool truncated = false;
    if (want_symbols && seed_count > 0) {
        (void)cbm_store_bfs_multi(store, seeds, seed_count, direction, NULL, 0, depth,
                                  CHANGES_BFS_LIMIT_MAX, &impact, &truncated);
    }

    bool is_error = (git_status != 0 && file_count == 0);
    char *out_str = NULL;

    if (!legacy_json) {
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        cbm_tree_scalar_str(&sb, "base", base_branch);
        if (merge_base[0]) {
            cbm_tree_scalar_str(&sb, "merge_base", merge_base);
        }
        cbm_tree_scalar_str(&sb, "direction", direction);
        if (is_error) {
            char hint_buf[CBM_SZ_256];
            snprintf(hint_buf, sizeof(hint_buf),
                     "git diff exited with status %d. Check that branch '%s' exists.", git_status,
                     base_branch);
            cbm_tree_scalar_str(&sb, "hint", hint_buf);
        }
        /* changed files (the git result) */
        char cf[CBM_SZ_64];
        snprintf(cf, sizeof(cf), "changed_files: %d\n", file_count);
        cbm_sb_append(&sb, cf);
        for (int i = 0; i < file_count; i++) {
            cbm_sb_append(&sb, "  ");
            cbm_sb_append(&sb, files[i]);
            cbm_sb_append(&sb, "\n");
        }
        cbm_tree_scalar_int(&sb, "seed_symbols", seed_count);
        if (want_symbols) {
            detect_emit_impacted_tree(&sb, &impact, imp_limit);
            /* module rollup: a quotient view of the blast radius */
            if (impact.visited_count > 0) {
                cbm_sb_append(&sb, "impacted_modules: (rows: module count)\n");
                char (*mods)[CBM_SZ_128] = malloc(DETECT_MODCAP * CBM_SZ_128);
                int *mcnt = malloc(DETECT_MODCAP * sizeof(int));
                if (mods && mcnt) {
                    int overflow = 0;
                    int nmods = detect_module_rollup(&impact, mods, mcnt, &overflow);
                    for (int j = 0; j < nmods; j++) {
                        char mrow[CBM_SZ_256];
                        snprintf(mrow, sizeof(mrow), "  %s %d\n", mods[j], mcnt[j]);
                        cbm_sb_append(&sb, mrow);
                    }
                    if (overflow > 0) {
                        char orow[CBM_SZ_128];
                        snprintf(orow, sizeof(orow), "  (other) %d\n", overflow);
                        cbm_sb_append(&sb, orow);
                    }
                }
                free(mods);
                free(mcnt);
            }
            if (truncated) {
                cbm_tree_scalar_bool(&sb, "truncated", true);
                cbm_tree_scalar_str(&sb, "hint",
                                    "impact hit the safety ceiling — narrow with a lower "
                                    "'depth' or a smaller diff");
            }
        }
        out_str = cbm_sb_finish(&sb);
    } else {
        /* format:"json" = json-stringified tree: same model, structured. */
        yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
        yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
        yyjson_mut_doc_set_root(doc, root_obj);
        yyjson_mut_obj_add_strcpy(doc, root_obj, "base", base_branch);
        if (merge_base[0]) {
            yyjson_mut_obj_add_strcpy(doc, root_obj, "merge_base", merge_base);
        }
        yyjson_mut_obj_add_strcpy(doc, root_obj, "direction", direction);
        yyjson_mut_val *cf = yyjson_mut_arr(doc);
        for (int i = 0; i < file_count; i++) {
            yyjson_mut_arr_add_strcpy(doc, cf, files[i]);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "changed_files", cf);
        yyjson_mut_obj_add_int(doc, root_obj, "seed_symbols", seed_count);
        yyjson_mut_obj_add_int(doc, root_obj, "impacted_total", impact.visited_count);
        int imp_shown = impact.visited_count < imp_limit ? impact.visited_count : imp_limit;
        yyjson_mut_obj_add_int(doc, root_obj, "impacted_shown", imp_shown);
        yyjson_mut_val *imp = yyjson_mut_arr(doc);
        for (int i = 0; i < imp_shown; i++) {
            yyjson_mut_val *o = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_strcpy(
                doc, o, "qn",
                impact.visited[i].node.qualified_name ? impact.visited[i].node.qualified_name : "");
            yyjson_mut_obj_add_strcpy(
                doc, o, "label", impact.visited[i].node.label ? impact.visited[i].node.label : "");
            yyjson_mut_obj_add_strcpy(
                doc, o, "file",
                impact.visited[i].node.file_path ? impact.visited[i].node.file_path : "");
            yyjson_mut_obj_add_int(doc, o, "hop", impact.visited[i].hop);
            yyjson_mut_arr_add_val(imp, o);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "impacted", imp);
        /* Model parity with the tree encoding: the complete module rollup. */
        if (impact.visited_count > 0) {
            char (*mods)[CBM_SZ_128] = malloc(DETECT_MODCAP * CBM_SZ_128);
            int *mcnt = malloc(DETECT_MODCAP * sizeof(int));
            if (mods && mcnt) {
                int overflow = 0;
                int nmods = detect_module_rollup(&impact, mods, mcnt, &overflow);
                yyjson_mut_val *rollup = yyjson_mut_arr(doc);
                for (int j = 0; j < nmods; j++) {
                    yyjson_mut_val *o = yyjson_mut_obj(doc);
                    yyjson_mut_obj_add_strcpy(doc, o, "module", mods[j]);
                    yyjson_mut_obj_add_int(doc, o, "count", mcnt[j]);
                    yyjson_mut_arr_add_val(rollup, o);
                }
                if (overflow > 0) {
                    yyjson_mut_val *o = yyjson_mut_obj(doc);
                    yyjson_mut_obj_add_strcpy(doc, o, "module", "(other)");
                    yyjson_mut_obj_add_int(doc, o, "count", overflow);
                    yyjson_mut_arr_add_val(rollup, o);
                }
                yyjson_mut_obj_add_val(doc, root_obj, "impacted_modules", rollup);
            }
            free(mods);
            free(mcnt);
        }
        yyjson_mut_obj_add_bool(doc, root_obj, "truncated", truncated);
        if (is_error) {
            char hint_buf[CBM_SZ_256];
            snprintf(hint_buf, sizeof(hint_buf),
                     "git diff exited with status %d. Check that branch '%s' exists.", git_status,
                     base_branch);
            yyjson_mut_obj_add_strcpy(doc, root_obj, "hint", hint_buf);
        }
        out_str = changes_doc_to_str(doc);
        yyjson_mut_doc_free(doc);
    }

    cbm_store_traverse_free(&impact);
    for (int i = 0; i < file_count; i++) {
        free(files[i]);
    }
    free(files);
    free(seeds);
    free(hunks);
    free(direction);
    free(root_path);
    free(project);
    free(base_branch);
    free(scope);

    cbm_store_close(store);
    return cbm_operation_result_take(out_str, is_error || out_str == NULL);
}
