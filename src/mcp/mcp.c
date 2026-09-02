/*
 * mcp.c — MCP server: JSON-RPC 2.0 over stdio with graph tools.
 *
 * Uses yyjson for fast JSON parsing/building.
 * Single-threaded event loop: read line → parse → dispatch → respond.
 */

// operations

#include "foundation/constants.h"

enum {
    MCP_FIELD_SIZE = 1040,
    MCP_TIMEOUT_MS = 1000,
    MCP_HALF_SEC_US = 500000,
    MCP_COL_2 = 2,
    MCP_COL_3 = 3,
    MCP_COL_4 = 4,
    MCP_COL_7 = 7,
    MCP_COL_10 = 10,
    MCP_COL_16 = 16,
    MCP_DB_EXT = 3,      /* strlen(".db") */
    MCP_MIN_DB_NAME = 4, /* min length for "x.db" */
    MCP_SEPARATOR = 2,   /* space for separator chars */
    MCP_DEFAULT_DEPTH = 3,
    MCP_DEFAULT_BFS_DEPTH = 2,
    MCP_DEFAULT_LIMIT = 10,
    MCP_BFS_LIMIT = 100,            /* default per-direction trace budget (limit param raises) */
    MCP_BFS_LIMIT_MAX = 5000,       /* hard ceiling for the limit param (context-bomb guard) */
    MCP_DEFAULT_IMPACT_LIMIT = 200, /* detect_changes per-symbol rows; rollup stays complete */
    MCP_SNIPPET_MAX_LINES = 500,    /* get_code_snippet line cap (whole-file Module guard) */
    MCP_N_DEFAULTS_2 = 2,
    MCP_URI_PREFIX = 7,      /* strlen("file://") */
    MCP_CONTENT_PREFIX = 15, /* strlen("Content-Length:") */
    MCP_RETURN_2 = 2,
    MCP_TOOLS_PAGE_SIZE = 8,
    MCP_HELP_TOOLS_WRAP_COL = 74, /* --help tool list stays readable on 80-col terminals */
    MCP_MAX_CROSS_REPO_TARGETS = 4096,
    MCP_COMPARE_DEFAULT_LIMIT = 200,
    MCP_COMPARE_MAX_LIMIT = 1000,
    MCP_COMPARE_DEFAULT_SCAN_LIMIT = 2000000,
    MCP_COMPARE_MAX_SCAN_LIMIT = 10000000,
    MCP_COMPARE_SET_BYTE_BUDGET = 512 * 1024,
};
#define MCP_MS_TO_US 1000LL
#define MCP_S_TO_US 1000000LL

#define SLEN(s) (sizeof(s) - 1)
#include "mcp/mcp.h"
#include "mcp/mcp_internal.h"
#include "operations/operation.h"
#include "store/store.h"
#include <sqlite3.h>
#include "cypher/cypher.h"
#include "discover/discover.h"
#include "pipeline/pipeline.h"
#include "pipeline/pass_cross_repo.h"
#include "git/git_context.h"
#include "cli/cli.h"
#include "watcher/watcher.h"
#include "foundation/mem.h"
#include "foundation/diagnostics.h"
#include "foundation/platform.h"
#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/compat_thread.h"
#include "foundation/log.h"
#include "foundation/limits.h"
#include "foundation/subprocess.h"
#include "mcp/index_supervisor.h"
#include "operations/compact_out.h"
#include "foundation/str_util.h"
#include "foundation/workspace.h"
#include "foundation/dump_verify.h"
#include "foundation/compat_regex.h"
#include "pipeline/artifact.h"

#ifdef _WIN32
#include "foundation/win_utf8.h"
#include <direct.h>
#include <io.h>
#include <process.h>
#include <windows.h>
#define getpid _getpid
/* Write through the descriptor cbm_mkstemp returned rather than reopening its
 * path — see search_scratch_open. Mirrors config_toml_edit.c's toml_fdopen. */
#define mcp_fdopen _fdopen
#define mcp_close _close
#else
#include <unistd.h>
#include <poll.h>
#include <fcntl.h>
#define mcp_fdopen fdopen
#define mcp_close close
#endif
#include <yyjson/yyjson.h>
#include <ctype.h>
#include <limits.h>
#include <stdarg.h> // va_list, for the bounded help-list appender
#include <stdint.h> // int64_t
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <errno.h>
#include <stdatomic.h>

/* ── Constants ────────────────────────────────────────────────── */

/* Default snippet fallback line count */
#define SNIPPET_DEFAULT_LINES 50

/* Idle store eviction: close cached project store after this many seconds
 * of inactivity to free SQLite memory during idle periods. */
#define STORE_IDLE_TIMEOUT_S 60

/* Directory permissions: rwxr-xr-x */
#define ADR_DIR_PERMS 0755

/* JSON-RPC 2.0 standard error codes */
#define JSONRPC_PARSE_ERROR (-32700)
#define JSONRPC_METHOD_NOT_FOUND (-32601)
#define JSONRPC_INVALID_PARAMS (-32602)
#define JSONRPC_INTERNAL_ERROR (-32603)

/* MCP stdio framing limits. The body limit is also the upper bound used by the
 * daemon IPC transport; headers stay deliberately small to prevent a peer from
 * growing getline buffers without bound through ignored extension headers. */
#define MCP_MAX_MESSAGE_SIZE ((size_t)10U * 1024U * 1024U)
#define MCP_MAX_HEADER_SIZE ((size_t)8U * 1024U)
#define MCP_SEARCH_OUTPUT_MAX ((size_t)64U * 1024U * 1024U)
#define MCP_SEARCH_SCAN_TIMEOUT_MS ((uint64_t)30000U)
#define MCP_FILE_OUTLINE_OUTPUT_MAX ((size_t)2U * 1024U * 1024U)

/* ── Helpers ────────────────────────────────────────────────────── */

static char *heap_strdup(const char *s) {
    if (!s) {
        return NULL;
    }
    size_t len = strlen(s);
    char *d = malloc(len + SKIP_ONE);
    if (d) {
        memcpy(d, s, len + SKIP_ONE);
    }
    return d;
}

/* Write yyjson_mut_doc to heap-allocated JSON string.
 * ALLOW_INVALID_UNICODE: some database strings may contain non-UTF-8 bytes
 * from older indexing runs — don't fail serialization over it. */
static char *yy_doc_to_str(yyjson_mut_doc *doc) {
    size_t len = 0;
    char *s = yyjson_mut_write(doc, YYJSON_WRITE_ALLOW_INVALID_UNICODE, &len);
    return s;
}

/* ══════════════════════════════════════════════════════════════════
 *  JSON-RPC PARSING
 * ══════════════════════════════════════════════════════════════════ */

int cbm_jsonrpc_parse(const char *line, cbm_jsonrpc_request_t *out) {
    memset(out, 0, sizeof(*out));
    out->id = CBM_NOT_FOUND;

    yyjson_doc *doc = yyjson_read(line, strlen(line), 0);
    if (!doc) {
        return CBM_NOT_FOUND;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    if (!yyjson_is_obj(root)) {
        yyjson_doc_free(doc);
        return CBM_NOT_FOUND;
    }

    yyjson_val *v_jsonrpc = yyjson_obj_get(root, "jsonrpc");
    yyjson_val *v_method = yyjson_obj_get(root, "method");
    yyjson_val *v_id = yyjson_obj_get(root, "id");
    yyjson_val *v_params = yyjson_obj_get(root, "params");

    if (!v_method || !yyjson_is_str(v_method)) {
        yyjson_doc_free(doc);
        return CBM_NOT_FOUND;
    }

    out->jsonrpc =
        heap_strdup(v_jsonrpc && yyjson_is_str(v_jsonrpc) ? yyjson_get_str(v_jsonrpc) : "2.0");
    out->method = heap_strdup(yyjson_get_str(v_method));

    if (v_id) {
        out->has_id = true;
        if (yyjson_is_int(v_id)) {
            out->id = yyjson_get_int(v_id);
        } else if (yyjson_is_str(v_id)) {
            /* JSON-RPC 2.0 §4 permits string ids (Claude Desktop uses them).
             * Preserve verbatim instead of coercing via strtol (issue #253). */
            out->id_str = heap_strdup(yyjson_get_str(v_id));
        }
    }

    if (v_params) {
        out->params_raw = yyjson_val_write(v_params, 0, NULL);
    }

    yyjson_doc_free(doc);
    return 0;
}

void cbm_jsonrpc_request_free(cbm_jsonrpc_request_t *r) {
    if (!r) {
        return;
    }
    safe_str_free(&r->jsonrpc);
    safe_str_free(&r->method);
    safe_str_free(&r->id_str);
    safe_str_free(&r->params_raw);
    memset(r, 0, sizeof(*r));
}

/* ══════════════════════════════════════════════════════════════════
 *  JSON-RPC FORMATTING
 * ══════════════════════════════════════════════════════════════════ */

char *cbm_jsonrpc_format_response(const cbm_jsonrpc_response_t *resp) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "jsonrpc", "2.0");
    if (resp->id_str) {
        yyjson_mut_obj_add_str(doc, root, "id", resp->id_str);
    } else {
        yyjson_mut_obj_add_int(doc, root, "id", resp->id);
    }

    if (resp->error_json) {
        /* Parse the error JSON and embed */
        yyjson_doc *err_doc = yyjson_read(resp->error_json, strlen(resp->error_json), 0);
        if (err_doc) {
            yyjson_mut_val *err_val = yyjson_val_mut_copy(doc, yyjson_doc_get_root(err_doc));
            yyjson_mut_obj_add_val(doc, root, "error", err_val);
            yyjson_doc_free(err_doc);
        }
    } else if (resp->result_json) {
        /* Parse the result JSON and embed */
        yyjson_doc *res_doc = yyjson_read(resp->result_json, strlen(resp->result_json), 0);
        if (res_doc) {
            yyjson_mut_val *res_val = yyjson_val_mut_copy(doc, yyjson_doc_get_root(res_doc));
            yyjson_mut_obj_add_val(doc, root, "result", res_val);
            yyjson_doc_free(res_doc);
        }
    } else {
        /* JSON-RPC 2.0 spec: response MUST contain "result" or "error" */
        yyjson_mut_obj_add_null(doc, root, "result");
    }

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

char *cbm_jsonrpc_format_error(int64_t id, int code, const char *message) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "jsonrpc", "2.0");
    yyjson_mut_obj_add_int(doc, root, "id", id);

    yyjson_mut_val *err = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_int(doc, err, "code", code);
    yyjson_mut_obj_add_str(doc, err, "message", message);
    yyjson_mut_obj_add_val(doc, root, "error", err);

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

/* ══════════════════════════════════════════════════════════════════
 *  MCP PROTOCOL HELPERS
 * ══════════════════════════════════════════════════════════════════ */

char *cbm_mcp_text_result(const char *text, bool is_error) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *content = yyjson_mut_arr(doc);
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, item, "type", "text");
    yyjson_mut_obj_add_str(doc, item, "text", text ? text : "");
    yyjson_mut_arr_add_val(content, item);
    yyjson_mut_obj_add_val(doc, root, "content", content);

    bool has_structured_content = false;
    if (text) {
        yyjson_doc *structured_doc = yyjson_read(text, strlen(text), 0);
        if (structured_doc) {
            yyjson_val *structured_root = yyjson_doc_get_root(structured_doc);
            if (yyjson_is_obj(structured_root)) {
                yyjson_mut_val *structured = yyjson_val_mut_copy(doc, structured_root);
                if (structured) {
                    yyjson_mut_obj_add_val(doc, root, "structuredContent", structured);
                    has_structured_content = true;
                }
            }
            yyjson_doc_free(structured_doc);
        }
    }
    if (!has_structured_content && is_error) {
        /* structuredContent has now been wrong in both directions, so the rule
         * is spelled out here in full:
         *
         *   - JSON-object payload  -> structuredContent = the PARSED object
         *     (the branch above; the spec's structured+serialized pattern).
         *   - error                -> structuredContent = {"error": <text>} —
         *     bounded, small, and the only machine-readable failure form.
         *   - anything else       -> NO structuredContent key at all.
         *
         * Pre-#1488 the "anything else" case duplicated the payload verbatim
         * ({"text": <payload>} beside an identical content[0].text — 2.05x the
         * bytes on a 20k-node query_graph, #1375). #1488 replaced that with an
         * EMPTY object on the theory that it "still satisfies outputSchema" —
         * but clients that honor a declared outputSchema treat structuredContent
         * as THE authoritative result, so every tree-format reply rendered as
         * literally "{}" in Claude Code and friends (#1522). An empty object
         * beside a non-empty payload is not conservative; it is a wrong answer.
         *
         * The key is therefore OMITTED for text-shaped payloads, and no tool
         * declares an outputSchema anymore (see mcp_add_tool_def): output is
         * format-parameter-polymorphic, so a static schema was never truthful.
         * tests/test_mcp.c binds all three branches; scripts/smoke-test.sh
         * asserts the same contract on the shipped binary. */
        yyjson_mut_val *structured = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, structured, "error", text ? text : "");
        yyjson_mut_obj_add_val(doc, root, "structuredContent", structured);
    }
    yyjson_mut_obj_add_bool(doc, root, "isError", is_error);

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

bool cbm_mcp_cancel_request_matches(const char *params_json, int64_t active_id,
                                    const char *active_id_str) {
    if (!params_json) {
        return false;
    }

    yyjson_doc *doc = yyjson_read(params_json, strlen(params_json), 0);
    if (!doc) {
        return false;
    }

    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *request_id = yyjson_obj_get(root, "requestId");
    bool matches = false;
    if (request_id) {
        if (active_id_str) {
            matches =
                yyjson_is_str(request_id) && strcmp(yyjson_get_str(request_id), active_id_str) == 0;
        } else {
            matches = yyjson_is_int(request_id) && yyjson_get_int(request_id) == active_id;
        }
    }

    yyjson_doc_free(doc);
    return matches;
}

/* ── Tool definitions ─────────────────────────────────────────── */

typedef struct {
    const char *name;
    const char *title;
    const char *description;
    const char *input_schema; /* JSON string */
} tool_def_t;

static const tool_def_t TOOLS[] = {
    {"index_repository", "Index repository",
     "Index a repository into the knowledge graph. "
     "Special mode 'cross-repo-intelligence': skip extraction, only match Routes/Channels "
     "across projects to create CROSS_HTTP_CALLS/CROSS_ASYNC_CALLS/CROSS_CHANNEL edges. "
     "Requires target_projects param. Ensure target projects have fresh indexes first. "
     "COVERAGE: the response reports files that were NOT fully indexed — 'skipped' (not "
     "indexed at all: oversized/read/parse failures) and 'parse_partial' (indexed, but "
     "constructs inside the listed line ranges could not be parsed and MAY be missing from "
     "the graph). The embedded lists carry counts plus a FEW EXAMPLES only; the complete "
     "lists are in the per-run 'logfile' (path in the response) and queryable any time via "
     "index_status or structurally via query_graph(graph=\"missed\"). Both signals are "
     "best-effort: absence of a flag is NOT a completeness guarantee; prefer grep inside "
     "flagged ranges. Separately, 'excluded' + 'not_indexed_files' list what was "
     "deliberately NOT indexed (gitignore/.cbmignore/skip-lists) — by design, not failures.",
     "{\"type\":\"object\",\"properties\":{\"repo_path\":{\"type\":\"string\",\"description\":"
     "\"Path to the repository\"},"
     "\"mode\":{\"type\":\"string\","
     "\"enum\":[\"full\",\"moderate\",\"fast\",\"cross-repo-intelligence\"],"
     "\"default\":\"full\",\"description\":\"All modes run type-aware LSP call/usage "
     "resolution (per-file + cross-file). full: all files + similarity/semantic edges. "
     "moderate: filtered files + similarity/semantic. fast: filtered files, no "
     "similarity/semantic. cross-repo-intelligence: match Routes/Channels across projects.\"},"
     "\"target_projects\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},"
     "\"description\":\"Projects to search for cross-repo links (cross-repo-intelligence mode). "
     "Use [\\\"*\\\"] for all indexed projects. Run list_projects to see available projects.\"},"
     "\"name\":{\"type\":\"string\",\"description\":"
     "\"Override the derived project name. Non-ASCII bytes are encoded and unsafe path characters "
     "are normalized.\"},"
     "\"persistence\":{\"type\":\"boolean\",\"default\":false,\"description\":"
     "\"Write compressed artifact to .codebase-memory/graph.db.zst for team sharing. "
     "Teammates can bootstrap from the artifact instead of full re-indexing.\"}"
     "},\"required\":[\"repo_path\"]}"},

    {"search_graph", "Search graph",
     "Search the code knowledge graph for functions, classes, routes, and variables. Use INSTEAD "
     "OF grep/glob when finding code definitions, implementations, or relationships. Three search "
     "modes: (1) query='update settings' for BM25 ranked full-text search with camelCase "
     "splitting and structural label boosting — recommended for natural-language discovery; "
     "(2) name_pattern='.*regex.*' for exact pattern matching; (3) semantic_query=[...] for "
     "vector cosine search that bridges vocabulary (finds 'publish' when you search 'send'). "
     "The three modes are independent and can be combined in a single call. "
     "RESPONSE: prefix-grouped tree rows by default — a shared (qn-prefix, file) group "
     "header printed once, then `name label lines in out` per row (full qn = group prefix "
     "+ dot + name). in/out = selected degree across CALLS, USAGE, CALL_REFERENCE, "
     "INHERITS, and IMPLEMENTS; other edge types are excluded. These are NOT caller/callee "
     "counts — use trace_path for callers. Add per-node "
     "property columns via "
     "fields (e.g. [\"complexity\",\"signature\",\"docstring\"]); format=\"json\" returns "
     "the SAME tree model as structured JSON. "
     "PAGINATION: results are capped at limit (default 50). The response always includes "
     "'total' (full match count before limit) and 'has_more' (true when total > "
     "offset+returned). Detect truncation with has_more, then page by re-calling with "
     "offset=offset+limit until has_more is false. Narrow first via label/file_pattern/"
     "min_degree before paginating large result sets.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},"
     "\"query\":{\"type\":\"string\",\"description\":\"Natural-language or keyword full-text "
     "search using BM25 ranking. Tokens are split on whitespace; camelCase identifiers are "
     "indexed as individual words (updateCloudClient → update, cloud, client). Results are "
     "ranked with structural boosting: Functions/Methods +10, Routes +8, Classes/Interfaces +5. "
     "Noise labels (File/Folder/Module/Variable) are filtered out. When provided, name_pattern "
     "is ignored.\"},"
     "\"label\":{\"type\":\"string\"},\"name_pattern\":{\"type\":\"string\"},\"qn_pattern\":{"
     "\"type\":\"string\"},\"file_pattern\":{\"type\":\"string\"},"
     "\"relationship\":{\"type\":\"string\"},\"min_degree\":{\"type\":\"integer\"},"
     "\"max_degree\":{\"type\":\"integer\"},\"exclude_entry_points\":{\"type\":\"boolean\"},"
     "\"include_connected\":{\"type\":\"boolean\"},\"semantic_query\":{"
     "\"type\":\"array\",\"items\":{\"type\":\"string\"},\"description\":\"MUST be an ARRAY of "
     "keyword strings (e.g. [\\\"send\\\",\\\"pubsub\\\",\\\"publish\\\"]) — NOT a single string. "
     "Each keyword is scored independently via per-keyword min-cosine; results reflect functions "
     "that score well on ALL keywords. Requires moderate/full index mode. Results appear in the "
     "'semantic_results' field (separate from 'results').\"},\"limit\":{\"type\":"
     "\"integer\",\"description\":\"Max results per call. Default 50. Response carries "
     "'total' (full match count) and 'has_more' (true if truncated) so callers can "
     "detect the limit and paginate.\"},\"offset\":{\"type\":\"integer\",\"default\":0,"
     "\"description\":\"Skip the first N matching nodes. Combine with 'limit' to page: "
     "increment offset by limit and re-call while has_more is true.\"},"
     "\"format\":{\"type\":\"string\",\"enum\":[\"tree\",\"json\"],\"default\":\"tree\","
     "\"description\":\"Response encoding. tree (default): prefix-grouped text rows. "
     "json: the SAME tree model as structured JSON (groups + column-ordered row arrays).\"},"
     "\"fields\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},\"description\":"
     "\"Extra per-node property columns, e.g. complexity, cognitive, "
     "signature, docstring, return_type, is_test, lines(int). Core row columns "
     "(qn/label/file/lines/in/out) are always present — do not request them here. "
     "Missing values emit as empty cells.\"},"
     "\"detail\":{\"type\":\"string\",\"enum\":[\"ids\",\"default\"],\"default\":\"default\","
     "\"description\":\"ids: bare qualified-name enumeration (one column) — cheapest form "
     "for wide sweeps where per-row metadata is noise. default: full rows.\"}},"
     "\"required\":[\"project\"]}"},

    {"query_graph", "Query graph",
     "Execute a Cypher query against the knowledge graph for complex multi-hop patterns, "
     "aggregations, and cross-service analysis. The response includes 'total' (returned "
     "row count). There is a hard 100k row ceiling — for broad queries add LIMIT in the "
     "Cypher itself or use search_graph + offset/limit pagination instead. "
     "COMPLEXITY / BOTTLENECKS: every Function and Method node carries queryable complexity "
     "properties — cyclomatic (complexity), cognitive, loop_count, loop_depth (max nested-loop "
     "depth, a polynomial-degree proxy), plus interprocedural transitive_loop_depth (worst-case "
     "nested-loop degree propagated along CALLS edges) and a recursive flag. Additional "
     "hot-path signals: linear_scan_in_loop (count of find/contains/indexOf-style scans inside a "
     "loop — the hidden O(n^2) that loop_depth misses), alloc_in_loop (allocations/appends inside "
     "a loop), recursion_in_loop (a self-call inside a loop), unguarded_recursion (recursion with "
     "no conditionally-guarded base case), param_count and max_access_depth (structure smells). "
     "Find all hot-path candidates in one query, e.g. MATCH (f:Function) WHERE "
     "f.transitive_loop_depth >= 3 OR f.linear_scan_in_loop >= 1 RETURN f.qualified_name, "
     "f.transitive_loop_depth, f.linear_scan_in_loop ORDER BY f.transitive_loop_depth DESC. "
     "MISSED GRAPH: pass graph=\"missed\" to query the best-effort miss graph instead — the "
     "file structure of ONLY the files the indexer could NOT fully index (Project → Folder → "
     "File nodes with CONTAINS_FOLDER/CONTAINS_FILE edges; each File carries kind "
     "(\"parse_partial\" = indexed but constructs in the flagged line ranges MAY be missing; "
     "or a skip phase) and detail (the line ranges / reason)). Example: MATCH (f:File) WHERE "
     "f.kind = \\\"parse_partial\\\" RETURN f.file_path, f.detail. Absence from this graph is "
     "NOT a completeness guarantee.",
     "{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\",\"description\":\"Cypher "
     "query\"},\"project\":{\"type\":\"string\"},"
     "\"graph\":{\"type\":\"string\",\"enum\":[\"code\",\"missed\"],\"default\":\"code\","
     "\"description\":\"Which graph to query: the code knowledge graph (default) or the "
     "missed graph (only files not fully indexed, laid out as their file structure).\"},"
     "\"max_rows\":{\"type\":\"integer\","
     "\"description\":"
     "\"Optional row limit. Default: unlimited up to a 100k row "
     "ceiling. No offset support — use search_graph for paginated browsing.\"}},"
     "\"required\":[\"query\",\"project\"]}"},

    {"trace_path", "Trace path",
     "Trace paths through the code graph. Modes: calls (callers/callees), data_flow (value "
     "propagation with args at each hop), cross_service (through HTTP/async Route nodes). "
     "Use INSTEAD OF grep for callers, dependencies, impact analysis, or data flow tracing. "
     "RESPONSE: prefix-grouped tree rows — callees/callers grouped under their shared "
     "qn-prefix, `name hop` per row (full qn = group prefix + dot + name); exact "
     "callees_total/callers_total on every page = ALL nodes reachable within depth (transitive, "
     "not just direct; test files excluded unless include_tests). risk/args flags use a flat "
     "table. "
     "`truncated: true` + `next` = more rows — pass next back as cursor. "
     "format=\"json\" returns the SAME tree model as structured JSON.",
     "{\"type\":\"object\",\"properties\":{\"function_name\":{\"type\":\"string\"},\"project\":{"
     "\"type\":\"string\"},\"direction\":{\"type\":\"string\",\"enum\":[\"inbound\",\"outbound\","
     "\"both\"],\"default\":\"both\"},\"depth\":{\"type\":\"integer\",\"default\":3},"
     "\"limit\":{\"type\":\"integer\",\"default\":100,\"minimum\":1,\"maximum\":5000,"
     "\"description\":\"Rows per page. callees_total/callers_total always carry the exact full "
     "counts; when a page is truncated the response carries next — see cursor.\"},"
     "\"cursor\":{\"type\":\"string\",\"description\":\"Resume token from a previous response's "
     "'next' field. Pass it back with ALL other arguments identical to get the following page "
     "with no duplicates. Cursors outlive nothing: after a reindex you get a stale_cursor error "
     "— just re-run the original query.\"},\"mode\":{"
     "\"type\":\"string\",\"enum\":[\"calls\",\"data_flow\",\"cross_service\"],\"default\":"
     "\"calls\",\"description\":\"calls: follow CALLS edges. data_flow: follow CALLS+DATA_FLOWS "
     "with arg expressions. cross_service: follow HTTP_CALLS+ASYNC_CALLS+DATA_FLOWS through "
     "Routes, plus CROSS_* cross-repo edges (CROSS_HTTP_CALLS/ASYNC_CALLS/CHANNEL/GRPC_CALLS/"
     "GRAPHQL_CALLS/TRPC_CALLS) to hop into other services.\"},\"parameter_name\":{\"type\":"
     "\"string\",\"description\":\"For data_flow mode: "
     "scope trace to a specific parameter name\"},\"edge_types\":{\"type\":\"array\",\"items\":{"
     "\"type\":\"string\"}},\"risk_labels\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Add risk classification (CRITICAL/HIGH/MEDIUM/LOW) based on hop distance"
     "\"},\"include_tests\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Include test files in results. When false (default), test files are "
     "filtered out. When true, test nodes are included with a test column/marker.\"},"
     "\"format\":{\"type\":\"string\",\"enum\":[\"tree\",\"json\"],\"default\":\"tree\","
     "\"description\":\"Response encoding. tree (default): prefix-grouped text rows. "
     "json: the SAME tree model as structured JSON (groups + column-ordered row arrays).\"},"
     "\"include_evidence\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Add how each hop was resolved: a strategy class (lsp | language_rule | "
     "heuristic | unresolved) and the resolver's confidence. Off by default — it adds two "
     "columns per row. Use it to judge whether an edge is trustworthy, not to find edges.\"}},"
     "\"required\":[\"function_name\",\"project\"]}"},

    {"get_code_snippet", "Get code snippet",
     "Read source code for a function/class/symbol. IMPORTANT: First call search_graph to find the "
     "exact qualified_name, then pass it here. This is a read tool, not a search tool. Accepts "
     "full qualified_name (exact match) or short function name (returns suggestions if ambiguous). "
     "If the response carries a 'coverage_note', the file was only partially indexed — constructs "
     "in the noted line ranges may be missing from the graph (best-effort signal); prefer grep "
     "there and treat the returned source as ground truth.",
     "{\"type\":\"object\",\"properties\":{\"qualified_name\":{\"type\":\"string\",\"description\":"
     "\"Full qualified_name from search_graph, or short function name\"},\"project\":{"
     "\"type\":\"string\"},\"include_neighbors\":{"
     "\"type\":\"boolean\",\"default\":false}},\"required\":[\"qualified_name\",\"project\"]}"},

    {"get_file_outline", "Get file outline",
     "Return a compact declaration outline for one exact repository-relative file. Results are "
     "filtered by optional exact labels, ordered deterministically by source position, and "
     "bounded with exact total/offset/limit pagination metadata. File/folder/container nodes "
     "are excluded. The query observes request cancellation and fails without partial output.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},"
     "\"file_path\":{\"type\":\"string\",\"description\":\"Exact repository-relative "
     "file path\"},\"labels\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},"
     "\"maxItems\":16,\"description\":\"Optional exact node-label filter\"},"
     "\"limit\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":200,\"default\":100},"
     "\"offset\":{\"type\":\"integer\",\"minimum\":0,\"default\":0},"
     "\"format\":{\"type\":\"string\",\"enum\":[\"tree\",\"json\"],"
     "\"default\":\"tree\"}},\"additionalProperties\":false,"
     "\"required\":[\"project\",\"file_path\"]}"},

    {"get_graph_schema", "Get graph schema",
     "Get the schema of the knowledge graph (node labels, edge types)",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"}},\"required\":["
     "\"project\"]}"},

    {"compare_graphs", "Compare graphs",
     "Compare two indexed project snapshots. Returns deterministic target-only additions and "
     "base-only removals for stable node and edge identities using a bounded streaming merge. "
     "Each result set is independently capped by limit and a fixed 512 KiB encoded-byte budget; "
     "exact totals and truncation reasons are always reported.",
     "{\"type\":\"object\",\"properties\":{"
     "\"base_project\":{\"type\":\"string\",\"minLength\":1},"
     "\"target_project\":{\"type\":\"string\",\"minLength\":1},"
     "\"limit\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":1000,\"default\":200},"
     "\"scan_limit\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":10000000,"
     "\"default\":2000000}},\"required\":[\"base_project\",\"target_project\"],"
     "\"additionalProperties\":false}"},

    {"get_architecture", "Get architecture",
     "Get high-level architecture overview. DEFAULT (no aspects) is a compact summary — "
     "overview counts, languages, packages, entry_points; request more via aspects:[...] "
     "(structure, dependencies, routes, hotspots, boundaries, layers, clusters, file_tree) or "
     "[\"all\"]. 'clusters' runs Leiden community detection over the call/import graph, "
     "surfacing the de-facto modules (label, member count, cohesion score, representative "
     "top_nodes, binding packages/edge_types) — the real architectural seams, which often cut "
     "across the folder layout. Optional path scopes analysis to nodes under that directory "
     "prefix (file_path).",
     /* The aspects enum mirrors VALID_ASPECTS (see aspect_is_valid) — update both together. */
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"path\":{\"type\":"
     "\"string\",\"description\":\"Optional directory prefix to scope architecture (e.g. "
     "apps/hoa)\"},"
     "\"aspects\":{\"type\":\"array\",\"items\":{\"type\":\"string\",\"enum\":[\"all\","
     "\"overview\",\"structure\",\"dependencies\",\"routes\",\"languages\",\"packages\","
     "\"entry_points\",\"hotspots\",\"boundaries\",\"layers\",\"file_tree\",\"clusters\","
     "\"cycles\"]},"
     "\"description\":\"Aspects to include. 'all' = everything; 'overview' = compact summary "
     "(all except file_tree); omit = all. 'cycles' is opt-in ONLY (never via all/overview): it "
     "scans the whole call graph for circular CALLS dependencies (SCCs of size > 1).\"}},"
     "\"required\":[\"project\"]}"},

    {"search_code", "Search code",
     "Graph-augmented code search. Finds text patterns via grep, then enriches results with "
     "the knowledge graph: deduplicates matches into containing functions, ranks by structural "
     "importance (definitions first, popular functions next, tests last). "
     "Modes: compact (default, signatures only — token efficient), full (source capped at a "
     "60-line window around the first match per hit; source_truncated marks the cut — use "
     "get_code_snippet for the complete symbol), "
     "files (just file paths). Use path_filter regex to scope results. "
     "TRUNCATION: enriched results are capped at limit (default 10). Response carries "
     "'total_grep_matches' (raw grep hit count) and 'total_results' (deduplicated function "
     "count) — compare to limit to detect truncation. There is no offset parameter; to see "
     "more, raise limit or narrow the query with file_pattern / path_filter.",
     "{\"type\":\"object\",\"properties\":{\"pattern\":{\"type\":\"string\"},\"project\":{\"type\":"
     "\"string\"},\"file_pattern\":{\"type\":\"string\",\"description\":\"Glob for grep "
     "--include (e.g. *.go)\"},\"path_filter\":{\"type\":\"string\",\"description\":\"Regex "
     "filter on result file paths (e.g. ^src/ or \\\\.(go|ts)$)\"},\"mode\":{\"type\":\"string\","
     "\"enum\":[\"compact\",\"full\",\"files\"],\"default\":\"compact\",\"description\":\"compact: "
     "signatures+metadata (default). full: with source. files: just file list.\"},"
     "\"context\":{\"type\":\"integer\",\"description\":\"Lines of context around each match "
     "(like grep -C). Only used in compact mode.\"},"
     "\"regex\":{\"type\":\"boolean\",\"default\":false},\"debug\":{\"type\":\"boolean\","
     "\"default\":false,\"description\":\"Include scope_ms, scan_ms, and enrich_ms phase timing "
     "diagnostics.\"},\"limit\":{\"type\":\"integer\","
     "\"description\":\"Max enriched results per call. Default 10. Response includes "
     "'total_grep_matches' and 'total_results' so callers can detect truncation. No "
     "offset parameter — raise limit or narrow with file_pattern / path_filter to see more."
     "\",\"default\":10,\"minimum\":1}},\"required\":[\"pattern\",\"project\"]}"},

    {"list_projects", "List projects", "List indexed projects with deterministic pagination",
     "{\"type\":\"object\",\"properties\":{\"offset\":{\"type\":\"integer\","
     "\"minimum\":0,\"default\":0},"
     "\"limit\":{\"type\":\"integer\",\"minimum\":1,\"maximum\":100,\"default\":50},"
     "\"include_details\":{\"type\":\"boolean\",\"default\":false,"
     "\"description\":\"Include branch, node/edge counts and database size. Slower.\"},"
     "\"metadata_only\":{\"type\":\"boolean\",\"description\":\"Deprecated compatibility "
     "alias for include_details=false.\"}}}"},
    {"delete_project", "Delete project", "Delete a project from the index",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"}},\"required\":["
     "\"project\"]}"},

    {"index_status", "Index status",
     "Get the indexing status of a project: node/edge counts, root path, git context, and the "
     "indexing-COVERAGE report — which files the indexer could NOT fully cover (best-effort "
     "signal): 'parse_partial' files WERE indexed but contain line ranges tree-sitter could not "
     "parse — constructs there MAY be missing from the graph (some are still recovered); "
     "'skipped' files were not indexed at all (oversized/read/parse failure). Use this before "
     "trusting graph completeness on a file: if a file is listed, ALSO grep it (especially the "
     "flagged ranges). IMPORTANT: absence from these lists is NOT a completeness guarantee — the "
     "signal only marks what the indexer can detect. For structural queries over the misses use "
     "query_graph(graph=\"missed\"). The report also carries 'not_indexed' — files/dirs excluded "
     "BY DESIGN (gitignore/.cbmignore/skip-lists): deliberate and deterministic, not failures; "
     "change the ignore rules and re-index to include them.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},"
     "\"verbose\":{\"type\":\"boolean\",\"default\":false,\"description\":\"Include the git "
     "context block (worktree/shadow path variants). Only needed when debugging where an index "
     "lives — omitted by default to keep the status lean.\"}},\"required\":["
     "\"project\"]}"},

    {"check_index_coverage", "Check index coverage",
     "Check authoritative indexing-coverage metadata for exact repository-relative paths and "
     "bounded path scopes. Use this after graph discovery for every cited or operated-on file; "
     "use scopes before negative/exhaustive claims because fully skipped files cannot appear in "
     "normal graph results. Returns coverage status separately from filesystem metadata freshness, "
     "plus structured parse-error ranges and direct-source fallback actions. The signal is "
     "best-effort: indexed_no_recorded_gap is not a completeness guarantee. At least one of "
     "'paths' or 'scopes' is required; the call is rejected at runtime if both are omitted.",
     "{\"type\":\"object\",\"properties\":{"
     "\"project\":{\"type\":\"string\"},"
     "\"paths\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},\"maxItems\":128,"
     "\"description\":\"Repository-relative files to check exactly. Required if 'scopes' is "
     "omitted.\"},"
     "\"scopes\":{\"type\":\"array\",\"items\":{\"type\":\"string\"},\"maxItems\":32,"
     "\"description\":\"Repository-relative path prefixes; use . for the project root. Required "
     "if 'paths' is omitted.\"},"
     "\"scope_limit\":{\"type\":\"integer\",\"default\":200,\"minimum\":1,\"maximum\":1000},"
     "\"scope_offset\":{\"type\":\"integer\",\"default\":0,\"minimum\":0}},"
     "\"required\":[\"project\"]"
     "}"},

    {"detect_changes", "Detect changes",
     "Map a git diff to its BLAST RADIUS. Resolves changed files to the symbols they define, then "
     "runs ONE multi-source graph traversal to the transitive impact set. RESPONSE: base + "
     "merge_base SHA, changed_files list, then impacted = prefix-grouped tree rows (name label "
     "hop; "
     "full qn = group prefix + dot + name) + an impacted_modules rollup; impacted_total + "
     "truncated are exact. Seeds (the changed symbols) are excluded from impacted; a changed file "
     "reached from another changed file is not counted as extra impact. format=\"json\" returns "
     "the "
     "same model as structured JSON.",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"scope\":{\"type\":"
     "\"string\",\"enum\":[\"files\",\"impact\"],\"description\":\"files: changed files only "
     "(no traversal). impact (default): files + the transitive impact set.\"},"
     "\"direction\":{\"type\":\"string\",\"enum\":[\"inbound\",\"outbound\",\"both\"],\"default\":"
     "\"inbound\",\"description\":\"inbound (default) = the blast radius: transitive CALLERS of "
     "the "
     "changed symbols. outbound = what the changed code depends on. both = union.\"},"
     "\"depth\":{\"type\":\"integer\",\"default\":2,\"description\":\"Max traversal hops from the "
     "changed symbols.\"},\"limit\":{\"type\":\"integer\",\"default\":200,\"maximum\":5000,"
     "\"description\":\"Per-symbol impacted rows shown (nearest hops first). impacted_total is "
     "always exact and the impacted_modules rollup always complete regardless.\"},"
     "\"base_branch\":{\"type\":"
     "\"string\",\"default\":\"main\"},\"since\":{\"type\":\"string\",\"description\":"
     "\"Git ref or tag to compare from (e.g. HEAD~5, v0.5.0). Diffs <ref>...HEAD.\"},"
     "\"format\":{\"type\":\"string\",\"enum\":[\"tree\",\"json\"],\"default\":\"tree\"}},"
     "\"required\":"
     "[\"project\"]}"},

    {"manage_adr", "Manage ADR", "Create or update Architecture Decision Records",
     "{\"type\":\"object\",\"properties\":{\"project\":{\"type\":\"string\"},\"mode\":{\"type\":"
     "\"string\",\"enum\":[\"get\",\"update\",\"set_sections\",\"sections\"],\"description\":"
     "\"update REPLACES the entire ADR document; set_sections rewrites only the named sections "
     "and leaves every other byte of the stored document untouched, so adding one entry does not "
     "mean re-sending the whole ADR (setting the same section to the same body twice leaves the "
     "document byte-identical, so retrying after a lost response is safe); sections only lists "
     "existing headings\"},\"content\":{\"type\":\"string\",\"description\":\"Complete replacement "
     "document required by update\"},\"section_updates\":{\"type\":\"object\",\"description\":"
     "\"Required by set_sections: section name -> new body for that section. Any heading name "
     "works, so a new entry can be added under its own heading; PURPOSE, STACK, ARCHITECTURE, "
     "PATTERNS, TRADEOFFS and PHILOSOPHY are the conventional ones. Names match exactly, "
     "including case, so 'Purpose' and 'PURPOSE' are different sections.\","
     "\"additionalProperties\":{\"type\":\"string\"}}},\"additionalProperties\":false,"
     "\"required\":[\"project\"]}"},

    {"ingest_traces", "Ingest traces", "Ingest runtime traces to enhance the knowledge graph",
     "{\"type\":\"object\",\"properties\":{\"traces\":{\"type\":\"array\",\"items\":{\"type\":"
     "\"object\",\"properties\":{\"caller\":{\"type\":\"string\"},\"callee\":{\"type\":\"string\"},"
     "\"count\":{\"type\":\"integer\"}},\"additionalProperties\":false}},\"project\":{\"type\":"
     "\"string\"}},\"required\":[\"traces\",\"project\"]}"},
};

static const int TOOL_COUNT = sizeof(TOOLS) / sizeof(TOOLS[0]);

typedef struct {
    const char *name;
    bool read_only;
    bool destructive;
    bool idempotent;
    bool open_world;
} tool_annotation_def_t;

/* Tool annotations are deliberately explicit. All tools operate on the local
 * repository/index domain, so none cross an open-world trust boundary. */
static const tool_annotation_def_t TOOL_ANNOTATIONS[] = {
    {"index_repository", false, false, true, false},
    {"search_graph", false, true, true, false},
    {"query_graph", false, true, true, false},
    {"trace_path", false, true, true, false},
    {"get_code_snippet", false, true, true, false},
    {"get_file_outline", false, true, true, false},
    {"get_graph_schema", false, true, true, false},
    {"compare_graphs", true, false, true, false},
    {"get_architecture", false, true, true, false},
    {"search_code", false, true, true, false},
    {"list_projects", true, false, true, false},
    {"delete_project", false, true, true, false},
    {"index_status", false, true, true, false},
    {"check_index_coverage", false, true, true, false},
    {"detect_changes", false, true, true, false},
    {"manage_adr", false, true, false, false},
    {"ingest_traces", false, false, false, false},
};

static const tool_annotation_def_t *mcp_tool_annotations(const char *name) {
    size_t count = sizeof(TOOL_ANNOTATIONS) / sizeof(TOOL_ANNOTATIONS[0]);
    for (size_t i = 0; i < count; i++) {
        if (strcmp(TOOL_ANNOTATIONS[i].name, name) == 0) {
            return &TOOL_ANNOTATIONS[i];
        }
    }
    return NULL;
}

static void mcp_add_json_schema(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key,
                                const char *schema_json) {
    yyjson_doc *schema_doc = yyjson_read(schema_json, strlen(schema_json), 0);
    if (schema_doc) {
        yyjson_mut_val *schema = yyjson_val_mut_copy(doc, yyjson_doc_get_root(schema_doc));
        if (schema) {
            yyjson_mut_obj_add_val(doc, obj, key, schema);
        }
        yyjson_doc_free(schema_doc);
    }
}

static void mcp_add_tool_def(yyjson_mut_doc *doc, yyjson_mut_val *tools, int i) {
    yyjson_mut_val *tool = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, tool, "name", TOOLS[i].name);
    yyjson_mut_obj_add_str(doc, tool, "title", TOOLS[i].title);
    yyjson_mut_obj_add_str(doc, tool, "description", TOOLS[i].description);

    mcp_add_json_schema(doc, tool, "inputSchema", TOOLS[i].input_schema);
    /* Deliberately NO outputSchema. Tool output is format-parameter-polymorphic
     * (tree text by default, a JSON object under format:"json"), so no static
     * schema is truthful — and a declared schema makes spec-honoring clients
     * read structuredContent as the authoritative result, which is exactly how
     * the empty-object regression rendered every tree reply as "{}" (#1522).
     * The blanket {"type":"object","additionalProperties":true} it replaced
     * validated anything and informed nobody. */

    const tool_annotation_def_t *def = mcp_tool_annotations(TOOLS[i].name);
    yyjson_mut_val *annotations = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_bool(doc, annotations, "readOnlyHint", def ? def->read_only : false);
    yyjson_mut_obj_add_bool(doc, annotations, "destructiveHint", def ? def->destructive : true);
    yyjson_mut_obj_add_bool(doc, annotations, "idempotentHint", def ? def->idempotent : false);
    yyjson_mut_obj_add_bool(doc, annotations, "openWorldHint", def ? def->open_world : true);
    yyjson_mut_obj_add_val(doc, tool, "annotations", annotations);

    yyjson_mut_arr_add_val(tools, tool);
}

static bool mcp_tool_allowed(cbm_mcp_tool_profile_t profile, const char *name) {
    static const char *const analysis_tools[] = {
        "search_graph",     "query_graph",      "trace_path",     "get_code_snippet",
        "get_file_outline", "get_graph_schema", "compare_graphs", "get_architecture",
        "search_code",      "list_projects",    "index_status",   "check_index_coverage",
        "detect_changes",
    };
    static const char *const scout_tools[] = {
        "search_graph",     "trace_path",    "get_code_snippet", "get_file_outline",
        "get_architecture", "list_projects", "index_status",     "check_index_coverage",
    };
    if (!name) {
        return false;
    }
    if (profile == CBM_MCP_TOOL_PROFILE_ALL) {
        return true;
    }
    const char *const *allowed = NULL;
    size_t allowed_count = 0U;
    if (profile == CBM_MCP_TOOL_PROFILE_ANALYSIS) {
        allowed = analysis_tools;
        allowed_count = sizeof(analysis_tools) / sizeof(analysis_tools[0]);
    } else if (profile == CBM_MCP_TOOL_PROFILE_SCOUT) {
        allowed = scout_tools;
        allowed_count = sizeof(scout_tools) / sizeof(scout_tools[0]);
    }
    for (size_t i = 0U; i < allowed_count; i++) {
        if (strcmp(name, allowed[i]) == 0) {
            return true;
        }
    }
    return false;
}

static const char *mcp_tool_profile_name(cbm_mcp_tool_profile_t profile) {
    return profile == CBM_MCP_TOOL_PROFILE_SCOUT ? "scout" : "analysis";
}

int cbm_mcp_parse_tool_profile_args(int argc, const char *const argv[const],
                                    cbm_mcp_tool_profile_t *profile_out) {
    if (argc < 0 || !argv || !profile_out) {
        return -1;
    }
    *profile_out = CBM_MCP_TOOL_PROFILE_ALL;
    for (int i = 1; i < argc; i++) {
        const char *arg = argv[i];
        if (!arg) {
            return -1;
        }
        if (strcmp(arg, "--tool-profile=analysis") == 0) {
            *profile_out = CBM_MCP_TOOL_PROFILE_ANALYSIS;
            continue;
        }
        if (strcmp(arg, "--tool-profile=scout") == 0) {
            *profile_out = CBM_MCP_TOOL_PROFILE_SCOUT;
            continue;
        }
        if (strcmp(arg, "--tool-profile") == 0) {
            if (i + 1 >= argc || !argv[i + 1]) {
                return -1;
            }
            if (strcmp(argv[i + 1], "analysis") == 0) {
                *profile_out = CBM_MCP_TOOL_PROFILE_ANALYSIS;
            } else if (strcmp(argv[i + 1], "scout") == 0) {
                *profile_out = CBM_MCP_TOOL_PROFILE_SCOUT;
            } else {
                return -1;
            }
            i++;
            continue;
        }
        if (strncmp(arg, "--tool-profile=", strlen("--tool-profile=")) == 0) {
            return -1;
        }
    }
    return 0;
}

bool cbm_mcp_tool_profile_allows_http(cbm_mcp_tool_profile_t profile) {
    return profile == CBM_MCP_TOOL_PROFILE_ALL;
}

static int mcp_allowed_tool_count(cbm_mcp_tool_profile_t profile) {
    int count = 0;
    for (int i = 0; i < TOOL_COUNT; i++) {
        if (mcp_tool_allowed(profile, TOOLS[i].name)) {
            count++;
        }
    }
    return count;
}

static char *cbm_mcp_tools_list_range(cbm_mcp_tool_profile_t profile, int offset, int limit,
                                      bool include_next_cursor) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *tools = yyjson_mut_arr(doc);

    if (offset < 0) {
        offset = 0;
    }
    int allowed_count = mcp_allowed_tool_count(profile);
    if (offset > allowed_count) {
        offset = allowed_count;
    }
    if (limit < 0 || limit > allowed_count) {
        limit = allowed_count;
    }

    int end = offset + limit;
    if (end > allowed_count) {
        end = allowed_count;
    }

    int visible = 0;
    for (int i = 0; i < TOOL_COUNT && visible < end; i++) {
        if (!mcp_tool_allowed(profile, TOOLS[i].name)) {
            continue;
        }
        if (visible >= offset) {
            mcp_add_tool_def(doc, tools, i);
        }
        visible++;
    }

    yyjson_mut_obj_add_val(doc, root, "tools", tools);
    if (include_next_cursor && end < allowed_count) {
        char cursor[32];
        snprintf(cursor, sizeof(cursor), "%d", end);
        yyjson_mut_obj_add_strcpy(doc, root, "nextCursor", cursor);
    }

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

char *cbm_mcp_tools_list(void) {
    return cbm_mcp_tools_list_range(CBM_MCP_TOOL_PROFILE_ALL, 0, TOOL_COUNT, false);
}

/* Return the JSON input_schema string for a tool by name, or NULL if unknown.
 * Used by the CLI to build --flag arguments and per-tool --help from the same
 * source of truth the MCP tools/list advertises. Static lifetime; do not free. */
const char *cbm_mcp_tool_input_schema(const char *tool_name) {
    if (!tool_name) {
        return NULL;
    }
    for (int i = 0; i < TOOL_COUNT; i++) {
        if (strcmp(TOOLS[i].name, tool_name) == 0) {
            return TOOLS[i].input_schema;
        }
    }
    return NULL;
}

int cbm_mcp_tool_count(void) {
    return TOOL_COUNT;
}

const char *cbm_mcp_tool_name(int index) {
    if (index < 0 || index >= TOOL_COUNT) {
        return NULL;
    }
    return TOOLS[index].name;
}

const char *cbm_mcp_tool_title(const char *tool_name) {
    if (!tool_name) {
        return NULL;
    }
    for (int i = 0; i < TOOL_COUNT; i++) {
        if (strcmp(TOOLS[i].name, tool_name) == 0) {
            return TOOLS[i].title;
        }
    }
    return NULL;
}

const char *cbm_mcp_tool_description(const char *tool_name) {
    if (!tool_name) {
        return NULL;
    }
    for (int i = 0; i < TOOL_COUNT; i++) {
        if (strcmp(TOOLS[i].name, tool_name) == 0) {
            return TOOLS[i].description;
        }
    }
    return NULL;
}

/* Render the top-level --help "Tools:" block from the registry tools/list
 * serves. The list used to be hand-maintained in the help text and drifted
 * when check_index_coverage was added (#1361); deriving it here makes that
 * divergence impossible. Heap-allocated; caller frees. */
/* Append at out[len] and return the bytes ACTUALLY written.
 *
 * snprintf returns the length it WOULD have written, so accumulating that value
 * lets len run past cap; the next `cap - len` then underflows to a huge size_t
 * and the following write lands outside the buffer. CodeQL flagged exactly that
 * shape here, and it is the same class #1173 just fixed in the Cypher list
 * builder. The capacity computed below does happen to be sufficient today —
 * which makes this the more dangerous version, not the safer one: the code is
 * correct only by an argument made ten lines away, so renaming a tool or
 * changing the wrap rule would turn it into an overflow with nothing to notice.
 * Clamping makes `len <= cap - 1` a local invariant no later edit can void. */
static size_t help_append(char *out, size_t cap, size_t len, const char *fmt, ...) {
    if (len + 1 >= cap) {
        return 0;
    }
    va_list args;
    va_start(args, fmt);
    int written = vsnprintf(out + len, cap - len, fmt, args);
    va_end(args);
    if (written <= 0) {
        return 0;
    }
    size_t room = cap - len - 1;
    return (size_t)written > room ? room : (size_t)written;
}

char *cbm_mcp_tools_help_list(void) {
    size_t cap = SLEN("Tools:") + 2; /* trailing newline + NUL */
    for (int i = 0; i < TOOL_COUNT; i++) {
        cap += strlen(TOOLS[i].name) + SLEN(" ,\n "); /* per-tool worst case incl. a wrap */
    }
    char *out = malloc(cap);
    if (!out) {
        return NULL;
    }
    size_t len = help_append(out, cap, 0, "Tools:");
    size_t col = len;
    for (int i = 0; i < TOOL_COUNT; i++) {
        const char *sep = (i + 1 < TOOL_COUNT) ? "," : "";
        size_t item = SLEN(" ") + strlen(TOOLS[i].name) + strlen(sep);
        if (i > 0 && col + item > MCP_HELP_TOOLS_WRAP_COL) {
            len += help_append(out, cap, len, "\n ");
            col = 1;
        }
        size_t wrote = help_append(out, cap, len, " %s%s", TOOLS[i].name, sep);
        len += wrote;
        col += wrote;
    }
    len += help_append(out, cap, len, "\n");
    (void)len; /* final length is not needed; the buffer is NUL-terminated */
    return out;
}

static int mcp_tools_cursor_offset(const char *params_json, bool *has_cursor_out) {
    if (has_cursor_out) {
        *has_cursor_out = false;
    }
    if (!params_json) {
        return 0;
    }

    yyjson_doc *doc = yyjson_read(params_json, strlen(params_json), 0);
    if (!doc) {
        return 0;
    }

    int offset = 0;
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *cursor = root ? yyjson_obj_get(root, "cursor") : NULL;
    if (cursor) {
        if (has_cursor_out) {
            *has_cursor_out = true;
        }
        offset = TOOL_COUNT;
        if (yyjson_is_str(cursor)) {
            const char *cursor_str = yyjson_get_str(cursor);
            if (cursor_str && *cursor_str != '\0') {
                char *endptr = NULL;
                errno = 0;
                long parsed = strtol(cursor_str, &endptr, 10);
                if (endptr && *endptr == '\0' && errno == 0 && parsed >= 0) {
                    offset = parsed > TOOL_COUNT ? TOOL_COUNT : (int)parsed;
                }
            }
        }
    }

    yyjson_doc_free(doc);
    return offset;
}

static char *cbm_mcp_tools_list_page(cbm_mcp_tool_profile_t profile, const char *params_json) {
    bool has_cursor = false;
    int offset = mcp_tools_cursor_offset(params_json, &has_cursor);
    if (!has_cursor) {
        return cbm_mcp_tools_list_range(profile, 0, TOOL_COUNT, false);
    }
    return cbm_mcp_tools_list_range(profile, offset, MCP_TOOLS_PAGE_SIZE, true);
}

/* ── Prompt definitions ───────────────────────────────────────── */

static void mcp_add_prompt_argument(yyjson_mut_doc *doc, yyjson_mut_val *arguments,
                                    const char *name, const char *title, const char *description,
                                    bool required) {
    yyjson_mut_val *argument = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, argument, "name", name);
    yyjson_mut_obj_add_str(doc, argument, "title", title);
    yyjson_mut_obj_add_str(doc, argument, "description", description);
    yyjson_mut_obj_add_bool(doc, argument, "required", required);
    yyjson_mut_arr_add_val(arguments, argument);
}

static char *cbm_mcp_prompts_list(void) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_val *prompts = yyjson_mut_arr(doc);

    yyjson_mut_val *explore = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, explore, "name", "explore_codebase");
    yyjson_mut_obj_add_str(doc, explore, "title", "Explore codebase");
    yyjson_mut_obj_add_str(doc, explore, "description",
                           "Explore a codebase with graph-first structural discovery.");
    yyjson_mut_val *explore_arguments = yyjson_mut_arr(doc);
    mcp_add_prompt_argument(doc, explore_arguments, "project", "Project",
                            "Indexed project name from list_projects.", true);
    mcp_add_prompt_argument(doc, explore_arguments, "question", "Question",
                            "Architecture or implementation question to investigate.", true);
    yyjson_mut_obj_add_val(doc, explore, "arguments", explore_arguments);
    yyjson_mut_arr_add_val(prompts, explore);

    yyjson_mut_val *review = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, review, "name", "review_change_impact");
    yyjson_mut_obj_add_str(doc, review, "title", "Review change impact");
    yyjson_mut_obj_add_str(doc, review, "description",
                           "Review affected callers, tests, boundaries, and risks.");
    yyjson_mut_val *review_arguments = yyjson_mut_arr(doc);
    mcp_add_prompt_argument(doc, review_arguments, "project", "Project",
                            "Indexed project name from list_projects.", true);
    mcp_add_prompt_argument(doc, review_arguments, "change", "Change",
                            "Change, symbol, or area whose impact should be reviewed.", true);
    mcp_add_prompt_argument(doc, review_arguments, "base_branch", "Base branch",
                            "Git branch or ref for detect_changes; defaults to main.", false);
    yyjson_mut_obj_add_val(doc, review, "arguments", review_arguments);
    yyjson_mut_arr_add_val(prompts, review);

    yyjson_mut_obj_add_val(doc, root, "prompts", prompts);
    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

static const char *mcp_prompt_string_argument(yyjson_val *arguments, const char *name) {
    if (!arguments || !yyjson_is_obj(arguments)) {
        return NULL;
    }
    yyjson_val *value = yyjson_obj_get(arguments, name);
    if (!value || !yyjson_is_str(value)) {
        return NULL;
    }
    const char *text = yyjson_get_str(value);
    return text && text[0] ? text : NULL;
}

static char *mcp_prompt_result(const char *description, const char *text) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "description", description);

    yyjson_mut_val *messages = yyjson_mut_arr(doc);
    yyjson_mut_val *message = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, message, "role", "user");
    yyjson_mut_val *content = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, content, "type", "text");
    yyjson_mut_obj_add_str(doc, content, "text", text);
    yyjson_mut_obj_add_val(doc, message, "content", content);
    yyjson_mut_arr_add_val(messages, message);
    yyjson_mut_obj_add_val(doc, root, "messages", messages);

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

static char *mcp_prompt_error_json(int code, const char *message) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_int(doc, root, "code", code);
    yyjson_mut_obj_add_str(doc, root, "message", message);
    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

static char *cbm_mcp_prompt_get(const char *params_json, char **error_json) {
    *error_json = NULL;
    yyjson_doc *params_doc = params_json ? yyjson_read(params_json, strlen(params_json), 0) : NULL;
    yyjson_val *params = params_doc ? yyjson_doc_get_root(params_doc) : NULL;
    yyjson_val *name_value =
        params && yyjson_is_obj(params) ? yyjson_obj_get(params, "name") : NULL;
    if (!name_value || !yyjson_is_str(name_value)) {
        *error_json = mcp_prompt_error_json(JSONRPC_INVALID_PARAMS, "Invalid prompt name");
        if (params_doc) {
            yyjson_doc_free(params_doc);
        }
        return NULL;
    }

    const char *name = yyjson_get_str(name_value);
    bool is_explore = strcmp(name, "explore_codebase") == 0;
    bool is_review = strcmp(name, "review_change_impact") == 0;
    if (!is_explore && !is_review) {
        *error_json = mcp_prompt_error_json(JSONRPC_INVALID_PARAMS, "Invalid prompt name");
        yyjson_doc_free(params_doc);
        return NULL;
    }

    yyjson_val *arguments = yyjson_obj_get(params, "arguments");
    const char *project = mcp_prompt_string_argument(arguments, "project");
    const char *request = mcp_prompt_string_argument(arguments, is_explore ? "question" : "change");
    if (!project || !request) {
        *error_json =
            mcp_prompt_error_json(JSONRPC_INVALID_PARAMS, "Missing required prompt arguments");
        yyjson_doc_free(params_doc);
        return NULL;
    }

    const char *base_branch = "main";
    yyjson_val *base_branch_value = is_review && arguments && yyjson_is_obj(arguments)
                                        ? yyjson_obj_get(arguments, "base_branch")
                                        : NULL;
    if (base_branch_value) {
        if (!yyjson_is_str(base_branch_value) || !yyjson_get_str(base_branch_value)[0]) {
            *error_json = mcp_prompt_error_json(JSONRPC_INVALID_PARAMS, "Invalid prompt arguments");
            yyjson_doc_free(params_doc);
            return NULL;
        }
        base_branch = yyjson_get_str(base_branch_value);
    }

    static const char EXPLORE_TEMPLATE[] =
        "Explore project \"%s\" to answer: %s\n\n"
        "Use graph tools first: search_graph to find relevant symbols, get_code_snippet for "
        "exact source, and trace_path(direction=\"both\") for callers and callees. Use "
        "get_architecture for broad orientation and query_graph only for multi-hop patterns. "
        "Check has_more and paginate. Fall back to search_code or grep only for literal or "
        "non-code text, or where graph coverage is incomplete.";
    static const char REVIEW_TEMPLATE[] =
        "Review change impact in project \"%s\" for: %s\n\n"
        "Use detect_changes with base_branch \"%s\", then trace_path(direction=\"both\", "
        "include_tests=true) for affected callers, callees, and tests. Read exact definitions "
        "with get_code_snippet and use query_graph for cross-boundary patterns. Report affected "
        "callers, tests, boundaries, and risks; do not modify files.";

    size_t text_size = strlen(project) + strlen(request) + strlen(base_branch) +
                       (is_explore ? sizeof(EXPLORE_TEMPLATE) : sizeof(REVIEW_TEMPLATE));
    char *text = malloc(text_size);
    if (!text) {
        *error_json = mcp_prompt_error_json(JSONRPC_INTERNAL_ERROR, "Internal error");
        yyjson_doc_free(params_doc);
        return NULL;
    }
    if (is_explore) {
        snprintf(text, text_size, EXPLORE_TEMPLATE, project, request);
    } else {
        snprintf(text, text_size, REVIEW_TEMPLATE, project, request, base_branch);
    }

    char *result = mcp_prompt_result(
        is_explore ? "Graph-first codebase exploration" : "Graph-first change-impact review", text);
    free(text);
    yyjson_doc_free(params_doc);
    return result;
}

/* Supported protocol versions, newest first. The server picks the newest
 * version that it shares with the client (per MCP spec version negotiation). */
static const char *SUPPORTED_PROTOCOL_VERSIONS[] = {
    "2025-11-25",
    "2025-06-18",
    "2025-03-26",
    "2024-11-05",
};
static const int SUPPORTED_VERSION_COUNT =
    (int)(sizeof(SUPPORTED_PROTOCOL_VERSIONS) / sizeof(SUPPORTED_PROTOCOL_VERSIONS[0]));

static const char MCP_SERVER_INSTRUCTIONS[] =
    "Use graph tools first for structural code discovery: search_graph to find symbols, "
    "trace_path for callers and callees, get_code_snippet for exact source, query_graph for "
    "complex multi-hop patterns, and get_architecture for orientation. Use search_code or "
    "filesystem grep for literal or non-code text, or when graph coverage is insufficient. "
    "Call list_projects before initial use and index_repository only when a repository is not "
    "indexed or to force immediate freshness after a large external update. Once indexed, "
    "watched projects auto-refresh in the background; use index_status for project health and "
    "check_index_coverage for every cited path and for scopes behind negative or exhaustive "
    "claims. Coverage is best-effort, never proof of completeness. Check has_more or nextCursor "
    "and paginate when present.";

static const char MCP_ANALYSIS_SERVER_INSTRUCTIONS[] =
    "This is the analysis tool profile; graph and index mutation tools are unavailable. Use "
    "list_projects and index_status to select a current graph project, then use search_graph, "
    "trace_path, get_code_snippet, query_graph, get_architecture, and search_code for read-only "
    "analysis. Call check_index_coverage for every cited path and for scopes behind negative or "
    "exhaustive claims; read flagged ranges or skipped files directly. Coverage is best-effort, "
    "never proof of completeness. Check has_more or nextCursor and paginate when present. If the "
    "project is missing or stale, ask the parent agent to index or refresh it.";

static const char MCP_SCOUT_SERVER_INSTRUCTIONS[] =
    "This is the scout tool profile; only the fast positive-discovery graph tools are available. "
    "Use list_projects and index_status to select a current graph project, then use search_graph, "
    "trace_path, get_code_snippet, and get_architecture with narrow limits. Call "
    "check_index_coverage once for every cited path and read flagged ranges directly. Findings "
    "are provisional: do not make absence, exhaustive-impact, or dead-code claims. If the project "
    "is missing or stale, ask the parent agent to index or refresh it.";

static char *cbm_mcp_initialize_response_for_profile(const char *params_json,
                                                     cbm_mcp_tool_profile_t profile) {
    /* Determine protocol version: if client requests a version we support,
     * echo it back; otherwise respond with our latest. */
    const char *version = SUPPORTED_PROTOCOL_VERSIONS[0]; /* default: latest */
    if (params_json) {
        yyjson_doc *pdoc = yyjson_read(params_json, strlen(params_json), 0);
        if (pdoc) {
            yyjson_val *pv = yyjson_obj_get(yyjson_doc_get_root(pdoc), "protocolVersion");
            if (pv && yyjson_is_str(pv)) {
                const char *requested = yyjson_get_str(pv);
                for (int i = 0; i < SUPPORTED_VERSION_COUNT; i++) {
                    if (strcmp(requested, SUPPORTED_PROTOCOL_VERSIONS[i]) == 0) {
                        version = SUPPORTED_PROTOCOL_VERSIONS[i];
                        break;
                    }
                }
            }
            yyjson_doc_free(pdoc);
        }
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "protocolVersion", version);

    yyjson_mut_val *impl = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_str(doc, impl, "name", "codebase-memory-mcp");
    yyjson_mut_obj_add_str(doc, impl, "version", cbm_cli_get_version());
    yyjson_mut_obj_add_val(doc, root, "serverInfo", impl);

    yyjson_mut_val *caps = yyjson_mut_obj(doc);
    yyjson_mut_val *tools_cap = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_bool(doc, tools_cap, "listChanged", false);
    yyjson_mut_obj_add_val(doc, caps, "tools", tools_cap);
    yyjson_mut_val *prompts_cap = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_bool(doc, prompts_cap, "listChanged", false);
    yyjson_mut_obj_add_val(doc, caps, "prompts", prompts_cap);
    yyjson_mut_obj_add_val(doc, root, "capabilities", caps);
    const char *instructions = MCP_SERVER_INSTRUCTIONS;
    if (profile == CBM_MCP_TOOL_PROFILE_ANALYSIS) {
        instructions = MCP_ANALYSIS_SERVER_INSTRUCTIONS;
    } else if (profile == CBM_MCP_TOOL_PROFILE_SCOUT) {
        instructions = MCP_SCOUT_SERVER_INSTRUCTIONS;
    }
    yyjson_mut_obj_add_str(doc, root, "instructions", instructions);

    char *out = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return out;
}

char *cbm_mcp_initialize_response(const char *params_json) {
    return cbm_mcp_initialize_response_for_profile(params_json, CBM_MCP_TOOL_PROFILE_ALL);
}

/* ══════════════════════════════════════════════════════════════════
 *  ARGUMENT EXTRACTION
 * ══════════════════════════════════════════════════════════════════ */

char *cbm_mcp_get_tool_name(const char *params_json) {
    yyjson_doc *doc = yyjson_read(params_json, strlen(params_json), 0);
    if (!doc) {
        return NULL;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *name = yyjson_obj_get(root, "name");
    char *result = NULL;
    if (name && yyjson_is_str(name)) {
        result = heap_strdup(yyjson_get_str(name));
    }
    yyjson_doc_free(doc);
    return result;
}

char *cbm_mcp_get_arguments(const char *params_json) {
    yyjson_doc *doc = yyjson_read(params_json, strlen(params_json), 0);
    if (!doc) {
        return NULL;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *args = yyjson_obj_get(root, "arguments");
    char *result = NULL;
    if (args) {
        result = yyjson_val_write(args, 0, NULL);
    }
    yyjson_doc_free(doc);
    return result ? result : heap_strdup("{}");
}

char *cbm_mcp_get_string_arg(const char *args_json, const char *key) {
    yyjson_doc *doc = yyjson_read(args_json, strlen(args_json), 0);
    if (!doc) {
        return NULL;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *val = yyjson_obj_get(root, key);
    char *result = NULL;
    if (val && yyjson_is_str(val)) {
        result = heap_strdup(yyjson_get_str(val));
    }
    yyjson_doc_free(doc);
    return result;
}

static char *canonicalize_repo_path_if_exists(char *repo_path) {
    if (!repo_path) {
        return NULL;
    }
    bool root_syntax = true;
    for (const char *p = repo_path; *p; p++) {
        if (*p != '/' && *p != '\\' && *p != ':') {
            root_syntax = false;
            break;
        }
    }
    if (root_syntax) {
        return repo_path;
    }

    char real[CBM_SZ_4K];
    /* Wide-path canonicalization: the old _access/_fullpath pair decoded the
     * UTF-8 repo_path through the ANSI codepage and corrupted CJK paths on
     * CJK-locale systems (#973). */
    if (cbm_canonical_path(repo_path, real, sizeof(real))) {
        cbm_normalize_path_sep(real);
        char *canonical = heap_strdup(real);
        if (canonical) {
            free(repo_path);
            return canonical;
        }
    }

    return repo_path;
}

static bool repo_path_is_absolute(const char *path) {
    if (!path || path[0] == '\0') {
        return false;
    }
#ifdef _WIN32
    /* Path separators are normalized before this helper is called. A drive-
     * relative path such as "C:repo" is deliberately not considered absolute. */
    return (path[0] == '/' && path[1] == '/') ||
           (isalpha((unsigned char)path[0]) && path[1] == ':' && path[2] == '/');
#else
    return path[0] == '/';
#endif
}

static char *normalize_project_arg(char *project) {
    if (!project || (!strchr(project, '/') && !strchr(project, '\\'))) {
        return project;
    }

    project = canonicalize_repo_path_if_exists(project);
    char *normalized = cbm_project_name_from_path(project);
    if (normalized) {
        free(project);
        return normalized;
    }
    return project;
}

/* Forward decls — defined below alongside store resolution. */
static const char *cache_dir(char *buf, size_t bufsz);
static bool is_project_db_file(const char *name, size_t len);
bool cbm_validate_project_name(const char *project);

/* #1025: agents naturally pass the repo FOLDER name ("codebase-memory-mcp"),
 * but indexed project names derive from the full path
 * (E:\project\graph\x -> "E-project-graph-x"), so the exact lookup fails
 * while list_projects clearly shows the project. When no <project>.db exists,
 * scan cache-dir FILENAMES for a segment-aligned tail match ("-<project>.db"):
 * exactly one match adopts the full name; zero or several keep the original so
 * the existing not-found error (which lists all candidates) fires. Filename-
 * level only — internal-name drift stays #704's fallback in resolve_store. */
static char *resolve_project_tail(char *project) {
    if (!project || !cbm_validate_project_name(project)) {
        return project;
    }
    char dir[CBM_SZ_1K];
    cache_dir(dir, sizeof(dir));
    char exact[CBM_SZ_2K];
    snprintf(exact, sizeof(exact), "%s/%s.db", dir, project);
    if (cbm_file_exists(exact)) {
        return project; /* exact name — untouched fast path */
    }
    size_t plen = strlen(project);
    char match[CBM_SZ_1K] = "";
    int matches = 0;
    cbm_dir_t *d = cbm_opendir(dir);
    if (!d) {
        return project;
    }
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(d)) != NULL) {
        const char *n = entry->name;
        size_t len = strlen(n);
        if (!is_project_db_file(n, len)) {
            continue;
        }
        size_t stem_len = len - MCP_DB_EXT; /* strip ".db" */
        if (stem_len <= plen + 1 || stem_len >= sizeof(match)) {
            continue;
        }
        if (n[stem_len - plen - 1] != '-' || strncmp(n + stem_len - plen, project, plen) != 0) {
            continue;
        }
        matches++;
        if (matches > 1) {
            break; /* ambiguous — keep the original name */
        }
        memcpy(match, n, stem_len);
        match[stem_len] = '\0';
    }
    cbm_closedir(d);
    if (matches == 1) {
        cbm_log_info("mcp.project_tail_resolved", "passed", project, "resolved", match);
        free(project);
        return heap_strdup(match);
    }
    return project;
}

/* Resolve the project argument, accepting the canonical "project" key plus the
 * aliases a caller naturally reaches for (#640): list_projects surfaces the
 * field as "name" and the not-found hint says "pass the project name", so
 * "project_name" is the usual guess; "project_id" / "projectName" are accepted
 * too. NOT bare "name" — index_repository uses "name" for an explicit
 * project-name override. Caller must free() the result. */
static char *get_project_arg(const char *args_json) {
    char *p = cbm_mcp_get_string_arg(args_json, "project");
    if (!p) {
        p = cbm_mcp_get_string_arg(args_json, "project_name");
    }
    if (!p) {
        p = cbm_mcp_get_string_arg(args_json, "project_id");
    }
    if (!p) {
        p = cbm_mcp_get_string_arg(args_json, "projectName");
    }
    return resolve_project_tail(normalize_project_arg(p));
}

int cbm_mcp_get_int_arg(const char *args_json, const char *key, int default_val) {
    yyjson_doc *doc = yyjson_read(args_json, strlen(args_json), 0);
    if (!doc) {
        return default_val;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *val = yyjson_obj_get(root, key);
    int result = default_val;
    if (val && yyjson_is_int(val)) {
        result = yyjson_get_int(val);
    }
    yyjson_doc_free(doc);
    return result;
}

bool cbm_mcp_get_bool_arg(const char *args_json, const char *key) {
    yyjson_doc *doc = yyjson_read(args_json, strlen(args_json), 0);
    if (!doc) {
        return false;
    }
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *val = yyjson_obj_get(root, key);
    bool result = false;
    if (val && yyjson_is_bool(val)) {
        result = yyjson_get_bool(val);
    }
    yyjson_doc_free(doc);
    return result;
}

/* ══════════════════════════════════════════════════════════════════
 *  MCP SERVER
 * ══════════════════════════════════════════════════════════════════ */

struct cbm_mcp_server {
    cbm_store_t *store;     /* currently open project store (or NULL) */
    bool owns_store;        /* true if we opened the store */
    char *current_project;  /* which project store is open for (heap) */
    time_t store_last_used; /* last time resolve_store was called for a named project */

    /* Session + auto-index state */
    char session_root[CBM_SZ_1K];     /* detected project root path */
    char session_project[CBM_SZ_256]; /* derived project name */
    bool session_detected;            /* true after first detection attempt */
    char *allowed_root;               /* explicit per-session boundary (heap, nullable) */
    bool allowed_root_policy_set;     /* true even when explicit policy is unrestricted */
    bool background_tasks;            /* per-server update/auto-index work enabled */
    struct cbm_watcher *watcher;      /* external watcher ref (not owned) */
    struct cbm_config *config;        /* external config ref (not owned) */
    cbm_mcp_index_executor_fn index_executor;
    void *index_executor_context;
    cbm_proc_log_cb index_log_callback;
    void *index_log_context;
    cbm_mcp_project_mutation_begin_fn mutation_begin;
    cbm_mcp_project_mutation_try_begin_fn mutation_try_begin;
    cbm_mcp_project_mutation_end_fn mutation_end;
    void *mutation_context;
    cbm_mcp_quarantine_test_hook_fn quarantine_test_hook;
    void *quarantine_test_context;
    cbm_mcp_command_test_hook_fn command_test_hook;
    void *command_test_context;
#ifdef CBM_ENABLE_TEST_SEAMS
    cbm_mcp_auto_index_count_test_hook_fn auto_index_count_test_hook;
    void *auto_index_count_test_context;
#endif
    size_t search_output_limit_override;
    const char *search_scan_command_override;
    uint64_t search_scan_timeout_override_ms;
    bool search_scan_timeout_override_set;
    cbm_thread_t autoindex_tid;
    bool autoindex_active; /* true if auto-index thread was started */

    /* Request-scoped cancellation. The flag is shared by every cancellable
     * operation reached during one tool dispatch; active_pipeline remains a
     * diagnostic pointer for index_repository only. */
    cbm_mutex_t request_scope_mutex;
    unsigned int request_scope_depth;
    atomic_int pipeline_cancel_requested;
    cbm_pipeline_t *active_pipeline; /* non-NULL while index_repository runs */
    int64_t active_request_id;       /* JSON-RPC id of the in-progress tool call */
    char *active_request_id_str;     /* string JSON-RPC id of the in-progress tool call */
    cbm_mcp_tool_profile_t tool_profile;
};

static cbm_mcp_server_t *mcp_server_alloc_base(void) {
    cbm_mcp_server_t *srv = calloc(CBM_ALLOC_ONE, sizeof(*srv));
    if (!srv) {
        return NULL;
    }
    cbm_mutex_init(&srv->request_scope_mutex);
    atomic_init(&srv->pipeline_cancel_requested, 0);
    srv->owns_store = true;
    srv->tool_profile = CBM_MCP_TOOL_PROFILE_ALL;
    srv->background_tasks = true;
    return srv;
}

cbm_mcp_server_t *cbm_mcp_server_new(const char *store_path) {
    cbm_mcp_server_t *srv = mcp_server_alloc_base();
    if (!srv) {
        return NULL;
    }

    /* If a store_path is given, open that project directly.
     * Otherwise, create an in-memory store for test/embedded use. */
    if (store_path) {
        srv->store = cbm_store_open(store_path);
        srv->current_project = heap_strdup(store_path);
    } else {
        srv->store = cbm_store_open_memory();
    }
    return srv;
}

cbm_mcp_server_t *cbm_mcp_server_new_deferred_store(void) {
    return mcp_server_alloc_base();
}

void cbm_mcp_server_set_tool_profile(cbm_mcp_server_t *srv, cbm_mcp_tool_profile_t profile) {
    if (srv) {
        srv->tool_profile = profile;
    }
}

cbm_store_t *cbm_mcp_server_store(cbm_mcp_server_t *srv) {
    return srv ? srv->store : NULL;
}

void cbm_mcp_server_set_project(cbm_mcp_server_t *srv, const char *project) {
    if (!srv) {
        return;
    }
    free(srv->current_project);
    srv->current_project = project ? heap_strdup(project) : NULL;
}

void cbm_mcp_server_set_watcher(cbm_mcp_server_t *srv, struct cbm_watcher *w) {
    if (srv) {
        srv->watcher = w;
    }
}

void cbm_mcp_server_set_config(cbm_mcp_server_t *srv, struct cbm_config *cfg) {
    if (srv) {
        srv->config = cfg;
    }
}

#ifdef CBM_ENABLE_TEST_SEAMS
void cbm_mcp_server_set_auto_index_count_test_hook(cbm_mcp_server_t *srv,
                                                   cbm_mcp_auto_index_count_test_hook_fn hook,
                                                   void *context) {
    if (srv) {
        srv->auto_index_count_test_hook = hook;
        srv->auto_index_count_test_context = context;
    }
}
#endif

bool cbm_mcp_server_set_session_context(cbm_mcp_server_t *srv, const char *session_root,
                                        const char *allowed_root) {
    if (!srv || !session_root || session_root[0] == '\0' ||
        strlen(session_root) >= sizeof(srv->session_root)) {
        return false;
    }

    char *project = cbm_project_name_from_path(session_root);
    if (!project || project[0] == '\0' || strlen(project) >= sizeof(srv->session_project)) {
        free(project);
        return false;
    }

    char *allowed_copy = allowed_root ? heap_strdup(allowed_root) : NULL;
    if (allowed_root && !allowed_copy) {
        free(project);
        return false;
    }

    snprintf(srv->session_root, sizeof(srv->session_root), "%s", session_root);
    snprintf(srv->session_project, sizeof(srv->session_project), "%s", project);
    free(project);

    free(srv->allowed_root);
    srv->allowed_root = allowed_copy;
    srv->allowed_root_policy_set = true;
    srv->session_detected = true;
    return true;
}

const char *cbm_mcp_server_session_root(const cbm_mcp_server_t *srv) {
    return srv ? srv->session_root : NULL;
}

const char *cbm_mcp_server_session_project(const cbm_mcp_server_t *srv) {
    return srv ? srv->session_project : NULL;
}

const char *cbm_mcp_server_allowed_root(const cbm_mcp_server_t *srv) {
    return srv ? srv->allowed_root : NULL;
}

void cbm_mcp_server_set_background_tasks(cbm_mcp_server_t *srv, bool enabled) {
    if (srv) {
        srv->background_tasks = enabled;
    }
}

void cbm_mcp_server_set_index_executor(cbm_mcp_server_t *srv, cbm_mcp_index_executor_fn executor,
                                       void *context) {
    if (srv) {
        srv->index_executor = executor;
        srv->index_executor_context = context;
    }
}

void cbm_mcp_server_set_index_log_callback(cbm_mcp_server_t *srv, cbm_proc_log_cb callback,
                                           void *context) {
    if (srv) {
        srv->index_log_callback = callback;
        srv->index_log_context = callback ? context : NULL;
    }
}

void cbm_mcp_server_set_project_mutation_guard(cbm_mcp_server_t *srv,
                                               cbm_mcp_project_mutation_begin_fn begin,
                                               cbm_mcp_project_mutation_end_fn end, void *context) {
    if (!srv) {
        return;
    }
    /* A half-configured guard could acquire without releasing (or mutate
     * without acquiring), so accept only complete callback pairs. */
    if ((begin == NULL) != (end == NULL)) {
        return;
    }
    srv->mutation_begin = begin;
    srv->mutation_try_begin = NULL;
    srv->mutation_end = end;
    srv->mutation_context = begin ? context : NULL;
}

void cbm_mcp_server_set_project_mutation_try_guard(
    cbm_mcp_server_t *srv, cbm_mcp_project_mutation_try_begin_fn try_begin) {
    if (srv && srv->mutation_begin) {
        srv->mutation_try_begin = try_begin;
    }
}

static bool mcp_project_mutation_begin(cbm_mcp_server_t *srv, const char *project) {
    return !srv->mutation_begin || srv->mutation_begin(srv->mutation_context, project);
}

static bool mcp_project_mutation_try_begin(cbm_mcp_server_t *srv, const char *project) {
    return !srv->mutation_begin ||
           (srv->mutation_try_begin && srv->mutation_try_begin(srv->mutation_context, project));
}

static void mcp_project_mutation_end(cbm_mcp_server_t *srv, const char *project) {
    if (srv->mutation_end) {
        srv->mutation_end(srv->mutation_context, project);
    }
}

void cbm_mcp_server_free(cbm_mcp_server_t *srv) {
    if (!srv) {
        return;
    }
    if (srv->autoindex_active) {
        cbm_thread_join(&srv->autoindex_tid);
    }
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
    }
    free(srv->current_project);
    free(srv->allowed_root);
    free(srv->active_request_id_str);
    cbm_mutex_destroy(&srv->request_scope_mutex);
    free(srv);
}

/* ── Idle store eviction ──────────────────────────────────────── */

void cbm_mcp_server_evict_idle(cbm_mcp_server_t *srv, int timeout_s) {
    if (!srv || !srv->store) {
        return;
    }
    /* Protect initial in-memory stores that were never accessed via a named project.
     * store_last_used stays 0 until resolve_store is called with a non-NULL project. */
    if (srv->store_last_used == 0) {
        return;
    }

    time_t now = time(NULL);
    if ((now - srv->store_last_used) < timeout_s) {
        return;
    }

    if (srv->owns_store) {
        cbm_store_close(srv->store);
    }
    srv->store = NULL;
    free(srv->current_project);
    srv->current_project = NULL;
    srv->store_last_used = 0;
}

bool cbm_mcp_server_has_cached_store(cbm_mcp_server_t *srv) {
    return (srv && srv->store != NULL) != 0;
}

bool cbm_mcp_server_release_pristine_memory_store(cbm_mcp_server_t *srv) {
    const char *db_path = srv && srv->store ? cbm_store_db_path(srv->store) : NULL;
    if (!srv || !srv->owns_store || !srv->store || srv->current_project ||
        srv->store_last_used != 0 || db_path != NULL) {
        return false;
    }
    cbm_store_close(srv->store);
    srv->store = NULL;
    return true;
}

cbm_pipeline_t *cbm_mcp_server_active_pipeline(cbm_mcp_server_t *srv) {
    return srv ? srv->active_pipeline : NULL;
}

bool cbm_mcp_server_cancel_active(cbm_mcp_server_t *srv) {
    if (!srv) {
        return false;
    }
    cbm_mutex_lock(&srv->request_scope_mutex);
    bool active = srv->request_scope_depth != 0;
    if (active) {
        atomic_store_explicit(&srv->pipeline_cancel_requested, 1, memory_order_release);
    }
    cbm_mutex_unlock(&srv->request_scope_mutex);
    return active;
}

bool cbm_mcp_server_request_scope_begin(cbm_mcp_server_t *srv) {
    if (!srv) {
        return false;
    }
    cbm_mutex_lock(&srv->request_scope_mutex);
    bool available = srv->request_scope_depth < UINT_MAX;
    if (available) {
        if (srv->request_scope_depth == 0) {
            atomic_store_explicit(&srv->pipeline_cancel_requested, 0, memory_order_release);
        }
        srv->request_scope_depth++;
    }
    cbm_mutex_unlock(&srv->request_scope_mutex);
    return available;
}

void cbm_mcp_server_request_scope_end(cbm_mcp_server_t *srv) {
    if (!srv) {
        return;
    }
    cbm_mutex_lock(&srv->request_scope_mutex);
    if (srv->request_scope_depth > 0) {
        srv->request_scope_depth--;
        if (srv->request_scope_depth == 0) {
            atomic_store_explicit(&srv->pipeline_cancel_requested, 0, memory_order_release);
        }
    }
    cbm_mutex_unlock(&srv->request_scope_mutex);
}

static bool mcp_request_cancelled(const cbm_mcp_server_t *srv) {
    return srv && atomic_load_explicit(&srv->pipeline_cancel_requested, memory_order_acquire) != 0;
}

void cbm_mcp_server_set_quarantine_test_hook(cbm_mcp_server_t *srv,
                                             cbm_mcp_quarantine_test_hook_fn hook, void *context) {
    if (!srv) {
        return;
    }
    srv->quarantine_test_hook = hook;
    srv->quarantine_test_context = context;
}

void cbm_mcp_server_set_command_test_hook(cbm_mcp_server_t *srv, cbm_mcp_command_test_hook_fn hook,
                                          void *context) {
    if (!srv) {
        return;
    }
    srv->command_test_hook = hook;
    srv->command_test_context = context;
}

void cbm_mcp_server_set_search_output_limit_for_test(cbm_mcp_server_t *srv, size_t limit) {
    if (srv) {
        srv->search_output_limit_override = limit;
    }
}

void cbm_mcp_server_set_search_scan_command_for_test(cbm_mcp_server_t *srv, const char *command) {
    if (srv) {
        srv->search_scan_command_override = command;
    }
}

void cbm_mcp_server_set_search_scan_timeout_for_test(cbm_mcp_server_t *srv, uint64_t timeout_ms,
                                                     bool override_set) {
    if (srv) {
        srv->search_scan_timeout_override_ms = timeout_ms;
        srv->search_scan_timeout_override_set = override_set;
    }
}

/* ── Cache dir + project DB path helpers ───────────────────────── */

/* Returns the cache directory. Writes to buf, returns buf for convenience. */
static const char *cache_dir(char *buf, size_t bufsz) {
    const char *dir = cbm_resolve_cache_dir();
    if (!dir) {
        dir = cbm_tmpdir();
    }
    snprintf(buf, bufsz, "%s", dir);
    return buf;
}

/* Returns full .db path for a project: <cache_dir>/<project>.db */
static const char *project_db_path(const char *project, char *buf, size_t bufsz) {
    if (!cbm_validate_project_name(project)) {
        buf[0] = '\0';
        return buf;
    }
    char dir[CBM_SZ_1K];
    cache_dir(dir, sizeof(dir));
    snprintf(buf, bufsz, "%s/%s.db", dir, project);
    return buf;
}

/* ── Store resolution ──────────────────────────────────────────── */

/* Read the sole INTERNAL project name from a .db file at full_path.
 * Opens the file query-mode (no create) and succeeds ONLY when the db holds
 * exactly one project row with a non-empty name — this filters ghost/empty
 * /corrupt dbs (0-byte file, missing `projects` table, or >1 row). On success
 * the internal name is copied into name_out; if out_store is non-NULL the open
 * handle is transferred to the caller (who must cbm_store_close it). On failure
 * the store is always closed. Defined after is_project_db_file below. */
static bool db_internal_project_name(const char *full_path, char *name_out, size_t name_sz,
                                     cbm_store_t **out_store);

/* #704 fallback: scan the cache dir for the db whose sole internal project name
 * equals `project`, returning an open store handle (caller owns it) or NULL.
 * Used only when <project>.db is absent or its internal name differs from the
 * passed name (drifted filename). Defined after is_project_db_file below. */
static cbm_store_t *resolve_store_fallback_scan(const char *project);

static bool reserve_unique_corrupt_pending(const char *path, char *pending, size_t pending_size,
                                           char *backup, size_t backup_size) {
    static atomic_uint_fast64_t sequence = 0;
    for (unsigned int attempt = 0; attempt < 128; attempt++) {
        uint64_t token = cbm_now_ns() ^ ((uint64_t)(unsigned int)getpid() << 32) ^
                         atomic_fetch_add_explicit(&sequence, 1, memory_order_relaxed);
        int backup_written =
            snprintf(backup, backup_size, "%s.corrupt.%016llx", path, (unsigned long long)token);
        int pending_written = snprintf(pending, pending_size, "%s.corrupt.pending.%016llx", path,
                                       (unsigned long long)token);
        if (backup_written <= 0 || (size_t)backup_written >= backup_size || pending_written <= 0 ||
            (size_t)pending_written >= pending_size) {
            return false;
        }
        if (cbm_file_exists(backup)) {
            continue;
        }
#ifdef _WIN32
        wchar_t *wide = cbm_path_to_wide(pending);
        HANDLE file = wide ? CreateFileW(wide, GENERIC_READ | GENERIC_WRITE, 0, NULL, CREATE_NEW,
                                         FILE_ATTRIBUTE_NORMAL, NULL)
                           : INVALID_HANDLE_VALUE;
        DWORD create_error = file == INVALID_HANDLE_VALUE ? GetLastError() : ERROR_SUCCESS;
        free(wide);
        if (file != INVALID_HANDLE_VALUE) {
            CloseHandle(file);
            return true;
        }
        if (create_error != ERROR_FILE_EXISTS && create_error != ERROR_ALREADY_EXISTS) {
            return false;
        }
#else
        int fd = open(pending, O_WRONLY | O_CREAT | O_EXCL, 0600);
        if (fd >= 0) {
            (void)close(fd);
            return true;
        }
        if (errno != EEXIST) {
            return false;
        }
#endif
    }
    return false;
}

static void discard_corrupt_pending(const char *pending) {
    if (!pending) {
        return;
    }
    (void)cbm_remove_db_sidecars(pending);
    (void)cbm_unlink(pending);
}

#ifndef _WIN32
static bool sync_parent_directory(const char *path) {
    char directory[CBM_SZ_2K];
    int written = snprintf(directory, sizeof(directory), "%s", path ? path : "");
    if (written <= 0 || (size_t)written >= sizeof(directory)) {
        return false;
    }
    char *slash = strrchr(directory, '/');
    if (!slash) {
        snprintf(directory, sizeof(directory), ".");
    } else if (slash == directory) {
        slash[1] = '\0';
    } else {
        *slash = '\0';
    }
    int fd = open(directory, O_RDONLY | O_DIRECTORY);
    if (fd < 0) {
        return false;
    }
    int rc;
    do {
        rc = fsync(fd);
    } while (rc != 0 && errno == EINTR);
    (void)close(fd);
    return rc == 0;
}
#endif

/* Publish only a fully closed SQLite snapshot, without ever replacing a prior
 * recovery file. POSIX link() and Windows MoveFileExW without REPLACE are
 * atomic no-clobber operations within the cache directory. */
static bool publish_corrupt_backup(const char *pending, const char *backup) {
#ifdef _WIN32
    wchar_t *wide_pending = cbm_path_to_wide(pending);
    wchar_t *wide_backup = cbm_path_to_wide(backup);
    bool published = wide_pending && wide_backup &&
                     MoveFileExW(wide_pending, wide_backup, MOVEFILE_WRITE_THROUGH) != 0;
    free(wide_pending);
    free(wide_backup);
    return published;
#else
    if (link(pending, backup) != 0) {
        return false;
    }
    if (!sync_parent_directory(backup)) {
        (void)cbm_unlink(backup);
        return false;
    }
    /* A crash before this cleanup merely leaves a second link to the same
     * complete snapshot; the published recovery generation is already safe. */
    (void)cbm_unlink(pending);
    (void)sync_parent_directory(backup);
    return true;
#endif
}

static bool quarantine_step_allowed(cbm_mcp_server_t *srv, const char *step) {
    return !srv || !srv->quarantine_test_hook ||
           srv->quarantine_test_hook(srv->quarantine_test_context, step);
}

/* Create one transactionally consistent, self-contained recovery snapshot
 * (SQLite backup incorporates committed WAL frames), publish it atomically,
 * and only then remove the corrupt live generation. A crash can therefore
 * leave the live DB, the completed backup, or both, but never destroys the
 * only recoverable generation. */
static bool quarantine_corrupt_store(cbm_mcp_server_t *srv, const char *project, const char *path,
                                     char *backup_out, size_t backup_out_size) {
    char backup[CBM_SZ_2K];
    char pending[CBM_SZ_2K];
    /* #1425 belt-and-braces: an empty store path would render the backup as a
     * bare relative ".corrupt.<hex>" in the process cwd. There is nothing at
     * such a path worth quarantining. */
    if (!path || !path[0]) {
        cbm_log_error("store.auto_clean_failed", "project", project, "path", "", "reason",
                      "empty store path");
        return false;
    }
    if (!reserve_unique_corrupt_pending(path, pending, sizeof(pending), backup, sizeof(backup))) {
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "cannot reserve unique backup");
        return false;
    }

    if (cbm_store_backup_path(path, pending) != CBM_STORE_OK ||
        cbm_store_prepare_path_for_replace(pending) != CBM_STORE_OK) {
        discard_corrupt_pending(pending);
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "cannot create self-contained recovery snapshot");
        return false;
    }

    cbm_store_t *snapshot = cbm_store_open_path_query(pending);
    if (!snapshot) {
        discard_corrupt_pending(pending);
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "recovery snapshot cannot be reopened");
        return false;
    }
    cbm_store_close(snapshot);

    if (!quarantine_step_allowed(srv, "before_snapshot_publish") ||
        !publish_corrupt_backup(pending, backup)) {
        discard_corrupt_pending(pending);
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "cannot atomically publish recovery snapshot");
        return false;
    }
    discard_corrupt_pending(pending);

    if (!quarantine_step_allowed(srv, "after_snapshot_publish")) {
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "backup complete; live generation retained", "backup", backup);
        return false;
    }

    if (cbm_unlink(path) != 0 && errno != ENOENT) {
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "backup complete; live database removal failed", "backup", backup);
        return false;
    }
    if (cbm_remove_db_sidecars(path) != 0) {
        cbm_log_error("store.auto_clean_sidecars", "project", project, "path", path, "reason",
                      "backup complete; stale sidecar cleanup deferred");
    }

    if (backup_out && backup_out_size > 0) {
        snprintf(backup_out, backup_out_size, "%s", backup);
    }
    return true;
}

/* Open the right project's .db file for query tools.
 * Caches the connection — reopens only when project changes.
 * Tracks last-access time so the event loop can evict idle stores. */
typedef enum {
    STORE_RECOVERY_NONE,
    STORE_RECOVERY_BUSY,
    STORE_RECOVERY_TRY_GUARD_UNAVAILABLE,
} store_recovery_status_t;

static cbm_store_t *resolve_store_internal(cbm_mcp_server_t *srv, const char *project,
                                           bool mutation_already_held, bool nonblocking_recovery,
                                           store_recovery_status_t *recovery_status) {
    if (recovery_status) {
        *recovery_status = STORE_RECOVERY_NONE;
    }
    if (!project) {
        return NULL; /* project is required — no implicit fallback */
    }

    srv->store_last_used = time(NULL);

    /* Already open for this project? */
    if (srv->current_project && strcmp(srv->current_project, project) == 0 && srv->store) {
        return srv->store;
    }

    /* Close old store */
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
        srv->store = NULL;
    }

    /* Open project's .db file — query-only open (no SQLITE_OPEN_CREATE) to
     * prevent ghost .db file creation for unknown/unindexed projects.
     * #1425: an invalid project name yields an empty path. SQLite opens ""
     * as an anonymous temp db, which then fails the integrity check and
     * quarantines a db that never existed — as a RELATIVE .corrupt.<hex>
     * file in the daemon's cwd. Skip the direct open entirely; the fallback
     * scan below still resolves legacy dbs whose internal name predates
     * validation. */
    char path[CBM_SZ_1K];
    project_db_path(project, path, sizeof(path));
    srv->store = path[0] ? cbm_store_open_path_query(path) : NULL;
    if (srv->store) {
        /* Check DB integrity — back up (never silently delete) a corrupt DB */
        if (!cbm_store_check_integrity(srv->store)) {
            cbm_store_close(srv->store);
            srv->store = NULL;
            bool mutation_acquired = mutation_already_held;
            if (!mutation_acquired) {
                mutation_acquired = nonblocking_recovery
                                        ? mcp_project_mutation_try_begin(srv, project)
                                        : mcp_project_mutation_begin(srv, project);
            }
            if (!mutation_acquired) {
                if (nonblocking_recovery && recovery_status) {
                    *recovery_status = srv->mutation_try_begin
                                           ? STORE_RECOVERY_BUSY
                                           : STORE_RECOVERY_TRY_GUARD_UNAVAILABLE;
                }
                return NULL;
            }

            /* The lease may have waited behind a publisher. Re-open and trust
             * only the current generation, never the stale pre-wait verdict.
             * Use the verdict API here — this is the point that decides whether
             * a healthy DB gets quarantined. The plain bool check cannot tell
             * corruption from a transient SQLITE_BUSY race (#1206: concurrent
             * instances quarantining each other's DBs) and does not run
             * quick_check, so page-torn DBs with an intact projects table sail
             * through (#1037). Only a confirmed CORRUPT verdict is quarantined;
             * TRANSIENT (lock/IO) falls through and retries on next access. */
            srv->store = cbm_store_open_path_query(path);
            cbm_integrity_verdict_t verdict = srv->store
                                                  ? cbm_store_check_integrity_verdict(srv->store)
                                                  : CBM_INTEGRITY_TRANSIENT;
            bool current_valid = (verdict == CBM_INTEGRITY_OK);
            if (verdict == CBM_INTEGRITY_TRANSIENT) {
                /* The DB could not be conclusively evaluated (lock contention,
                 * busy writer, IO hiccup). Do NOT quarantine — close and let
                 * the next resolve retry. A spurious quarantine here is exactly
                 * what destroys healthy DBs under concurrent access. */
                cbm_store_close(srv->store);
                srv->store = NULL;
                if (recovery_status) {
                    *recovery_status = STORE_RECOVERY_BUSY;
                }
                if (!mutation_already_held) {
                    mcp_project_mutation_end(srv, project);
                }
                return NULL;
            }
            if (!current_valid) {
                cbm_store_close(srv->store);
                srv->store = NULL;
                char backup[CBM_SZ_2K] = {0};
                bool quarantined =
                    quarantine_corrupt_store(srv, project, path, backup, sizeof(backup));
                cbm_log_error("store.auto_clean", "project", project, "path", path, "action",
                              quarantined ? "corrupt generation quarantined"
                                          : "corrupt generation preserved",
                              "backup", quarantined ? backup : "none");
            }
            if (!mutation_already_held) {
                mcp_project_mutation_end(srv, project);
            }
            if (!srv->store) {
                return NULL;
            }
        }

        /* Verify the project actually exists in this database.
         * A .db file may exist but be empty (e.g., after delete_project on
         * Linux where unlink defers actual removal). Opening an empty/deleted
         * store without closing it leaks the SQLite connection. */
        cbm_project_t proj_verify = {0};
        if (cbm_store_get_project(srv->store, project, &proj_verify) == CBM_STORE_OK) {
            cbm_project_free_fields(&proj_verify);
            srv->owns_store = true;
            free(srv->current_project);
            srv->current_project = heap_strdup(project);
            return srv->store; /* fast path: filename == internal name */
        }
        /* #704: <project>.db exists but its INTERNAL project name differs from
         * the passed name (a copied/renamed db, or a legacy '.'-vs-'-' username
         * twin). Close it and fall through to the cache-dir scan below. */
        cbm_store_close(srv->store);
        srv->store = NULL;
    }

    /* #704 fallback: either <project>.db is absent or its internal name drifted
     * from its filename. Node rows are keyed on the INTERNAL name (== the passed
     * name, since list_projects now advertises internal names), so scan the
     * cache dir for the db whose sole internal project name equals `project` and
     * adopt it. Runs ONLY on the fallback — the common fast path is unchanged.
     * No match → NULL (a genuine typo stays not-found). */
    cbm_store_t *scanned = resolve_store_fallback_scan(project);
    if (scanned) {
        srv->store = scanned;
        srv->owns_store = true;
        free(srv->current_project);
        srv->current_project = heap_strdup(project);
    }

    return srv->store;
}

static cbm_store_t *resolve_store(cbm_mcp_server_t *srv, const char *project) {
    return resolve_store_internal(srv, project, false, false, NULL);
}

/* Forward decl — definition lives below alongside list_projects. */
static bool is_project_db_file(const char *name, size_t len);

/* Forward decl — definition lives below in handle_trace_call_path's helpers. */
static void free_node_contents(cbm_node_t *n);

/* Scan cache dir for .db files, writing comma-separated quoted names into out.
 * Returns the number of projects found. */
static int collect_db_project_names(const char *dir_path, char *out, size_t out_sz) {
    int count = 0;
    int offset = 0;
    cbm_dir_t *d = cbm_opendir(dir_path);
    if (!d) {
        return 0;
    }
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(d)) != NULL) {
        const char *n = entry->name;
        size_t len = strlen(n);
        if (!is_project_db_file(n, len)) {
            continue;
        }
        /* #704: advertise the db's INTERNAL project name, not its filename, and
         * skip ghost/empty/corrupt dbs — so the hint lists names the user can
         * actually pass to resolve a store. */
        char full_path[CBM_SZ_2K];
        snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, n);
        char iname[CBM_SZ_1K];
        if (!db_internal_project_name(full_path, iname, sizeof(iname), NULL)) {
            continue;
        }
        /* Element-boundary write: only emit this name if the WHOLE element —
         * optional leading comma + "iname" — plus the NUL fits in what remains.
         * Never truncate mid-token; a partial name would corrupt the JSON array
         * (issue #235). Stop cleanly at the last name that fits: the array then
         * always holds complete names and `count` == its length. */
        size_t off = (size_t)offset;
        size_t need = strlen(iname) + 2 /* quotes */ + (count > 0 ? 1u : 0u) /* comma */;
        if (off + need + 1 > out_sz) {
            break; /* would not fit entirely — stop at this element boundary */
        }
        if (count > 0) {
            out[offset++] = ',';
        }
        int wrote = snprintf(out + offset, out_sz - (size_t)offset, "\"%s\"", iname);
        if (wrote > 0) {
            offset += wrote; /* guaranteed to fit (checked above) — no truncation */
        }
        count++;
    }
    cbm_closedir(d);
    return count;
}

static void add_git_context_string(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key,
                                   const char *value) {
    if (value) {
        yyjson_mut_obj_add_strcpy(doc, obj, key, value);
    } else {
        yyjson_mut_obj_add_null(doc, obj, key);
    }
}

static __attribute__((unused)) void add_git_context_json(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *root_path) {
    cbm_git_context_t ctx = {0};
    (void)cbm_git_context_resolve(root_path, &ctx);

    yyjson_mut_val *git = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_bool(doc, git, "is_git", ctx.is_git);
    yyjson_mut_obj_add_bool(doc, git, "is_worktree", ctx.is_worktree);
    yyjson_mut_obj_add_bool(doc, git, "is_detached", ctx.is_detached);
    yyjson_mut_obj_add_bool(doc, git, "root_exists", ctx.root_exists);
    add_git_context_string(doc, git, "worktree_root", ctx.worktree_root);
    add_git_context_string(doc, git, "git_dir", ctx.git_dir);
    add_git_context_string(doc, git, "git_common_dir", ctx.git_common_dir);
    add_git_context_string(doc, git, "canonical_root", ctx.canonical_root);
    add_git_context_string(doc, git, "branch", ctx.branch);
    add_git_context_string(doc, git, "branch_slug", ctx.branch_slug);
    add_git_context_string(doc, git, "head_sha", ctx.head_sha);
    add_git_context_string(doc, git, "base_sha", ctx.base_sha);
    yyjson_mut_obj_add_val(doc, obj, "git", git);

    cbm_git_context_free(&ctx);
}

/* Build a helpful error listing available projects. Caller must free() result. */
static char *build_project_list_error(const char *reason) {
    char dir_path[CBM_SZ_1K];
    cache_dir(dir_path, sizeof(dir_path));

    char projects[CBM_SZ_4K] = "";
    int count = collect_db_project_names(dir_path, projects, sizeof(projects));

    enum { ERR_BUF_SZ = 5120 };
    char buf[ERR_BUF_SZ];
    if (count > 0) {
        snprintf(buf, sizeof(buf),
                 "{\"error\":\"%s\",\"hint\":\"Use list_projects to see all indexed projects, "
                 "then pass it as the \\\"project\\\" "
                 "argument.\",\"available_projects\":[%s],\"count\":%d}",
                 reason, projects, count);
    } else {
        snprintf(buf, sizeof(buf),
                 "{\"error\":\"%s\",\"hint\":\"No projects indexed yet. "
                 "Call index_repository first.\"}",
                 reason);
    }
    return heap_strdup(buf);
}

/* Distinct from "unknown project": the caller omitted the project argument
 * entirely (no recognized key). Name the literal "project" key so the fix is
 * obvious (#640). Caller must free() result. */
static char *build_missing_project_error(void) {
    return heap_strdup("{\"error\":\"missing required argument: project\",\"hint\":\"Pass "
                       "the project as the \\\"project\\\" argument, e.g. "
                       "{\\\"project\\\":\\\"<name from list_projects>\\\"}. Run "
                       "list_projects to see indexed projects.\"}");
}

/* Pick the right no-store error: a NULL project means the argument was missing
 * (clearer message); a non-NULL project that didn't resolve means it's
 * unknown/unindexed (list the available ones). */
static char *build_no_store_error(const char *project) {
    return project ? build_project_list_error("project not found or not indexed")
                   : build_missing_project_error();
}

/* Bail with the right error when no store is available. */
#define REQUIRE_STORE(store, project)                     \
    do {                                                  \
        if (!(store)) {                                   \
            char *_err = build_no_store_error(project);   \
            char *_res = cbm_mcp_text_result(_err, true); \
            free(_err);                                   \
            free(project);                                \
            return _res;                                  \
        }                                                 \
    } while (0)

static bool project_has_adr(cbm_store_t *store, const char *project, const char *root_path) {
    if (store && project) {
        cbm_adr_t adr;
        memset(&adr, 0, sizeof(adr));
        if (cbm_store_adr_get(store, project, &adr) == CBM_STORE_OK) {
            cbm_store_adr_free(&adr);
            return true;
        }
    }

    if (!root_path) {
        return false;
    }

    char adr_path[CBM_SZ_4K];
    snprintf(adr_path, sizeof(adr_path), "%s/.codebase-memory/adr.md", root_path);
    struct stat adr_st;
    return stat(adr_path, &adr_st) == 0;
}

/* ── Tool handler implementations ─────────────────────────────── */

/* Return true if filename is a valid project .db file (not temp/internal).
 *
 * Project names derived from /tmp/... source roots legitimately begin with
 * "tmp-" (cbm_project_name_from_path: "/tmp/bench/..." → "tmp-bench-...";
 * see tests/test_pipeline.c fixtures), so the prefix must NOT be excluded.
 * The "_" prefix is reserved for internal/hidden DBs, and ":memory:" is the
 * SQLite in-memory marker (defensive — never appears as a real file). */
static bool is_project_db_file(const char *name, size_t len) {
    if (len < MCP_MIN_DB_NAME || strcmp(name + len - MCP_DB_EXT, ".db") != 0) {
        return false;
    }
    if (strncmp(name, "_", SLEN("_")) == 0 || strncmp(name, ":memory:", SLEN(":memory:")) == 0) {
        return false;
    }
    return true;
}

/* db_internal_project_name — see forward declaration above resolve_store. */
static bool db_internal_project_name(const char *full_path, char *name_out, size_t name_sz,
                                     cbm_store_t **out_store) {
    if (out_store) {
        *out_store = NULL;
    }
    cbm_store_t *st = cbm_store_open_path_query(full_path);
    if (!st) {
        return false; /* nonexistent / unreadable */
    }
    cbm_project_t *projs = NULL;
    int n = 0;
    bool ok = false;
    if (cbm_store_list_projects(st, &projs, &n) == CBM_STORE_OK) {
        /* Ignore internal shadow projects ("<name>::missed" miss-graph rows):
         * they share the db with the primary project and must not make it
         * unresolvable — requiring n == 1 over ALL rows made every project
         * with a miss graph vanish from list_projects and the UI (#1044). */
        int primary = -1;
        int primary_count = 0;
        for (int i = 0; i < n; i++) {
            if (projs[i].name && projs[i].name[0] && !strstr(projs[i].name, "::")) {
                primary = i;
                primary_count++;
            }
        }
        if (primary_count == 1) {
            snprintf(name_out, name_sz, "%s", projs[primary].name);
            ok = true;
        }
    }
    cbm_store_free_projects(projs, n);
    if (ok && out_store) {
        *out_store = st; /* transfer ownership to caller */
    } else {
        cbm_store_close(st);
    }
    return ok;
}

/* resolve_store_fallback_scan — see forward declaration above resolve_store. */
static cbm_store_t *resolve_store_fallback_scan(const char *project) {
    char dir_path[CBM_SZ_1K];
    cache_dir(dir_path, sizeof(dir_path));
    cbm_dir_t *d = cbm_opendir(dir_path);
    if (!d) {
        return NULL;
    }
    cbm_store_t *found = NULL;
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(d)) != NULL) {
        const char *n = entry->name;
        size_t len = strlen(n);
        if (!is_project_db_file(n, len)) {
            continue;
        }
        char full_path[CBM_SZ_2K];
        snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, n);
        char iname[CBM_SZ_1K];
        cbm_store_t *st = NULL;
        if (db_internal_project_name(full_path, iname, sizeof(iname), &st)) {
            if (strcmp(iname, project) == 0) {
                found = st; /* adopt — caller takes ownership */
                break;
            }
            cbm_store_close(st);
        }
    }
    cbm_closedir(d);
    return found;
}

/* Open a .db file briefly, collect node/edge counts and root_path,
 * then append a JSON entry to arr. */
static __attribute__((unused)) void build_project_json_entry(yyjson_mut_doc *doc, yyjson_mut_val *arr, const char *dir_path,
                                     const char *name, size_t name_len, int64_t size_bytes,
                                     bool include_details) {
    (void)name_len;

    char full_path[CBM_SZ_2K];
    snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, name);

    /* #704: key on the db's INTERNAL project name, not its filename. Node/edge
     * rows are tagged with the internal name, so a drifted filename (copied or
     * renamed db, legacy '.'-vs-'-' username twin) would otherwise report 0
     * nodes/edges and be unresolvable. Skip ghost/empty/corrupt dbs entirely so
     * they don't appear as resolvable projects. */
    char project_name[CBM_SZ_1K];
    cbm_store_t *pstore = NULL;
    if (!db_internal_project_name(full_path, project_name, sizeof(project_name), &pstore)) {
        return; /* ghost / unreadable — not a resolvable project */
    }

    int nodes = 0;
    int edges = 0;
    if (include_details) {
        nodes = cbm_store_count_nodes(pstore, project_name);
        edges = cbm_store_count_edges(pstore, project_name);
    }
    char root_path_buf[CBM_SZ_1K] = "";
    cbm_project_t proj = {0};
    if (cbm_store_get_project(pstore, project_name, &proj) == CBM_STORE_OK) {
        if (proj.root_path) {
            snprintf(root_path_buf, sizeof(root_path_buf), "%s", proj.root_path);
        }
        cbm_project_free_fields(&proj);
    }
    cbm_store_close(pstore);

    yyjson_mut_val *p = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_strcpy(doc, p, "name", project_name);
    yyjson_mut_obj_add_strcpy(doc, p, "root_path", root_path_buf);
    /* Listing stays lean: only the branch (the one git fact that
     * disambiguates same-repo projects). The 12-field git block — mostly
     * null for non-git roots — cost ~10KB across a full cache and is one
     * index_status call away for the project you actually care about. */
    if (include_details && root_path_buf[0]) {
        cbm_git_context_t gctx = {0};
        (void)cbm_git_context_resolve(root_path_buf, &gctx);
        if (gctx.is_git && gctx.branch) {
            yyjson_mut_obj_add_strcpy(doc, p, "branch", gctx.branch);
        }
        cbm_git_context_free(&gctx);
    }
    if (include_details) {
        yyjson_mut_obj_add_int(doc, p, "nodes", nodes);
        yyjson_mut_obj_add_int(doc, p, "edges", edges);
        yyjson_mut_obj_add_int(doc, p, "size_bytes", size_bytes);
    }
    yyjson_mut_arr_add_val(arr, p);
}

static __attribute__((unused)) int project_db_name_cmp(const void *a, const void *b) {
    const char *const *sa = (const char *const *)a;
    const char *const *sb = (const char *const *)b;
    return strcmp(*sa, *sb);
}

/* list_projects: scan cache directory for .db files.
 * Each project is a single .db file — no central registry needed. */
/* verify_project_indexed — returns a heap-allocated error JSON string when the
 * named project has not been indexed yet, or NULL when the project exists.
 * resolve_store uses cbm_store_open_path_query (no SQLITE_OPEN_CREATE), so
 * store is NULL for missing .db files (REQUIRE_STORE fires first). This
 * function catches the remaining case: a .db file exists but has no indexed
 * nodes (e.g., an empty or half-initialised project).
 * Callers that receive a non-NULL return value must free(project) themselves
 * before returning the error string. */
static __attribute__((unused)) char *verify_project_indexed(cbm_store_t *store, const char *project) {
    cbm_project_t proj_check = {0};
    if (cbm_store_get_project(store, project, &proj_check) != CBM_STORE_OK) {
        char *err = build_project_list_error("project not indexed — run index_repository first");
        char *res = cbm_mcp_text_result(err, true);
        free(err);
        return res;
    }
    cbm_project_free_fields(&proj_check);
    return NULL;
}

/* compare_graphs deliberately bypasses resolve_store(): it needs two
 * independently-owned request-scoped read handles, while resolve_store caches
 * one handle on the server. Direct-name lookup stays the fast path; the
 * existing internal-name fallback preserves legacy renamed databases. */
static cbm_store_t *compare_open_project_store(const char *project) {
    char path[CBM_SZ_1K];
    project_db_path(project, path, sizeof(path));
    cbm_store_t *store = path[0] ? cbm_store_open_path_query(path) : NULL;
    if (store) {
        cbm_project_t row = {0};
        if (cbm_store_get_project(store, project, &row) == CBM_STORE_OK) {
            cbm_project_free_fields(&row);
            return store;
        }
        cbm_store_close(store);
    }
    return resolve_store_fallback_scan(project);
}

typedef struct {
    yyjson_mut_val *items;
    size_t returned;
    size_t encoded_bytes;
    bool budget_exhausted;
} compare_result_set_t;

typedef struct {
    cbm_mcp_server_t *server;
    yyjson_mut_doc *doc;
    size_t limit;
    compare_result_set_t nodes_added;
    compare_result_set_t nodes_removed;
    compare_result_set_t edges_added;
    compare_result_set_t edges_removed;
} compare_response_t;

static char *compare_graphs_error(const char *code, const char *message) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) {
        yyjson_mut_doc_free(doc);
        return cbm_mcp_text_result("compare_graphs failed: out of memory", true);
    }
    yyjson_mut_doc_set_root(doc, root);
    if (!yyjson_mut_obj_add_strcpy(doc, root, "error", message) ||
        !yyjson_mut_obj_add_strcpy(doc, root, "code", code)) {
        yyjson_mut_doc_free(doc);
        return cbm_mcp_text_result("compare_graphs failed: out of memory", true);
    }
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    if (!json) {
        return cbm_mcp_text_result("compare_graphs failed: out of memory", true);
    }
    char *result = cbm_mcp_text_result(json, true);
    free(json);
    return result;
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
    yyjson_doc *doc = yyjson_read(args ? args : "{}", args ? strlen(args) : SLEN("{}"), 0);
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
    if (!base || !yyjson_is_str(base) || yyjson_get_len(base) == 0 || !target ||
        !yyjson_is_str(target) || yyjson_get_len(target) == 0) {
        *error_message = "base_project and target_project are required non-empty strings";
        yyjson_doc_free(doc);
        return false;
    }
    if (strcmp(yyjson_get_str(base), yyjson_get_str(target)) == 0) {
        *error_message = "base_project and target_project must be distinct";
        yyjson_doc_free(doc);
        return false;
    }
    if (!compare_parse_bounded_integer(root, "limit", MCP_COMPARE_DEFAULT_LIMIT,
                                       MCP_COMPARE_MAX_LIMIT, limit, error_message) ||
        !compare_parse_bounded_integer(root, "scan_limit", MCP_COMPARE_DEFAULT_SCAN_LIMIT,
                                       MCP_COMPARE_MAX_SCAN_LIMIT, scan_limit, error_message)) {
        yyjson_doc_free(doc);
        return false;
    }

    *base_project = heap_strdup(yyjson_get_str(base));
    *target_project = heap_strdup(yyjson_get_str(target));
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

static char *sanitize_utf8_lossy(const char *s);

static bool compare_add_identity_string(yyjson_mut_doc *doc, yyjson_mut_val *object,
                                        const char *key, const char *value) {
    char *sanitized = sanitize_utf8_lossy(value);
    if (!sanitized) {
        return false;
    }
    bool ok = yyjson_mut_obj_add_strcpy(doc, object, key, sanitized);
    free(sanitized);
    return ok;
}

static yyjson_mut_val *compare_node_json(yyjson_mut_doc *doc,
                                         const cbm_graph_node_identity_t *node) {
    yyjson_mut_val *object = yyjson_mut_obj(doc);
    if (!object ||
        !compare_add_identity_string(doc, object, "qualified_name", node->qualified_name) ||
        !compare_add_identity_string(doc, object, "label", node->label) ||
        !compare_add_identity_string(doc, object, "file_path", node->file_path)) {
        return NULL;
    }
    return object;
}

static bool compare_append_item(compare_response_t *response, compare_result_set_t *set,
                                yyjson_mut_doc *item_doc, yyjson_mut_val *item) {
    if (!item_doc || !item) {
        yyjson_mut_doc_free(item_doc);
        return false;
    }
    char *encoded = yy_doc_to_str(item_doc);
    if (!encoded) {
        yyjson_mut_doc_free(item_doc);
        return false;
    }
    size_t encoded_len = strlen(encoded);
    size_t separator = set->returned > 0 ? 1U : 0U;
    free(encoded);

    if (set->encoded_bytes > MCP_COMPARE_SET_BYTE_BUDGET ||
        separator > MCP_COMPARE_SET_BYTE_BUDGET - set->encoded_bytes ||
        encoded_len > MCP_COMPARE_SET_BYTE_BUDGET - set->encoded_bytes - separator) {
        set->budget_exhausted = true;
        yyjson_mut_doc_free(item_doc);
        return true;
    }

    yyjson_mut_val *copy = yyjson_mut_val_mut_copy(response->doc, item);
    bool ok = copy && yyjson_mut_arr_add_val(set->items, copy);
    yyjson_mut_doc_free(item_doc);
    if (!ok) {
        return false;
    }
    set->encoded_bytes += separator + encoded_len;
    set->returned++;
    return true;
}

static bool compare_node_callback(void *context, bool added,
                                  const cbm_graph_node_identity_t *node) {
    compare_response_t *response = (compare_response_t *)context;
    compare_result_set_t *set = added ? &response->nodes_added : &response->nodes_removed;
    if (set->returned >= response->limit || set->budget_exhausted) {
        return true;
    }
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *item = doc ? compare_node_json(doc, node) : NULL;
    if (doc && item) {
        yyjson_mut_doc_set_root(doc, item);
    }
    return compare_append_item(response, set, doc, item);
}

static bool compare_edge_callback(void *context, bool added,
                                  const cbm_graph_edge_identity_t *edge) {
    compare_response_t *response = (compare_response_t *)context;
    compare_result_set_t *set = added ? &response->edges_added : &response->edges_removed;
    if (set->returned >= response->limit || set->budget_exhausted) {
        return true;
    }
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *item = doc ? yyjson_mut_obj(doc) : NULL;
    yyjson_mut_val *source = doc ? compare_node_json(doc, &edge->source) : NULL;
    yyjson_mut_val *target = doc ? compare_node_json(doc, &edge->target) : NULL;
    bool ok = item && source && target && yyjson_mut_obj_add_val(doc, item, "source", source) &&
              yyjson_mut_obj_add_val(doc, item, "target", target) &&
              compare_add_identity_string(doc, item, "type", edge->type) &&
              compare_add_identity_string(doc, item, "local_name_gen", edge->local_name_gen);
    if (ok) {
        yyjson_mut_doc_set_root(doc, item);
    }
    return compare_append_item(response, set, doc, ok ? item : NULL);
}

static bool compare_cancel_callback(void *context) {
    compare_response_t *response = (compare_response_t *)context;
    return mcp_request_cancelled(response->server);
}

static yyjson_mut_val *compare_project_json(yyjson_mut_doc *doc, const char *project,
                                            const cbm_graph_compare_project_t *metadata) {
    yyjson_mut_val *object = yyjson_mut_obj(doc);
    if (!object || !yyjson_mut_obj_add_strcpy(doc, object, "project", project) ||
        !compare_add_identity_string(doc, object, "generation", metadata->generation) ||
        !compare_add_identity_string(doc, object, "index_mode", metadata->index_mode) ||
        !yyjson_mut_obj_add_sint(doc, object, "node_count", metadata->node_count) ||
        !yyjson_mut_obj_add_sint(doc, object, "edge_count", metadata->edge_count)) {
        return NULL;
    }
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
        !yyjson_mut_obj_add_bool(doc, object, "truncated", truncated)) {
        return NULL;
    }
    if (truncated && set->returned >= limit && !yyjson_mut_arr_add_strcpy(doc, reasons, "limit")) {
        return NULL;
    }
    if (truncated && set->budget_exhausted &&
        !yyjson_mut_arr_add_strcpy(doc, reasons, "encoded_byte_budget")) {
        return NULL;
    }
    if (!yyjson_mut_obj_add_val(doc, object, "truncation_reasons", reasons)) {
        return NULL;
    }
    return object;
}

static char *handle_compare_graphs(cbm_mcp_server_t *server, const char *args) {
    char *base_project = NULL;
    char *target_project = NULL;
    uint64_t limit = 0;
    uint64_t scan_limit = 0;
    const char *argument_error = NULL;
    if (!compare_parse_arguments(args, &base_project, &target_project, &limit, &scan_limit,
                                 &argument_error)) {
        return compare_graphs_error("invalid_arguments", argument_error);
    }
    if (mcp_request_cancelled(server)) {
        free(base_project);
        free(target_project);
        return compare_graphs_error("cancelled", "compare_graphs cancelled for this request");
    }

    cbm_store_t *base_store = compare_open_project_store(base_project);
    if (!base_store) {
        free(base_project);
        free(target_project);
        return compare_graphs_error("project_not_indexed", "base project is not indexed");
    }
    cbm_store_t *target_store = compare_open_project_store(target_project);
    if (!target_store) {
        cbm_store_close(base_store);
        free(base_project);
        free(target_project);
        return compare_graphs_error("project_not_indexed", "target project is not indexed");
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    compare_response_t response = {
        .server = server,
        .doc = doc,
        .limit = (size_t)limit,
        .nodes_added = {.items = doc ? yyjson_mut_arr(doc) : NULL, .encoded_bytes = 2U},
        .nodes_removed = {.items = doc ? yyjson_mut_arr(doc) : NULL, .encoded_bytes = 2U},
        .edges_added = {.items = doc ? yyjson_mut_arr(doc) : NULL, .encoded_bytes = 2U},
        .edges_removed = {.items = doc ? yyjson_mut_arr(doc) : NULL, .encoded_bytes = 2U},
    };
    if (!doc || !root || !response.nodes_added.items || !response.nodes_removed.items ||
        !response.edges_added.items || !response.edges_removed.items) {
        cbm_store_close(target_store);
        cbm_store_close(base_store);
        yyjson_mut_doc_free(doc);
        free(base_project);
        free(target_project);
        return compare_graphs_error("allocation_failed", "could not allocate comparison result");
    }
    yyjson_mut_doc_set_root(doc, root);

    cbm_graph_compare_result_t comparison = {0};
    int compare_rc = cbm_store_compare_graphs(
        base_store, base_project, target_store, target_project, scan_limit, compare_cancel_callback,
        compare_node_callback, compare_edge_callback, &response, &comparison);
    cbm_store_close(target_store);
    cbm_store_close(base_store);

    if (compare_rc != CBM_STORE_OK) {
        yyjson_mut_doc_free(doc);
        free(base_project);
        free(target_project);
        if (compare_rc == CBM_STORE_CANCELLED) {
            return compare_graphs_error("cancelled", "compare_graphs cancelled for this request");
        }
        if (compare_rc == CBM_STORE_NOT_FOUND) {
            return compare_graphs_error("project_not_indexed", "project is not indexed");
        }
        if (compare_rc == CBM_STORE_SCAN_LIMIT) {
            return compare_graphs_error("scan_limit_exceeded",
                                        "combined graph rows exceed scan_limit");
        }
        if (compare_rc == CBM_STORE_CALLBACK_ERR) {
            return compare_graphs_error("allocation_failed",
                                        "could not allocate comparison result");
        }
        return compare_graphs_error("query_failed", "graph comparison query failed");
    }

    yyjson_mut_val *base = compare_project_json(doc, base_project, &comparison.base);
    yyjson_mut_val *target = compare_project_json(doc, target_project, &comparison.target);
    yyjson_mut_val *nodes = yyjson_mut_obj(doc);
    yyjson_mut_val *edges = yyjson_mut_obj(doc);
    yyjson_mut_val *limits = yyjson_mut_obj(doc);
    yyjson_mut_val *nodes_added =
        compare_set_json(doc, &response.nodes_added, comparison.nodes_added_total, response.limit);
    yyjson_mut_val *nodes_removed = compare_set_json(
        doc, &response.nodes_removed, comparison.nodes_removed_total, response.limit);
    yyjson_mut_val *edges_added =
        compare_set_json(doc, &response.edges_added, comparison.edges_added_total, response.limit);
    yyjson_mut_val *edges_removed = compare_set_json(
        doc, &response.edges_removed, comparison.edges_removed_total, response.limit);
    bool built =
        base && target && nodes && edges && limits && nodes_added && nodes_removed && edges_added &&
        edges_removed && yyjson_mut_obj_add_int(doc, root, "schema_version", 1) &&
        yyjson_mut_obj_add_val(doc, root, "base", base) &&
        yyjson_mut_obj_add_val(doc, root, "target", target) &&
        yyjson_mut_obj_add_val(doc, nodes, "added", nodes_added) &&
        yyjson_mut_obj_add_val(doc, nodes, "removed", nodes_removed) &&
        yyjson_mut_obj_add_val(doc, root, "nodes", nodes) &&
        yyjson_mut_obj_add_val(doc, edges, "added", edges_added) &&
        yyjson_mut_obj_add_val(doc, edges, "removed", edges_removed) &&
        yyjson_mut_obj_add_val(doc, root, "edges", edges) &&
        yyjson_mut_obj_add_uint(doc, limits, "limit", limit) &&
        yyjson_mut_obj_add_uint(doc, limits, "scan_limit", scan_limit) &&
        yyjson_mut_obj_add_uint(doc, limits, "encoded_byte_budget", MCP_COMPARE_SET_BYTE_BUDGET) &&
        yyjson_mut_obj_add_val(doc, root, "limits", limits);
    free(base_project);
    free(target_project);
    if (!built) {
        yyjson_mut_doc_free(doc);
        return compare_graphs_error("allocation_failed", "could not allocate comparison result");
    }
    if (mcp_request_cancelled(server)) {
        yyjson_mut_doc_free(doc);
        return compare_graphs_error("cancelled", "compare_graphs cancelled for this request");
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    if (!json) {
        return compare_graphs_error("allocation_failed", "could not serialize comparison result");
    }
    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* Validate edge type: uppercase letters + underscore only, max 64 chars. */
static __attribute__((unused)) bool validate_edge_type(const char *s) {
    if (!s || strlen(s) > CBM_SZ_64) {
        return false;
    }
    for (const char *c = s; *c; c++) {
        if (!(*c >= 'A' && *c <= 'Z') && *c != '_') {
            return false;
        }
    }
    return true;
}

/* Enrich search result with 1-hop connected node names. */
/* Add BFS results to a yyjson array (deduped by name). */
static void enrich_add_bfs(yyjson_mut_doc *doc, yyjson_mut_val *arr, cbm_traverse_result_t *tr) {
    for (int j = 0; j < tr->visited_count; j++) {
        if (tr->visited[j].node.name) {
            yyjson_mut_arr_add_strcpy(doc, arr, tr->visited[j].node.name);
        }
    }
}

/* Enrich search result with 1-hop connected node names (inbound + outbound). */
/* Build the connected-names array (1-hop callers + callees) for a node.
 * Returns a (possibly empty) yyjson array owned by doc. */
static yyjson_mut_val *enrich_connected(yyjson_mut_doc *doc, cbm_store_t *store, int64_t node_id,
                                        const char *relationship) {
    const char *et[] = {relationship ? relationship : "CALLS"};
    yyjson_mut_val *conn = yyjson_mut_arr(doc);

    /* BFS doesn't support "both" — run inbound + outbound separately. */
    cbm_traverse_result_t tr_in = {0};
    cbm_store_bfs(store, node_id, "inbound", et, SKIP_ONE, SKIP_ONE, MCP_DEFAULT_LIMIT, &tr_in);
    enrich_add_bfs(doc, conn, &tr_in);
    cbm_store_traverse_free(&tr_in);

    cbm_traverse_result_t tr_out = {0};
    cbm_store_bfs(store, node_id, "outbound", et, SKIP_ONE, SKIP_ONE, MCP_DEFAULT_LIMIT, &tr_out);
    enrich_add_bfs(doc, conn, &tr_out);
    cbm_store_traverse_free(&tr_out);

    return conn;
}

/* Text-tree variant: the same 1-hop neighbor names ';'-joined into buf (no
 * commas/spaces in names, so the cell needs no quoting). Empty when none. */
static void enrich_connected_joined(cbm_store_t *store, int64_t node_id, const char *relationship,
                                    char *buf, size_t bufsz) {
    buf[0] = '\0';
    const char *et[] = {relationship ? relationship : "CALLS"};
    size_t off = 0;
    const char *dirs[2] = {"inbound", "outbound"};
    for (int d = 0; d < 2; d++) {
        cbm_traverse_result_t tr = {0};
        cbm_store_bfs(store, node_id, dirs[d], et, SKIP_ONE, SKIP_ONE, MCP_DEFAULT_LIMIT, &tr);
        for (int j = 0; j < tr.visited_count && off + 2 < bufsz; j++) {
            if (!tr.visited[j].node.name) {
                continue;
            }
            int n =
                snprintf(buf + off, bufsz - off, "%s%s", off ? ";" : "", tr.visited[j].node.name);
            if (n < 0 || (size_t)n >= bufsz - off) {
                buf[off] = '\0';
                break;
            }
            off += (size_t)n;
        }
        cbm_store_traverse_free(&tr);
    }
}

/* Build an FTS5 MATCH expression from a free-form query string by splitting
 * on whitespace and joining the terms with OR.  Each token is also sanitized:
 * anything that isn't alnum or underscore is dropped, so the caller can't
 * inject FTS5 operators or double-quoted phrases.  Returns the number of
 * tokens emitted (0 if the query contained no usable terms). */
enum {
    BM25_MIN_BUF = 2, /* minimum buffer size: at least NUL + one char */
    BM25_SEP_RESERVE = 1,
    BM25_QUERY_BUF = 1024,
    BM25_DEFAULT_LIMIT = 50,
    BM25_COL_ID = 0,
    BM25_COL_LABEL = 1,
    BM25_COL_NAME = 2,
    BM25_COL_QN = 3,
    BM25_COL_FILE = 4,
    BM25_COL_START = 5,
    BM25_COL_END = 6,
    BM25_COL_RANK = 7,
    BM25_BIND_QUERY = 1,
    BM25_BIND_PROJECT = 2,
    BM25_BIND_LIMIT = 3,
    BM25_BIND_OFFSET = 4,
    BM25_BIND_INNER = 5,
    BM25_BIND_FILE = 6,
    BM25_SQL_AUTO_LEN = -1,
    /* Inner FTS5 candidate cap.  SQLite can early-terminate a plain FTS5 query
     * (no JOIN/WHERE on outer table) of the form:
     *   SELECT rowid, bm25() FROM nodes_fts WHERE MATCH ? ORDER BY bm25() LIMIT N
     * By fetching only the top BM25_INNER_LIMIT candidates from the FTS5 index
     * and then joining/filtering/re-ranking those, we bound all work to O(N) where
     * N = BM25_INNER_LIMIT rather than the full match set size. */
    BM25_INNER_LIMIT = 2000,
};

/* Column weights for nodes_fts (name, qualified_name, label, file_path, body).
 * The four identifier columns stay at parity; prose sits well below them.
 * FTS5 applies these to per-column term frequency BEFORE the tf-saturation
 * term, which is what makes the weighting BM25F-correct rather than a post-hoc
 * rescale. 0.3 is the findability-favouring end of the field weighting the IR
 * literature settles on for body text (typical title:body ratios run 3:1 to
 * 10:1): a prose-only hit still surfaces, but never outranks a node whose
 * IDENTIFIER matches.
 *
 * Defined once and used by BOTH the ranked query and the count query — they
 * share an inner candidate window, so different weights would silently
 * desynchronise the reported total from the rows returned.
 *
 * Safe against a legacy four-column nodes_fts: FTS5's bm25() reads a weight
 * only when an instance actually lands in that column (`nVal > ic`), so the
 * fifth weight is simply never consulted on a table that has no fifth
 * column. */
#define BM25_WEIGHTS "bm25(nodes_fts, 1.0, 1.0, 1.0, 1.0, 0.3)"

/* Module-local SQLITE_TRANSIENT wrapper to dodge performance-no-int-to-ptr.
 * See the matching helper in src/store/store.c for the same pattern. */
static sqlite3_destructor_type mcp_sqlite_transient(void) {
    static const volatile intptr_t raw = -1;
    sqlite3_destructor_type dtor = NULL;
    memcpy(&dtor, (const void *)&raw, sizeof(dtor));
    return dtor;
}
#define MCP_SQLITE_TRANSIENT (mcp_sqlite_transient())

static int bm25_build_match(const char *query, char *out, size_t out_size) {
    if (!query || !out || out_size < BM25_MIN_BUF) {
        return 0;
    }
    size_t pos = 0;
    int tokens = 0;
    const char *p = query;
    while (*p) {
        while (*p && !((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
                       (*p >= '0' && *p <= '9') || *p == '_')) {
            p++;
        }
        if (!*p) {
            break;
        }
        const char *tok_start = p;
        while (*p && ((*p >= 'a' && *p <= 'z') || (*p >= 'A' && *p <= 'Z') ||
                      (*p >= '0' && *p <= '9') || *p == '_')) {
            p++;
        }
        size_t tok_len = (size_t)(p - tok_start);
        if (tok_len == 0) {
            continue;
        }
        const char *sep = (tokens > 0) ? " OR " : "";
        size_t sep_len = strlen(sep);
        if (pos + sep_len + tok_len + BM25_SEP_RESERVE >= out_size) {
            break; /* out of room — stop cleanly, keep what we have */
        }
        memcpy(out + pos, sep, sep_len);
        pos += sep_len;
        memcpy(out + pos, tok_start, tok_len);
        pos += tok_len;
        tokens++;
    }
    out[pos] = '\0';
    return tokens;
}

static char *bm25_file_pattern_like(const char *file_pattern) {
    if (!file_pattern) {
        return NULL;
    }
    char *like = cbm_glob_to_like(file_pattern);
    if (like && !strchr(file_pattern, '*') && !strchr(file_pattern, '?')) {
        size_t len = strlen(like);
        char *contains = malloc(len + MCP_SEPARATOR + SKIP_ONE);
        if (contains) {
            contains[0] = '%';
            memcpy(contains + SKIP_ONE, like, len);
            contains[len + SKIP_ONE] = '%';
            contains[len + MCP_SEPARATOR] = '\0';
            free(like);
            like = contains;
        }
    }
    return like;
}

/* Run the BM25 full-text search path and return the JSON result string.
 * Returns NULL if FTS5 is unavailable or the query produced no usable tokens,
 * in which case the caller falls back to the regex-based search path. */
static __attribute__((unused)) char *bm25_search(cbm_store_t *store, const char *project, const char *query,
                         const char *file_pattern, int limit, int offset, bool toon) {
    sqlite3 *db = cbm_store_get_db(store);
    if (!db) {
        return NULL;
    }
    char fts_query[BM25_QUERY_BUF];
    int tok_count = bm25_build_match(query, fts_query, sizeof(fts_query));
    if (tok_count == 0) {
        return NULL;
    }
    char *file_like = bm25_file_pattern_like(file_pattern);

    /* BM25 ranked query using a two-step approach to enable FTS5 early termination.
     *
     * Flat queries of the form:
     *   SELECT ... FROM nodes_fts JOIN nodes WHERE MATCH ? AND n.project=? ORDER BY rank LIMIT N
     * block FTS5's WAND/MaxScore early-exit because the outer JOIN+WHERE conditions
     * are invisible to the FTS5 planner — it must score every matching document before
     * the project/label filter can discard any of them.  On a large codebase with 100K+
     * matches, this causes multi-minute queries.
     *
     * The fix: let FTS5 drive the inner subquery alone.  SQLite CAN early-terminate
     *   SELECT rowid, bm25(nodes_fts,...) FROM nodes_fts WHERE MATCH ? ORDER BY bm25() LIMIT N
     * because no outer predicate blocks it.  We fetch BM25_INNER_LIMIT top candidates
     * from the FTS5 index, then join/filter/boost only those rows.  bm25() returns a
     * NEGATIVE score (lower = more relevant). */
    const char *sql =
        "SELECT n.id, n.label, n.name, n.qualified_name, n.file_path, n.start_line, n.end_line, "
        "       (fts.base_rank "
        "        - CASE WHEN n.label IN ('Function','Method') THEN 10.0 "
        "               WHEN n.label = 'Route' THEN 8.0 "
        "               WHEN n.label IN (" CBM_SQL_TYPE_LIKE_LABELS ") THEN 5.0 "
        /* Relations rank with the type tier: a table IS the schema container
         * a data question is looking for (findability-first). */
        "               WHEN n.label IN (" CBM_SQL_RELATION_LABELS ") THEN 5.0 "
        "               ELSE 0.0 END) AS rank "
        "FROM ("
        "    SELECT rowid, " BM25_WEIGHTS " AS base_rank"
        "    FROM nodes_fts WHERE nodes_fts MATCH ?1"
        "    ORDER BY base_rank LIMIT ?5"
        ") fts "
        "JOIN nodes n ON n.id = fts.rowid "
        "WHERE n.project = ?2 "
        /* Section and Module are NO LONGER excluded (#518/#519): they are the
         * labels that carry prose — a Markdown section's body, a config file's
         * description — so excluding them made the body column unreachable.
         * This exclusion list is MIRRORED in the count query below; the two
         * must be changed together or results desynchronise from counts. */
        "  AND n.label NOT IN ('File','Folder','Variable','Project') "
        "  AND (?6 IS NULL OR n.file_path LIKE ?6) "
        /* rank ties are common (boosted floats) — the id tie-break makes
         * offset pages contractually stable across calls. */
        "ORDER BY rank, n.id "
        "LIMIT ?3 OFFSET ?4";

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, sql, BM25_SQL_AUTO_LEN, &stmt, NULL) != SQLITE_OK) {
        free(file_like);
        return NULL;
    }
    sqlite3_bind_text(stmt, BM25_BIND_QUERY, fts_query, BM25_SQL_AUTO_LEN, MCP_SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, BM25_BIND_PROJECT, project, BM25_SQL_AUTO_LEN, MCP_SQLITE_TRANSIENT);
    sqlite3_bind_int(stmt, BM25_BIND_LIMIT, limit > 0 ? limit : BM25_DEFAULT_LIMIT);
    sqlite3_bind_int(stmt, BM25_BIND_OFFSET, offset > 0 ? offset : 0);
    sqlite3_bind_int(stmt, BM25_BIND_INNER, BM25_INNER_LIMIT);
    if (file_like) {
        sqlite3_bind_text(stmt, BM25_BIND_FILE, file_like, BM25_SQL_AUTO_LEN, MCP_SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(stmt, BM25_BIND_FILE);
    }

    /* Count hits within the same inner-limit window — capped at BM25_INNER_LIMIT.
     * Uses the identical subquery structure so the FTS5 early-exit applies here too. */
    int total = 0;
    {
        const char *count_sql = "SELECT COUNT(*) FROM ("
                                "    SELECT fts.rowid FROM ("
                                "        SELECT rowid FROM nodes_fts WHERE nodes_fts MATCH ?1"
                                "        ORDER BY " BM25_WEIGHTS " LIMIT ?3"
                                "    ) fts "
                                "    JOIN nodes n ON n.id = fts.rowid "
                                "    WHERE n.project = ?2 "
                                /* MIRRORS the ranked query's filter verbatim — same weights, same
                                 * label exclusions. Changing one alone reports a total that does
                                 * not describe the rows returned. */
                                "      AND n.label NOT IN ('File','Folder','Variable','Project')"
                                "      AND (?6 IS NULL OR n.file_path LIKE ?6)"
                                ")";
        sqlite3_stmt *cs = NULL;
        if (sqlite3_prepare_v2(db, count_sql, BM25_SQL_AUTO_LEN, &cs, NULL) == SQLITE_OK) {
            sqlite3_bind_text(cs, BM25_BIND_QUERY, fts_query, BM25_SQL_AUTO_LEN,
                              MCP_SQLITE_TRANSIENT);
            sqlite3_bind_text(cs, BM25_BIND_PROJECT, project, BM25_SQL_AUTO_LEN,
                              MCP_SQLITE_TRANSIENT);
            sqlite3_bind_int(cs, BM25_BIND_LIMIT, BM25_INNER_LIMIT);
            if (file_like) {
                sqlite3_bind_text(cs, BM25_BIND_FILE, file_like, BM25_SQL_AUTO_LEN,
                                  MCP_SQLITE_TRANSIENT);
            } else {
                sqlite3_bind_null(cs, BM25_BIND_FILE);
            }
            if (sqlite3_step(cs) == SQLITE_ROW) {
                total = sqlite3_column_int(cs, 0);
            }
            sqlite3_finalize(cs);
        }
    }

    if (toon) {
        /* TOON: rows are buffered first because the table header carries the
         * row count, which sqlite only yields by stepping to completion. */
        cbm_sb_t rows;
        cbm_sb_init(&rows);
        int emitted = 0;
        while (sqlite3_step(stmt) == SQLITE_ROW) {
            char lines[CBM_SZ_32];
            int sl = sqlite3_column_int(stmt, BM25_COL_START);
            int el = sqlite3_column_int(stmt, BM25_COL_END);
            if (sl > 0) {
                snprintf(lines, sizeof(lines), "%d-%d", sl, el > sl ? el : sl);
            } else {
                lines[0] = '\0';
            }
            cbm_tree_row_begin(&rows);
            cbm_tree_cell_str(&rows, (const char *)sqlite3_column_text(stmt, BM25_COL_QN), true);
            cbm_tree_cell_str(&rows, (const char *)sqlite3_column_text(stmt, BM25_COL_LABEL),
                              false);
            cbm_tree_cell_str(&rows, (const char *)sqlite3_column_text(stmt, BM25_COL_FILE), false);
            cbm_tree_cell_str(&rows, lines, false);
            cbm_tree_cell_real(&rows, sqlite3_column_double(stmt, BM25_COL_RANK), false);
            cbm_tree_row_end(&rows);
            emitted++;
        }
        sqlite3_finalize(stmt);
        free(file_like);

        cbm_sb_t sb;
        cbm_sb_init(&sb);
        cbm_tree_scalar_int(&sb, "total", total);
        cbm_tree_scalar_str(&sb, "search_mode", "bm25");
        static const char *const cols[] = {"qn", "label", "file", "lines", "rank"};
        cbm_tree_table_header(&sb, "results", emitted, cols, 5);
        char *rows_text = cbm_sb_finish(&rows);
        cbm_sb_append(&sb, rows_text ? rows_text : "");
        free(rows_text);
        cbm_tree_scalar_bool(&sb, "has_more", total > offset + emitted);
        return cbm_sb_finish(&sb);
    }

    /* format:"json" = json-stringified tree: cols + column-ordered row
     * arrays (rank order preserved — no grouping on ranked output). */
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_int(doc, root, "total", total);
    yyjson_mut_obj_add_str(doc, root, "search_mode", "bm25");
    yyjson_mut_val *jcols = yyjson_mut_arr(doc);
    static const char *const bm25_cols[] = {"qn", "label", "file", "lines", "rank"};
    for (size_t ci = 0; ci < sizeof(bm25_cols) / sizeof(bm25_cols[0]); ci++) {
        yyjson_mut_arr_add_str(doc, jcols, bm25_cols[ci]);
    }
    yyjson_mut_obj_add_val(doc, root, "cols", jcols);

    yyjson_mut_val *rows = yyjson_mut_arr(doc);
    int emitted = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        char lines[CBM_SZ_32];
        int sl = sqlite3_column_int(stmt, BM25_COL_START);
        int el = sqlite3_column_int(stmt, BM25_COL_END);
        if (sl > 0) {
            snprintf(lines, sizeof(lines), "%d-%d", sl, el > sl ? el : sl);
        } else {
            lines[0] = '\0';
        }
        yyjson_mut_val *row = yyjson_mut_arr(doc);
        yyjson_mut_arr_add_strcpy(doc, row, (const char *)sqlite3_column_text(stmt, BM25_COL_QN));
        yyjson_mut_arr_add_strcpy(doc, row,
                                  (const char *)sqlite3_column_text(stmt, BM25_COL_LABEL));
        yyjson_mut_arr_add_strcpy(doc, row, (const char *)sqlite3_column_text(stmt, BM25_COL_FILE));
        yyjson_mut_arr_add_strcpy(doc, row, lines);
        yyjson_mut_arr_add_real(doc, row, sqlite3_column_double(stmt, BM25_COL_RANK));
        yyjson_mut_arr_add_val(rows, row);
        emitted++;
    }
    sqlite3_finalize(stmt);
    free(file_like);

    yyjson_mut_obj_add_val(doc, root, "rows", rows);
    yyjson_mut_obj_add_bool(doc, root, "has_more", total > offset + emitted);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return json;
}

/* Extract keyword strings from a yyjson array into `keywords`.  Returns the
 * number of strings copied (capped at `max_out`). */
static int extract_semantic_keywords(yyjson_val *sq_val, const char **keywords, int max_out) {
    int kw_count = (int)yyjson_arr_size(sq_val);
    if (kw_count > max_out) {
        kw_count = max_out;
    }
    size_t kw_idx = 0;
    size_t kw_max = 0;
    yyjson_val *kw_val;
    int ki = 0;
    yyjson_arr_foreach(sq_val, kw_idx, kw_max, kw_val) {
        if (ki < kw_count && yyjson_is_str(kw_val)) {
            keywords[ki++] = yyjson_get_str(kw_val);
        }
    }
    return ki;
}

/* Emit vector-search hits in the json-tree model: "semantic": {cols, rows}
 * — score order preserved (ranked output is never regrouped). */
static __attribute__((unused)) void emit_semantic_results(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                  cbm_vector_result_t *vresults, int vcount) {
    yyjson_mut_val *sem = yyjson_mut_obj(doc);
    yyjson_mut_val *scols = yyjson_mut_arr(doc);
    static const char *const sem_cols[] = {"qn", "label", "file", "score"};
    for (size_t ci = 0; ci < sizeof(sem_cols) / sizeof(sem_cols[0]); ci++) {
        yyjson_mut_arr_add_str(doc, scols, sem_cols[ci]);
    }
    yyjson_mut_obj_add_val(doc, sem, "cols", scols);
    yyjson_mut_val *sem_results = yyjson_mut_arr(doc);
    for (int v = 0; v < vcount; v++) {
        yyjson_mut_val *vitem = yyjson_mut_arr(doc);
        yyjson_mut_arr_add_strcpy(doc, vitem, vresults[v].qualified_name);
        yyjson_mut_arr_add_strcpy(doc, vitem, vresults[v].label);
        yyjson_mut_arr_add_strcpy(doc, vitem, vresults[v].file_path);
        yyjson_mut_arr_add_real(doc, vitem, vresults[v].score);
        yyjson_mut_arr_add_val(sem_results, vitem);
    }
    yyjson_mut_obj_add_val(doc, sem, "rows", sem_results);
    yyjson_mut_obj_add_val(doc, root, "semantic", sem);
}

/* Run the semantic_query vector search from raw args. Sets *out_vresults /
 * *out_vcount (caller frees via cbm_store_free_vector_results when vcount>0).
 * Returns true if semantic_query was provided as a non-array (type error —
 * caller should surface to the user). */
static __attribute__((unused)) bool run_semantic_query_core(const char *args, cbm_store_t *store, const char *project,
                                    int limit, cbm_vector_result_t **out_vresults, int *out_vcount,
                                    bool *out_present) {
    enum { MAX_KW_SEARCH = 32 };
    *out_vresults = NULL;
    *out_vcount = 0;
    if (out_present) {
        *out_present = false;
    }
    yyjson_doc *args_doc = yyjson_read(args, strlen(args), 0);
    yyjson_val *args_root = args_doc ? yyjson_doc_get_root(args_doc) : NULL;
    yyjson_val *sq_val = args_root ? yyjson_obj_get(args_root, "semantic_query") : NULL;
    if (out_present && sq_val) {
        *out_present = true;
    }
    bool type_error = false;
    if (sq_val && !yyjson_is_arr(sq_val)) {
        type_error = true;
    } else if (sq_val && yyjson_arr_size(sq_val) > 0) {
        const char *keywords[MAX_KW_SEARCH];
        int ki = extract_semantic_keywords(sq_val, keywords, MAX_KW_SEARCH);
        cbm_vector_result_t *vresults = NULL;
        int vcount = 0;
        int sem_limit = limit > 0 ? limit : CBM_SZ_16;
        if (cbm_store_vector_search(store, project, keywords, ki, sem_limit, &vresults, &vcount) ==
                CBM_STORE_OK &&
            vcount > 0) {
            *out_vresults = vresults;
            *out_vcount = vcount;
        }
    }
    if (args_doc) {
        yyjson_doc_free(args_doc);
    }
    return type_error;
}

/* ── Tree output for search_graph ───────────────────────────────────
 * Default response encoding: grouped tree rows (compact_out.h). The same
 * model is available as structured JSON via format:"json"; include_connected
 * adds a `connected` column in BOTH encodings. */

enum { SG_MAX_EXTRA_FIELDS = 12 };

/* Internal-only node properties never emitted to agents: similarity /
 * semantic pipeline intermediates (minhash fingerprint, structural profile,
 * body-token bag). They dominate payload size and carry zero agent value. */
static bool sg_field_blocked(const char *f) {
    return strcmp(f, "fp") == 0 || strcmp(f, "sp") == 0 || strcmp(f, "bt") == 0;
}

/* Core row columns every search result already carries. Requesting one as an
 * extra `fields` entry used to emit a silent empty column (core values are
 * node columns, never in the properties JSON) — field-eval agents burned a
 * round-trip on exactly that. Drop them and teach instead. */
static bool sg_field_is_core(const char *f) {
    return strcmp(f, "qn") == 0 || strcmp(f, "qualified_name") == 0 || strcmp(f, "name") == 0 ||
           strcmp(f, "label") == 0 || strcmp(f, "file") == 0 || strcmp(f, "file_path") == 0 ||
           strcmp(f, "path") == 0 || strcmp(f, "lines") == 0 || strcmp(f, "in") == 0 ||
           strcmp(f, "out") == 0;
}

/* Parse the `fields` argument (array of property names) into out[] as
 * pointers owned by the returned doc (caller frees the doc after emission).
 * Blocked internal fields are silently dropped; core-column requests are
 * dropped too and reported via *core_requested so the emitter can hint. */
static __attribute__((unused)) int sg_parse_fields(const char *args, const char *out[], int max_out, yyjson_doc **out_owner,
                           bool *core_requested) {
    *out_owner = NULL;
    if (core_requested) {
        *core_requested = false;
    }
    yyjson_doc *args_doc = yyjson_read(args, strlen(args), 0);
    yyjson_val *args_root = args_doc ? yyjson_doc_get_root(args_doc) : NULL;
    yyjson_val *fv = args_root ? yyjson_obj_get(args_root, "fields") : NULL;
    if (!fv || !yyjson_is_arr(fv)) {
        if (args_doc) {
            yyjson_doc_free(args_doc);
        }
        return 0;
    }
    int n = 0;
    size_t idx = 0;
    size_t max = 0;
    yyjson_val *item;
    yyjson_arr_foreach(fv, idx, max, item) {
        const char *s = yyjson_get_str(item);
        if (!s || !s[0] || sg_field_blocked(s)) {
            continue;
        }
        if (sg_field_is_core(s)) {
            if (core_requested) {
                *core_requested = true;
            }
            continue;
        }
        if (n < max_out) {
            out[n++] = s;
        }
    }
    if (n == 0) {
        yyjson_doc_free(args_doc);
        return 0;
    }
    *out_owner = args_doc;
    return n;
}

/* Append a property as one compact-output cell. Compound values stay one
 * column by using their compact JSON representation; the cell emitter quotes
 * and escapes that representation as needed. */
static void sg_toon_property_cell(cbm_sb_t *sb, yyjson_val *v) {
    if (v && yyjson_is_str(v)) {
        cbm_tree_cell_str(sb, yyjson_get_str(v), false);
    } else if (v && yyjson_is_bool(v)) {
        cbm_tree_cell_bool(sb, yyjson_get_bool(v), false);
    } else if (v && yyjson_is_int(v)) {
        cbm_tree_cell_int(sb, yyjson_get_int(v), false);
    } else if (v && yyjson_is_real(v)) {
        cbm_tree_cell_real(sb, yyjson_get_real(v), false);
    } else if (v && !yyjson_is_null(v)) {
        char *json = yyjson_val_write(v, 0, NULL);
        cbm_tree_cell_str(sb, json ? json : "", false);
        free(json);
    } else {
        cbm_tree_cell_str(sb, "", false);
    }
}

/* Append one row's extra-field cells, pulled from the node's properties. */
static void sg_toon_extra_cells(cbm_sb_t *sb, const char *props_json, const char *const *fields,
                                int nfields) {
    yyjson_doc *pd =
        (props_json && props_json[0]) ? yyjson_read(props_json, strlen(props_json), 0) : NULL;
    yyjson_val *pr = pd ? yyjson_doc_get_root(pd) : NULL;
    for (int f = 0; f < nfields; f++) {
        yyjson_val *v = (pr && yyjson_is_obj(pr)) ? yyjson_obj_get(pr, fields[f]) : NULL;
        sg_toon_property_cell(sb, v);
    }
    if (pd) {
        yyjson_doc_free(pd);
    }
}

/* "start-end" line range, or empty when the node carries no line info. */
static void sg_lines_str(char *out, size_t sz, int start, int end) {
    if (start > 0) {
        snprintf(out, sz, "%d-%d", start, end > start ? end : start);
    } else {
        out[0] = '\0';
    }
}

/* Emit the regex-path search results as a TOON table. */
static __attribute__((unused)) void emit_search_results_toon(cbm_sb_t *sb, const cbm_search_output_t *out, int offset,
                                     const char *const *fields, int nfields, bool detail_ids) {
    cbm_tree_scalar_int(sb, "total", out->total);
    if (detail_ids) {
        /* ids tier: bare qn enumeration — for "list everything matching X"
         * sweeps where per-row metadata is noise (LocAgent's fold tier). */
        static const char *const id_cols[] = {"qn"};
        cbm_tree_table_header(sb, "results", out->count, id_cols, 1);
        for (int i = 0; i < out->count; i++) {
            cbm_tree_row_begin(sb);
            cbm_tree_cell_str(sb, out->results[i].node.qualified_name, true);
            cbm_tree_row_end(sb);
        }
        cbm_tree_scalar_bool(sb, "has_more", out->total > offset + out->count);
        return;
    }
    const char *cols[6 + SG_MAX_EXTRA_FIELDS] = {"qn", "label", "file", "lines", "in", "out"};
    int ncols = 6;
    for (int f = 0; f < nfields; f++) {
        cols[ncols++] = fields[f];
    }
    cbm_tree_table_header(sb, "results", out->count, cols, ncols);
    for (int i = 0; i < out->count; i++) {
        const cbm_search_result_t *sr = &out->results[i];
        char lines[CBM_SZ_32];
        sg_lines_str(lines, sizeof(lines), sr->node.start_line, sr->node.end_line);
        cbm_tree_row_begin(sb);
        cbm_tree_cell_str(sb, sr->node.qualified_name, true);
        cbm_tree_cell_str(sb, sr->node.label, false);
        cbm_tree_cell_str(sb, sr->node.file_path, false);
        cbm_tree_cell_str(sb, lines, false);
        cbm_tree_cell_int(sb, sr->in_degree, false);
        cbm_tree_cell_int(sb, sr->out_degree, false);
        sg_toon_extra_cells(sb, sr->node.properties_json, fields, nfields);
        cbm_tree_row_end(sb);
    }
    cbm_tree_scalar_bool(sb, "has_more", out->total > offset + out->count);
}

/* ── Tree format (Phase-2 A/B candidate) ────────────────────────────
 * Prefix-factored, file-grouped output: the shared (qn-prefix, file) pair is
 * printed ONCE per group, rows beneath carry only the short name + data
 * cells. The reconstruction rule (qn = group-prefix + "." + name) is stated
 * once in the header so agents can copy exact join keys into follow-up
 * calls. Research basis: HDT front-coding (prefix factoring), LocAgent tree
 * ablation (tree > flat/DOT for LLM comprehension), Lost-in-Distance
 * (related rows adjacent — grouping by module does exactly that). */

/* qn-prefix = qualified_name minus its last '.'-segment. Returns length. */
static size_t sg_qn_prefix_len(const char *qn) {
    const char *last = qn ? strrchr(qn, '.') : NULL;
    return last ? (size_t)(last - qn) : 0;
}

static int sg_cmp_by_qn(const void *pa, const void *pb) {
    const cbm_search_result_t *a = (const cbm_search_result_t *)pa;
    const cbm_search_result_t *b = (const cbm_search_result_t *)pb;
    const char *qa = a->node.qualified_name ? a->node.qualified_name : "";
    const char *qb = b->node.qualified_name ? b->node.qualified_name : "";
    return strcmp(qa, qb);
}

static __attribute__((unused)) void emit_search_results_tree(cbm_sb_t *sb, cbm_search_output_t *out, int offset,
                                     const char *const *fields, int nfields, cbm_store_t *store,
                                     const char *relationship, bool include_connected) {
    char buf[CBM_SZ_512];
    char extra_cols[CBM_SZ_256] = "";
    for (int f = 0; f < nfields; f++) {
        strncat(extra_cols, " ", sizeof(extra_cols) - strlen(extra_cols) - 1);
        strncat(extra_cols, fields[f], sizeof(extra_cols) - strlen(extra_cols) - 1);
    }
    if (include_connected) {
        strncat(extra_cols, " connected", sizeof(extra_cols) - strlen(extra_cols) - 1);
    }
    snprintf(buf, sizeof(buf),
             "total: %d\nresults: %d  (rows: name label lines in out%s; "
             "qn = group prefix + \".\" + name)\n",
             out->total, out->count, extra_cols);
    cbm_sb_append(sb, buf);
    /* Sort by qn so same-prefix rows are adjacent (module clustering). */
    if (out->count > 1) {
        qsort(out->results, (size_t)out->count, sizeof(cbm_search_result_t), sg_cmp_by_qn);
    }
    char cur_group[CBM_SZ_1K] = "";
    for (int i = 0; i < out->count; i++) {
        const cbm_search_result_t *sr = &out->results[i];
        const char *qn = sr->node.qualified_name ? sr->node.qualified_name : "";
        const char *file = sr->node.file_path ? sr->node.file_path : "";
        size_t plen = sg_qn_prefix_len(qn);
        char group[CBM_SZ_1K];
        snprintf(group, sizeof(group), "%.*s (%s)", (int)plen, qn, file);
        if (strcmp(group, cur_group) != 0) {
            snprintf(cur_group, sizeof(cur_group), "%s", group);
            cbm_sb_append(sb, group);
            cbm_sb_append(sb, ":\n");
        }
        const char *shortname = plen ? qn + plen + 1 : qn;
        char lines[CBM_SZ_32];
        sg_lines_str(lines, sizeof(lines), sr->node.start_line, sr->node.end_line);
        char row[CBM_SZ_1K];
        snprintf(row, sizeof(row), "  %s %s %s %d %d", shortname,
                 sr->node.label ? sr->node.label : "", lines, sr->in_degree, sr->out_degree);
        cbm_sb_append(sb, row);
        /* Extra property columns (fields param). Routed through the shared
         * cell emitters so values with spaces (signatures, docstrings) are
         * QUOTED — a raw append would shift every following column. Missing
         * values emit as "-" (the emitter's empty-cell placeholder). */
        if (nfields > 0) {
            yyjson_doc *pd =
                (sr->node.properties_json && sr->node.properties_json[0])
                    ? yyjson_read(sr->node.properties_json, strlen(sr->node.properties_json), 0)
                    : NULL;
            yyjson_val *pr = pd ? yyjson_doc_get_root(pd) : NULL;
            for (int f = 0; f < nfields; f++) {
                yyjson_val *v = (pr && yyjson_is_obj(pr)) ? yyjson_obj_get(pr, fields[f]) : NULL;
                sg_toon_property_cell(sb, v);
            }
            if (pd) {
                yyjson_doc_free(pd);
            }
        }
        if (include_connected && sr->node.id > 0) {
            char joined[CBM_SZ_1K];
            enrich_connected_joined(store, sr->node.id, relationship, joined, sizeof(joined));
            cbm_tree_cell_str(sb, joined, false); /* empty emits "-" */
        }
        cbm_sb_append(sb, "\n");
    }
    snprintf(buf, sizeof(buf), "has_more: %s\n",
             out->total > offset + out->count ? "true" : "false");
    cbm_sb_append(sb, buf);
}

/* json-stringified tree: the SAME grouped model as the text tree, serialized
 * as JSON for agents that need structured parsing — groups with a shared
 * (qn_prefix, file) and column-ordered row ARRAYS (never per-row key
 * envelopes; that legacy shape was 84% key overhead). */
static __attribute__((unused)) void emit_search_results_tree_json(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                          cbm_search_output_t *out, int offset,
                                          const char *const *fields, int nfields,
                                          cbm_store_t *store, const char *relationship,
                                          bool include_connected) {
    yyjson_mut_obj_add_int(doc, root, "total", out->total);
    yyjson_mut_obj_add_int(doc, root, "count", out->count);
    yyjson_mut_val *cols = yyjson_mut_arr(doc);
    static const char *const col_names[] = {"name", "label", "lines", "in", "out"};
    for (size_t i = 0; i < sizeof(col_names) / sizeof(col_names[0]); i++) {
        yyjson_mut_arr_add_str(doc, cols, col_names[i]);
    }
    for (int f = 0; f < nfields; f++) {
        yyjson_mut_arr_add_strcpy(doc, cols, fields[f]);
    }
    if (include_connected) {
        yyjson_mut_arr_add_str(doc, cols, "connected");
    }
    yyjson_mut_obj_add_val(doc, root, "cols", cols);
    if (out->count > 1) {
        qsort(out->results, (size_t)out->count, sizeof(cbm_search_result_t), sg_cmp_by_qn);
    }
    yyjson_mut_val *groups = yyjson_mut_arr(doc);
    yyjson_mut_val *cur = NULL;
    yyjson_mut_val *cur_rows = NULL;
    char cur_key[CBM_SZ_1K] = "";
    for (int i = 0; i < out->count; i++) {
        const cbm_search_result_t *sr = &out->results[i];
        const char *qn = sr->node.qualified_name ? sr->node.qualified_name : "";
        const char *file = sr->node.file_path ? sr->node.file_path : "";
        size_t plen = sg_qn_prefix_len(qn);
        char key[CBM_SZ_1K];
        snprintf(key, sizeof(key), "%.*s|%s", (int)plen, qn, file);
        if (!cur || strcmp(key, cur_key) != 0) {
            snprintf(cur_key, sizeof(cur_key), "%s", key);
            cur = yyjson_mut_obj(doc);
            char prefix[CBM_SZ_1K];
            snprintf(prefix, sizeof(prefix), "%.*s", (int)plen, qn);
            yyjson_mut_obj_add_strcpy(doc, cur, "qn_prefix", prefix);
            yyjson_mut_obj_add_strcpy(doc, cur, "file", file);
            cur_rows = yyjson_mut_arr(doc);
            yyjson_mut_obj_add_val(doc, cur, "rows", cur_rows);
            yyjson_mut_arr_add_val(groups, cur);
        }
        char lines[CBM_SZ_32];
        sg_lines_str(lines, sizeof(lines), sr->node.start_line, sr->node.end_line);
        yyjson_mut_val *row = yyjson_mut_arr(doc);
        yyjson_mut_arr_add_strcpy(doc, row, plen ? qn + plen + 1 : qn);
        yyjson_mut_arr_add_strcpy(doc, row, sr->node.label ? sr->node.label : "");
        yyjson_mut_arr_add_strcpy(doc, row, lines);
        yyjson_mut_arr_add_int(doc, row, sr->in_degree);
        yyjson_mut_arr_add_int(doc, row, sr->out_degree);
        if (nfields > 0) {
            yyjson_doc *pd =
                (sr->node.properties_json && sr->node.properties_json[0])
                    ? yyjson_read(sr->node.properties_json, strlen(sr->node.properties_json), 0)
                    : NULL;
            yyjson_val *pr = pd ? yyjson_doc_get_root(pd) : NULL;
            for (int f = 0; f < nfields; f++) {
                yyjson_val *v = (pr && yyjson_is_obj(pr)) ? yyjson_obj_get(pr, fields[f]) : NULL;
                yyjson_mut_val *copy =
                    (v && !yyjson_is_null(v)) ? yyjson_val_mut_copy(doc, v) : NULL;
                if (copy) {
                    yyjson_mut_arr_add_val(row, copy);
                } else {
                    yyjson_mut_arr_add_null(doc, row);
                }
            }
            if (pd) {
                yyjson_doc_free(pd);
            }
        }
        if (include_connected && sr->node.id > 0) {
            yyjson_mut_arr_add_val(row, enrich_connected(doc, store, sr->node.id, relationship));
        }
        yyjson_mut_arr_add_val(cur_rows, row);
    }
    yyjson_mut_obj_add_val(doc, root, "groups", groups);
    yyjson_mut_obj_add_bool(doc, root, "has_more", out->total > offset + out->count);
}

/* Emit semantic vector-search results as a TOON table. */
static __attribute__((unused)) void emit_semantic_results_toon(cbm_sb_t *sb, const cbm_vector_result_t *vresults,
                                       int vcount) {
    static const char *const cols[] = {"qn", "label", "file", "score"};
    cbm_tree_table_header(sb, "semantic", vcount, cols, 4);
    for (int v = 0; v < vcount; v++) {
        cbm_tree_row_begin(sb);
        cbm_tree_cell_str(sb, vresults[v].qualified_name, true);
        cbm_tree_cell_str(sb, vresults[v].label, false);
        cbm_tree_cell_str(sb, vresults[v].file_path, false);
        cbm_tree_cell_real(sb, vresults[v].score, false);
        cbm_tree_row_end(sb);
    }
}

/* Indexing-coverage report (#963), attached to index_status: the best-effort
 * signal from the separate index_coverage table (coverage is metadata ABOUT
 * the graph, stored outside it). Full per-project list, capped generously. */
enum { COVERAGE_FILE_CAP = 500 };

static __attribute__((unused)) void add_coverage_report(yyjson_mut_doc *doc, yyjson_mut_val *root, cbm_store_t *store,
                                const char *project) {
    cbm_coverage_row_t *rows = NULL;
    int count = 0;
    (void)cbm_store_coverage_get(store, project, &rows, &count);

    yyjson_mut_val *pp_files = yyjson_mut_arr(doc);
    yyjson_mut_val *sk_files = yyjson_mut_arr(doc);
    yyjson_mut_val *ni_dirs = yyjson_mut_arr(doc);
    yyjson_mut_val *ni_files = yyjson_mut_arr(doc);
    int pp_n = 0;
    int sk_n = 0;
    int ni_dir_n = 0;
    int ni_file_n = 0;
    for (int i = 0; i < count; i++) {
        const char *kind = rows[i].kind ? rows[i].kind : "";
        if (strcmp(kind, "parse_partial") == 0) {
            if (pp_n < COVERAGE_FILE_CAP) {
                yyjson_mut_val *fe = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, fe, "path", rows[i].rel_path);
                yyjson_mut_obj_add_strcpy(doc, fe, "error_ranges",
                                          rows[i].detail ? rows[i].detail : "");
                yyjson_mut_arr_add_val(pp_files, fe);
            }
            pp_n++;
        } else if (strcmp(kind, "not_indexed_dir") == 0) {
            if (ni_dir_n < COVERAGE_FILE_CAP) {
                yyjson_mut_arr_add_strcpy(doc, ni_dirs, rows[i].rel_path);
            }
            ni_dir_n++;
        } else if (strcmp(kind, "not_indexed_file") == 0) {
            if (ni_file_n < COVERAGE_FILE_CAP) {
                yyjson_mut_val *fe = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, fe, "path", rows[i].rel_path);
                yyjson_mut_obj_add_strcpy(doc, fe, "reason", rows[i].detail ? rows[i].detail : "");
                yyjson_mut_arr_add_val(ni_files, fe);
            }
            ni_file_n++;
        } else {
            if (sk_n < COVERAGE_FILE_CAP) {
                yyjson_mut_val *fe = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_strcpy(doc, fe, "path", rows[i].rel_path);
                yyjson_mut_obj_add_strcpy(doc, fe, "reason", rows[i].detail ? rows[i].detail : "");
                yyjson_mut_obj_add_strcpy(doc, fe, "phase", rows[i].kind ? rows[i].kind : "");
                yyjson_mut_arr_add_val(sk_files, fe);
            }
            sk_n++;
        }
    }
    cbm_store_free_coverage(rows, count);

    yyjson_mut_val *pp = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, pp, "files", pp_files);
    yyjson_mut_obj_add_int(doc, pp, "count", pp_n);
    yyjson_mut_obj_add_bool(doc, pp, "truncated", pp_n > COVERAGE_FILE_CAP);
    yyjson_mut_obj_add_val(doc, root, "parse_partial", pp);

    yyjson_mut_val *sk = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, sk, "files", sk_files);
    yyjson_mut_obj_add_int(doc, sk, "count", sk_n);
    yyjson_mut_obj_add_bool(doc, sk, "truncated", sk_n > COVERAGE_FILE_CAP);
    yyjson_mut_obj_add_val(doc, root, "skipped", sk);

    /* By-design exclusions (#963 "purposely not indexed"): a deliberate,
     * deterministic class — NOT a failure and NOT best-effort. Dirs are
     * exhaustive; per-file entries are capped in discovery (2000) with the
     * truncation explicit. */
    yyjson_mut_val *ni = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_val(doc, ni, "dirs", ni_dirs);
    yyjson_mut_obj_add_int(doc, ni, "dirs_count", ni_dir_n);
    yyjson_mut_obj_add_val(doc, ni, "files", ni_files);
    yyjson_mut_obj_add_int(doc, ni, "files_count", ni_file_n);
    yyjson_mut_obj_add_bool(doc, ni, "truncated",
                            ni_dir_n > COVERAGE_FILE_CAP || ni_file_n > COVERAGE_FILE_CAP);
    if (ni_dir_n > 0 || ni_file_n > 0) {
        yyjson_mut_obj_add_str(doc, ni, "note",
                               "Purposely not indexed — excluded BY DESIGN via "
                               "gitignore/.cbmignore/skip-lists (see each file's reason). Not an "
                               "error: change the ignore rules and re-index to include them.");
    }
    yyjson_mut_obj_add_val(doc, root, "not_indexed", ni);

    if (pp_n > 0 || sk_n > 0) {
        yyjson_mut_obj_add_str(
            doc, root, "coverage_note",
            "Best-effort signal, not a completeness guarantee: parse_partial files WERE indexed, "
            "but constructs inside the listed line ranges (1-based) MAY be missing from the graph "
            "(tree-sitter error recovery still salvages some). skipped files were not indexed at "
            "all. Prefer text search (grep) for flagged files/ranges. Files absent from this list "
            "are NOT guaranteed to be fully indexed. (not_indexed entries are a separate, "
            "BY-DESIGN class — deliberate ignore rules, not failures.)");
    }
}

enum {
    COVERAGE_PATH_MAX = 128,
    COVERAGE_SCOPE_MAX = 32,
    COVERAGE_SCOPE_DEFAULT_LIMIT = 200,
    COVERAGE_SCOPE_MAX_LIMIT = 1000,
    COVERAGE_RANGE_MAX = 128,
};

bool cbm_path_within_root(const char *root_path, const char *abs_path); /* defined below */

typedef enum {
    COVERAGE_PATH_OK = 0,
    COVERAGE_PATH_OUTSIDE,
    COVERAGE_PATH_INVALID,
} coverage_path_result_t;

/* Normalize an untrusted repository-relative path without touching the
 * filesystem. Absolute paths, drive/UNC paths, control bytes, and any `..`
 * component are rejected. A root scope (`.`) normalizes to the empty prefix. */
static __attribute__((unused)) coverage_path_result_t coverage_normalize_rel(const char *input, bool allow_root, char *out,
                                                     size_t out_size) {
    if (!input || !out || out_size == 0U) {
        return COVERAGE_PATH_INVALID;
    }
    out[0] = '\0';
    size_t len = strlen(input);
    if (len == 0U || len >= out_size || input[0] == '/' || input[0] == '\\' ||
        (len >= 2U && isalpha((unsigned char)input[0]) && input[1] == ':')) {
        return COVERAGE_PATH_OUTSIDE;
    }

    size_t in = 0U;
    size_t written = 0U;
    while (in < len) {
        while (in < len && (input[in] == '/' || input[in] == '\\')) {
            in++;
        }
        if (in >= len) {
            break;
        }
        size_t start = in;
        while (in < len && input[in] != '/' && input[in] != '\\') {
            unsigned char c = (unsigned char)input[in];
            if (c < 0x20U) {
                return COVERAGE_PATH_INVALID;
            }
            in++;
        }
        size_t part_len = in - start;
        if (part_len == 1U && input[start] == '.') {
            continue;
        }
        if (part_len == 2U && input[start] == '.' && input[start + 1U] == '.') {
            return COVERAGE_PATH_OUTSIDE;
        }
        if (written > 0U) {
            if (written + 1U >= out_size) {
                return COVERAGE_PATH_INVALID;
            }
            out[written++] = '/';
        }
        if (written + part_len >= out_size) {
            return COVERAGE_PATH_INVALID;
        }
        memcpy(out + written, input + start, part_len);
        written += part_len;
    }
    out[written] = '\0';
    return written > 0U || allow_root ? COVERAGE_PATH_OK : COVERAGE_PATH_INVALID;
}

static int64_t coverage_stat_mtime_ns(const struct stat *st) {
#ifdef __APPLE__
    return ((int64_t)st->st_mtimespec.tv_sec * (int64_t)CBM_NSEC_PER_SEC) +
           (int64_t)st->st_mtimespec.tv_nsec;
#elif defined(_WIN32)
    return (int64_t)st->st_mtime * (int64_t)CBM_NSEC_PER_SEC;
#else
    return ((int64_t)st->st_mtim.tv_sec * (int64_t)CBM_NSEC_PER_SEC) + (int64_t)st->st_mtim.tv_nsec;
#endif
}

static __attribute__((unused)) const char *coverage_path_freshness(cbm_store_t *store, const char *project,
                                           const char *root_path, const char *rel_path,
                                           bool *outside) {
    *outside = false;
    if (!root_path || !root_path[0]) {
        return "unavailable";
    }
    char abs_path[CBM_SZ_4K];
    int n = snprintf(abs_path, sizeof(abs_path), "%s%s%s", root_path,
                     root_path[strlen(root_path) - 1U] == '/' ? "" : "/", rel_path);
    if (n < 0 || (size_t)n >= sizeof(abs_path)) {
        return "unavailable";
    }
    struct stat st;
    if (stat(abs_path, &st) != 0) {
        return "missing";
    }
    if (!cbm_path_within_root(root_path, abs_path)) {
        *outside = true;
        return "outside_project";
    }

    cbm_file_hash_t hash = {0};
    int rc = cbm_store_get_file_hash(store, project, rel_path, &hash);
    if (rc == CBM_STORE_NOT_FOUND) {
        return "not_tracked";
    }
    if (rc != CBM_STORE_OK) {
        return "unavailable";
    }
    bool matches = hash.mtime_ns == coverage_stat_mtime_ns(&st) && hash.size == st.st_size;
    cbm_store_clear_file_hash(&hash);
    return matches ? "metadata_match" : "metadata_changed";
}

static void coverage_add_ranges(yyjson_mut_doc *doc, yyjson_mut_val *row, const char *detail) {
    if (!detail || !detail[0]) {
        return;
    }
    yyjson_mut_val *ranges = yyjson_mut_arr(doc);
    const char *p = detail;
    int emitted = 0;
    while (*p && emitted < COVERAGE_RANGE_MAX) {
        while (*p == ' ' || *p == ',') {
            p++;
        }
        if (!isdigit((unsigned char)*p)) {
            break;
        }
        char *endptr = NULL;
        long start = strtol(p, &endptr, 10);
        if (endptr == p || start <= 0 || start > INT32_MAX) {
            break;
        }
        p = endptr;
        long end = start;
        if (*p == '-') {
            p++;
            long parsed = strtol(p, &endptr, 10);
            if (endptr == p || parsed < start || parsed > INT32_MAX) {
                break;
            }
            end = parsed;
            p = endptr;
        }
        yyjson_mut_val *range = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_int(doc, range, "start", start);
        yyjson_mut_obj_add_int(doc, range, "end", end);
        yyjson_mut_arr_add_val(ranges, range);
        emitted++;
        while (*p == ' ') {
            p++;
        }
        if (*p && *p != ',') {
            break;
        }
    }
    if (emitted > 0) {
        yyjson_mut_obj_add_val(doc, row, "ranges", ranges);
    }
}

static __attribute__((unused)) void coverage_add_row_json(yyjson_mut_doc *doc, yyjson_mut_val *array,
                                  const cbm_coverage_row_t *row, const char *requested_path) {
    yyjson_mut_val *item = yyjson_mut_obj(doc);
    yyjson_mut_obj_add_strcpy(doc, item, "path", row->rel_path ? row->rel_path : "");
    yyjson_mut_obj_add_strcpy(doc, item, "kind", row->kind ? row->kind : "");
    yyjson_mut_obj_add_strcpy(doc, item, "detail", row->detail ? row->detail : "");
    if (requested_path) {
        yyjson_mut_obj_add_str(
            doc, item, "match",
            row->rel_path && strcmp(row->rel_path, requested_path) == 0 ? "exact" : "ancestor");
    }
    if (row->kind && strcmp(row->kind, "parse_partial") == 0) {
        coverage_add_ranges(doc, item, row->detail);
    }
    yyjson_mut_arr_add_val(array, item);
}

static __attribute__((unused)) const char *coverage_status(const cbm_coverage_row_t *rows, int count,
                                   const char *requested_path, const char *recording_status,
                                   bool generation_matches, bool lookup_ok,
                                   bool exact_path_verified) {
    if (!lookup_ok) {
        return "coverage_unavailable";
    }
    bool exact = false;
    for (int i = 0; i < count; i++) {
        if (rows[i].rel_path && strcmp(rows[i].rel_path, requested_path) == 0) {
            exact = true;
            break;
        }
    }
    for (int pass = 0; pass < 3; pass++) {
        for (int i = 0; i < count; i++) {
            if (exact && (!rows[i].rel_path || strcmp(rows[i].rel_path, requested_path) != 0)) {
                continue;
            }
            const char *kind = rows[i].kind ? rows[i].kind : "";
            if (pass == 0 && strcmp(kind, "parse_partial") == 0) {
                return "partial";
            }
            if (pass == 1 && strncmp(kind, "not_indexed", 11) == 0) {
                return "excluded";
            }
            if (pass == 2 && kind[0]) {
                return "skipped";
            }
        }
    }
    bool recording_complete = recording_status && strcmp(recording_status, "complete") == 0;
    bool truncated_exact_path_verified =
        exact_path_verified && recording_status && strcmp(recording_status, "truncated") == 0;
    if (!generation_matches || (!recording_complete && !truncated_exact_path_verified)) {
        return "coverage_unavailable";
    }
    return "no_recorded_issue";
}

static __attribute__((unused)) const char *coverage_recommended_action(const char *status, const char *freshness) {
    if (!freshness || strcmp(freshness, "metadata_match") != 0) {
        return "read_source_and_reindex";
    }
    if (strcmp(status, "partial") == 0) {
        return "read_ranges_and_verify_scope";
    }
    if (strcmp(status, "skipped") == 0) {
        return "read_source_directly";
    }
    if (strcmp(status, "excluded") == 0) {
        return "read_source_or_change_ignore_rules";
    }
    if (strcmp(status, "no_recorded_issue") == 0) {
        return "use_graph_with_best_effort_caveat";
    }
    return "read_source_and_reindex";
}

/* delete_project: just erase the .db file (and WAL/SHM). */
static char *handle_delete_project(cbm_mcp_server_t *srv, const char *args) {
    char *name = get_project_arg(args);
    if (!name) {
        return cbm_mcp_text_result("project is required", true);
    }
    if (!mcp_project_mutation_begin(srv, name)) {
        free(name);
        return cbm_mcp_text_result("project operation cancelled or blocked by an active index",
                                   true);
    }

    /* Close store if it's the project being deleted */
    if (srv->current_project && strcmp(srv->current_project, name) == 0) {
        if (srv->owns_store && srv->store) {
            cbm_store_close(srv->store);
            srv->store = NULL;
        }
        free(srv->current_project);
        srv->current_project = NULL;
    }

    /* Wait for any in-progress pipeline to finish before deleting */
    cbm_pipeline_lock();

    /* Delete the .db file + WAL/SHM */
    char path[CBM_SZ_1K];
    project_db_path(name, path, sizeof(path));

    char wal[CBM_SZ_1K];
    char shm[CBM_SZ_1K];
    snprintf(wal, sizeof(wal), "%s-wal", path);
    snprintf(shm, sizeof(shm), "%s-shm", path);

    bool exists = (access(path, F_OK) == 0);
    const char *status = "not_found";
    const char *error_detail = NULL;
    bool is_error = false;

    if (exists) {
        int rc = cbm_unlink(path);
        (void)cbm_unlink(wal);
        (void)cbm_unlink(shm);
        if (rc == 0) {
            status = "deleted";
        } else {
            status = "delete_failed";
            error_detail = strerror(errno);
            is_error = true;
        }
    } else {
        is_error = true;
    }

    cbm_pipeline_unlock();

    if (srv->watcher) {
        cbm_watcher_unwatch(srv->watcher, name);
    }

    cbm_mem_collect(); /* return freed pages to OS after closing database */
    mcp_project_mutation_end(srv, name);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "project", name);
    yyjson_mut_obj_add_str(doc, root, "status", status);
    if (error_detail) {
        yyjson_mut_obj_add_str(doc, root, "error", error_detail);
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(name);

    char *result = cbm_mcp_text_result(json, is_error);
    free(json);
    return result;
}

/* Resolve edge types from args: explicit array > mode-based > default ("CALLS").
 * Writes types into out_types (max 16). Returns the parsed yyjson_doc if explicit
 * edge_types were found (caller must keep alive until types are consumed), or NULL. */
static __attribute__((unused)) yyjson_doc *resolve_trace_edge_types(const char *args, const char *mode,
                                            const char **out_types, int *out_count) {
    static const char *mode_calls[] = {"CALLS"};
    static const char *mode_data_flow[] = {"CALLS", "DATA_FLOWS"};
    static const char *mode_cross_svc[] = {
        "HTTP_CALLS",          "ASYNC_CALLS",       "DATA_FLOWS",    "CALLS",
        "CROSS_HTTP_CALLS",    "CROSS_ASYNC_CALLS", "CROSS_CHANNEL", "CROSS_GRPC_CALLS",
        "CROSS_GRAPHQL_CALLS", "CROSS_TRPC_CALLS"};

    *out_count = 0;

    yyjson_doc *et_doc = yyjson_read(args, strlen(args), 0);
    if (et_doc) {
        yyjson_val *et_arr = yyjson_obj_get(yyjson_doc_get_root(et_doc), "edge_types");
        if (et_arr && yyjson_is_arr(et_arr)) {
            size_t idx2;
            size_t max2;
            yyjson_val *val2;
            yyjson_arr_foreach(et_arr, idx2, max2, val2) {
                if (yyjson_is_str(val2) && *out_count < MCP_COL_16) {
                    out_types[(*out_count)++] = yyjson_get_str(val2);
                }
            }
        }
    }

    if (*out_count > 0) {
        return et_doc; /* caller must keep alive — pointers reference doc memory */
    }

    yyjson_doc_free(et_doc); /* no explicit types found, free */

    const char **defaults = mode_calls;
    int n_defaults = SKIP_ONE;
    if (mode && strcmp(mode, "data_flow") == 0) {
        defaults = mode_data_flow;
        n_defaults = MCP_N_DEFAULTS_2;
    } else if (mode && strcmp(mode, "cross_service") == 0) {
        defaults = mode_cross_svc;
        n_defaults = (int)(sizeof(mode_cross_svc) / sizeof(mode_cross_svc[0]));
    }
    for (int i = 0; i < n_defaults; i++) {
        out_types[i] = defaults[i];
    }
    *out_count = n_defaults;
    return NULL;
}

/* Check if a file path looks like a test file. The substring checks below
 * only catch a tests/ directory nested under another path component
 * (".../tests/foo"); a project-root-relative path like "tests/repro/foo.c"
 * has no leading slash before "tests" and fell through undetected, leaking
 * whole test subtrees into query_graph/trace_path results with the default
 * include_tests=false (#1294). */
static bool is_test_file(const char *path) {
    if (!path) {
        return false;
    }
    return strstr(path, "/test") != NULL || strstr(path, "test_") != NULL ||
           strstr(path, "_test.") != NULL || strstr(path, "/tests/") != NULL ||
           strstr(path, "/spec/") != NULL || strstr(path, ".test.") != NULL ||
           strncmp(path, "tests/", SLEN("tests/")) == 0 ||
           strncmp(path, "test/", SLEN("test/")) == 0 ||
           strncmp(path, "spec/", SLEN("spec/")) == 0 ||
           strncmp(path, "__tests__/", SLEN("__tests__/")) == 0;
}

/* Convert BFS traversal results into a yyjson_mut array. */
/* Find the CALLS-edge "args" JSON (the serialized arg expressions) on the edge
 * that leads to the given hop node, so data_flow mode can surface argument
 * expressions (#514). Returns the borrowed substring "[...]" inside the edge's
 * properties_json, with its length, or NULL when no args are recorded. */
static const char *bfs_edge_args_for_hop(cbm_traverse_result_t *tr, int64_t hop_node_id,
                                         size_t *out_len) {
    for (int e = 0; e < tr->edge_count; e++) {
        /* The hop node is the edge endpoint reached from the root side: for an
         * outbound trace it is the target, for inbound it is the source. Match
         * on either so both directions surface their args. */
        if (tr->edges[e].target_id != hop_node_id && tr->edges[e].source_id != hop_node_id) {
            continue;
        }
        const char *pj = tr->edges[e].properties_json;
        if (!pj) {
            continue;
        }
        const char *args = strstr(pj, "\"args\"");
        if (!args) {
            continue;
        }
        const char *open = strchr(args, '[');
        if (!open) {
            continue;
        }
        int depth = 0;
        const char *p = open;
        for (; *p; p++) {
            if (*p == '[') {
                depth++;
            } else if (*p == ']') {
                depth--;
                if (depth == 0) {
                    p++;
                    break;
                }
            }
        }
        *out_len = (size_t)(p - open);
        return open;
    }
    return NULL;
}

/* Classify a resolver strategy into the CLOSED public vocabulary.
 *
 * The indexer records ~20 internal strategy names on CALLS edges
 * (lsp_trait_dispatch, php_self_static, callee_suffix, ...) and the set grows
 * with every language. Publishing those verbatim would make each internal
 * resolver name public API by accident, so a rename would silently change a
 * user-visible field. We publish the CLASS instead: adding lsp_foo_dispatch
 * maps automatically, while a genuinely new KIND of resolution fails the
 * pinning test in tests/test_mcp.c and forces a deliberate decision.
 *
 * Returns NULL only for a NULL/empty strategy — every non-empty value lands in
 * a class, so an unmapped strategy can never silently vanish from output. */
const char *cbm_mcp_edge_strategy_class(const char *strategy) {
    if (!strategy || !strategy[0]) {
        return NULL;
    }
    /* Order matters: lsp_unresolved is an LSP strategy that failed, and the
     * caller cares that it did NOT resolve — so it classifies as unresolved. */
    if (strcmp(strategy, "lsp_unresolved") == 0 || strcmp(strategy, "unknown") == 0) {
        return "unresolved";
    }
    if (strncmp(strategy, "lsp_", 4) == 0) {
        return "lsp";
    }
    if (strncmp(strategy, "php_", 4) == 0 || strncmp(strategy, "perl_", 5) == 0) {
        return "language_rule";
    }
    /* Everything else is a name/shape heuristic: callee_suffix,
     * field_type_hint, service_pattern, fastapi_depends, unique_name, ... */
    return "heuristic";
}

/* Find the resolution evidence on the edge that leads to the given hop node —
 * the same endpoint-matching rule as bfs_edge_args_for_hop (target for an
 * outbound trace, source for inbound, so both directions surface it).
 *
 * Reads only from tr->edges, and callers pass the PAGINATED view, so evidence
 * is emitted for the rows on this page and nothing else.
 *
 * Returns false when the edge carries no strategy (non-CALLS edges do not). */
static bool bfs_edge_evidence_for_hop(cbm_traverse_result_t *tr, int64_t hop_node_id,
                                      const char **class_out, double *confidence_out) {
    for (int e = 0; e < tr->edge_count; e++) {
        if (tr->edges[e].target_id != hop_node_id && tr->edges[e].source_id != hop_node_id) {
            continue;
        }
        const char *pj = tr->edges[e].properties_json;
        if (!pj) {
            continue;
        }
        const char *key = strstr(pj, "\"strategy\"");
        if (!key) {
            continue;
        }
        const char *open = strchr(key + 10, '"');
        if (!open) {
            continue;
        }
        open++;
        const char *close = strchr(open, '"');
        if (!close || close == open) {
            continue;
        }
        char raw[CBM_SZ_64];
        size_t len = (size_t)(close - open);
        if (len >= sizeof(raw)) {
            len = sizeof(raw) - 1;
        }
        memcpy(raw, open, len);
        raw[len] = '\0';
        const char *cls = cbm_mcp_edge_strategy_class(raw);
        if (!cls) {
            continue;
        }
        *class_out = cls;
        /* Confidence rides on the same edge; absent is reported as -1 so a
         * genuine 0.0 stays distinguishable from "not recorded". */
        *confidence_out = -1.0;
        const char *conf = strstr(pj, "\"confidence\"");
        if (conf) {
            const char *colon = strchr(conf, ':');
            if (colon) {
                *confidence_out = strtod(colon + 1, NULL);
            }
        }
        return true;
    }
    return false;
}

/* TOON table for one trace direction: callees[N]{qn,hop,...} with optional
 * risk / test / args columns. `name` is omitted (it is the qn's last
 * segment); the per-item JSON key envelope was 84% of the legacy payload. */
static __attribute__((unused)) void bfs_to_toon_table(cbm_sb_t *sb, const char *key, cbm_traverse_result_t *tr,
                              bool risk_labels, bool include_tests, bool data_flow) {
    int visible = 0;
    for (int i = 0; i < tr->visited_count; i++) {
        if (!include_tests && is_test_file(tr->visited[i].node.file_path)) {
            continue;
        }
        visible++;
    }
    const char *cols[5] = {"qn", "hop"};
    int ncols = 2;
    if (risk_labels) {
        cols[ncols++] = "risk";
    }
    if (include_tests) {
        cols[ncols++] = "test";
    }
    if (data_flow) {
        cols[ncols++] = "args";
    }
    cbm_tree_table_header(sb, key, visible, cols, ncols);
    for (int i = 0; i < tr->visited_count; i++) {
        const char *fp = tr->visited[i].node.file_path;
        bool test = is_test_file(fp);
        if (!include_tests && test) {
            continue;
        }
        cbm_tree_row_begin(sb);
        cbm_tree_cell_str(sb, tr->visited[i].node.qualified_name, true);
        cbm_tree_cell_int(sb, tr->visited[i].hop, false);
        if (risk_labels) {
            cbm_tree_cell_str(sb, cbm_risk_label(cbm_hop_to_risk(tr->visited[i].hop)), false);
        }
        if (include_tests) {
            cbm_tree_cell_bool(sb, test, false);
        }
        if (data_flow) {
            size_t alen = 0;
            const char *ea = bfs_edge_args_for_hop(tr, tr->visited[i].node.id, &alen);
            if (ea && alen > 0 && alen < CBM_SZ_1K) {
                char abuf[CBM_SZ_1K];
                memcpy(abuf, ea, alen);
                abuf[alen] = '\0';
                cbm_tree_cell_str(sb, abuf, false);
            } else {
                cbm_tree_cell_str(sb, "", false);
            }
        }
        cbm_tree_row_end(sb);
    }
}

static char *snippet_suggestions(const char *input, cbm_node_t *nodes, int count);

/* Rank a candidate for name resolution. The label tier (callable > class-like >
 * module/file) is the primary key; WITHIN a tier the larger definition by line
 * span wins. In practice the .c-over-.h and C-main-over-shell-main preferences
 * come primarily from span (the real definition has the larger body), since the
 * competing matches usually share a tier — no file extension is hardcoded.
 * Consequence: two same-tier candidates with equal span tie and are reported
 * ambiguous (see pick_resolved_node) rather than guessed. */
enum {
    RES_RANK_CALLABLE = 2,     /* Function / Method */
    RES_RANK_OTHER = 1,        /* Class / Struct / etc. */
    RES_RANK_MODULE = 0,       /* Module / File */
    RES_LABEL_WEIGHT = 1000000 /* label tier dominates span */
};
static long node_resolution_score(const cbm_node_t *n) {
    long label_rank = RES_RANK_MODULE;
    if (n->label) {
        if (strcmp(n->label, "Function") == 0 || strcmp(n->label, "Method") == 0) {
            label_rank = RES_RANK_CALLABLE;
        } else if (strcmp(n->label, "Module") != 0 && strcmp(n->label, "File") != 0) {
            label_rank = RES_RANK_OTHER;
        }
    }
    long span = (long)n->end_line - (long)n->start_line;
    if (span < 0) {
        span = 0;
    }
    return label_rank * (long)RES_LABEL_WEIGHT + span;
}

/* A "real" callable definition: a Function/Method node with a non-empty body
 * span (end_line > start_line). A body-less node (start_line == end_line) is an
 * ambient declaration / signature stub — e.g. a TypeScript `.d.ts` declaration
 * — which is a *fragment* of one logical symbol, not a distinct definition. The
 * distinction lets pick_resolved_node union a stub with its real implementation
 * (#546) while still treating two genuinely-different same-named functions as
 * ambiguous rather than conflating their caller sets. */
static bool node_is_real_callable_def(const cbm_node_t *n) {
    if (!n->label) {
        return false;
    }
    if (strcmp(n->label, "Function") != 0 && strcmp(n->label, "Method") != 0) {
        return false;
    }
    return (long)n->end_line - (long)n->start_line > 0;
}

/* Pick the best-resolving node among name matches. Sets *ambiguous when the
 * matches can't be reduced to one logical symbol, so resolution never silently
 * traces (or conflates) the wrong same-named node:
 *   1. the top score is shared by >1 candidate (a genuine rank/span tie), or
 *   2. two or more *real* callable definitions share the name — distinct
 *      implementations, not a definition plus its body-less stub(s).
 * Rule 2 completes rule 1: without it, two same-named functions whose bodies
 * differ in length score differently, dodge the tie, and get their caller sets
 * unioned by bfs_union_same_name (#546) into one confidently-conflated answer.
 * Body-less .d.ts stubs still union with their implementation (#650). */
static __attribute__((unused)) int pick_resolved_node(const cbm_node_t *nodes, int count, bool *ambiguous) {
    *ambiguous = false;
    if (count <= 1) {
        return 0;
    }
    int best = 0;
    long best_score = node_resolution_score(&nodes[0]);
    for (int i = 1; i < count; i++) {
        long s = node_resolution_score(&nodes[i]);
        if (s > best_score) {
            best_score = s;
            best = i;
        }
    }
    int top_count = 0;
    int real_def_count = 0;
    for (int i = 0; i < count; i++) {
        if (node_resolution_score(&nodes[i]) == best_score) {
            top_count++;
        }
        if (node_is_real_callable_def(&nodes[i])) {
            real_def_count++;
        }
    }
    if (real_def_count > 1) {
        *ambiguous = true;
    }
    if (top_count > 1) {
        *ambiguous = true;
    }
    return best;
}

static int node_hop_cmp_hop_id(const void *pa, const void *pb) {
    const cbm_node_hop_t *a = (const cbm_node_hop_t *)pa;
    const cbm_node_hop_t *b = (const cbm_node_hop_t *)pb;
    if (a->hop != b->hop) {
        return a->hop < b->hop ? -1 : 1;
    }
    if (a->node.id != b->node.id) {
        return a->node.id < b->node.id ? -1 : 1;
    }
    return 0;
}

/* BFS from EVERY node sharing the resolved name and merge the results, so the
 * caller/callee set is complete even when one logical symbol is represented by
 * more than one graph node — e.g. a real .ts implementation plus an ambient
 * .d.ts stub, whose inbound CALLS edges are otherwise split across the two
 * nodes and silently truncated by tracing only one (#546). visited hops are
 * deduped by node id; edges are concatenated. Ownership of all heap fields
 * transfers into *out, freed by cbm_store_traverse_free. */
static __attribute__((unused)) void bfs_union_same_name(cbm_store_t *store, const cbm_node_t *nodes, int node_count,
                                const char *direction, const char **edge_types, int edge_type_count,
                                int depth, int limit, cbm_traverse_result_t *out) {
    memset(out, 0, sizeof(*out));
    int vcap = 0, ecap = 0;
    for (int k = 0; k < node_count; k++) {
        cbm_traverse_result_t tr = {0};
        cbm_store_bfs(store, nodes[k].id, direction, edge_types, edge_type_count, depth, limit,
                      &tr);
        for (int i = 0; i < tr.visited_count; i++) {
            bool dup = false;
            for (int j = 0; j < out->visited_count; j++) {
                if (out->visited[j].node.id == tr.visited[i].node.id) {
                    /* Min-hop across seeds: keep-first recorded the EARLIER
                     * seed's (possibly longer) distance; hop feeds risk_labels
                     * and pagination watermarks, so it must match the
                     * single-BFS MIN(hop) semantics (#797). */
                    if (tr.visited[i].hop < out->visited[j].hop) {
                        out->visited[j].hop = tr.visited[i].hop;
                    }
                    dup = true;
                    break;
                }
            }
            if (dup) {
                continue;
            }
            if (out->visited_count >= vcap) {
                vcap = vcap ? vcap * 2 : 8;
                out->visited = safe_realloc(out->visited, vcap * sizeof(cbm_node_hop_t));
            }
            out->visited[out->visited_count++] = tr.visited[i];
            memset(&tr.visited[i], 0, sizeof(tr.visited[i])); /* ownership moved */
        }
        for (int i = 0; i < tr.edge_count; i++) {
            /* Overlapping seed neighborhoods yield the same edge from more
             * than one BFS — dedup by (source, target, type). */
            bool edup = false;
            for (int j = 0; j < out->edge_count; j++) {
                if (out->edges[j].source_id == tr.edges[i].source_id &&
                    out->edges[j].target_id == tr.edges[i].target_id && out->edges[j].type &&
                    tr.edges[i].type && strcmp(out->edges[j].type, tr.edges[i].type) == 0) {
                    edup = true;
                    break;
                }
            }
            if (edup) {
                continue;
            }
            if (out->edge_count >= ecap) {
                ecap = ecap ? ecap * 2 : 8;
                out->edges = safe_realloc(out->edges, ecap * sizeof(cbm_edge_info_t));
            }
            out->edges[out->edge_count++] = tr.edges[i];
            memset(&tr.edges[i], 0, sizeof(tr.edges[i])); /* ownership moved */
        }
        cbm_store_traverse_free(&tr); /* frees only the un-moved (root + dup) fields */
    }
    /* Canonical (hop, id) order — a pure function of the graph, independent of
     * seed iteration order; required for deterministic output and watermarks. */
    if (out->visited_count > 1) {
        qsort(out->visited, (size_t)out->visited_count, sizeof(cbm_node_hop_t),
              node_hop_cmp_hop_id);
    }
}

/* ── Pagination cursors (stateless, exactly-once) ────────────────────
 * Token: "c1.<leg>.<generation>.<qhash>.<hop>.<id>" — version, trace leg
 * (o=callees, i=callers), the store generation (per-DB uid + mutation
 * counter), an FNV-1a-64 hash of the canonical query params, and the
 * (hop, node_id) watermark of the last emitted row in canonical order.
 * Stateless by design: the server re-traverses (the recursive CTE pays the
 * full reachable-set cost regardless of LIMIT, so a page costs what one
 * call costs today) and skips to the watermark. The generation stamp turns
 * every post-reindex cursor into a loud, actionable error — node ids are
 * never reused across rebuilds, so silently resuming would be wrong. */

static uint64_t cursor_fnv1a64(const char *s, uint64_t h) {
    while (s && *s) {
        h ^= (uint64_t)(unsigned char)*s++;
        h *= 0x100000001b3ULL;
    }
    return h;
}

typedef struct {
    char leg;            /* 'o' callees, 'i' callers */
    char generation[96]; /* store generation at mint time */
    uint64_t qhash;      /* canonical-params hash */
    int hop;             /* watermark: last emitted row */
    int64_t node_id;
} trace_cursor_t;

/* Hash the params that define the traversal identity. A cursor replayed with
 * different params must fail loudly, never silently mis-skip. */
static __attribute__((unused)) uint64_t trace_params_hash(const char *project, const char *func_name, const char *direction,
                                  const char *mode, int depth, bool include_tests, int limit) {
    uint64_t h = 0xcbf29ce484222325ULL;
    h = cursor_fnv1a64(project ? project : "", h);
    h = cursor_fnv1a64("|", h);
    h = cursor_fnv1a64(func_name ? func_name : "", h);
    h = cursor_fnv1a64("|", h);
    h = cursor_fnv1a64(direction ? direction : "", h);
    h = cursor_fnv1a64("|", h);
    h = cursor_fnv1a64(mode ? mode : "", h);
    char nums[64];
    snprintf(nums, sizeof(nums), "|%d|%d|%d", depth, include_tests ? 1 : 0, limit);
    h = cursor_fnv1a64(nums, h);
    return h;
}

static __attribute__((unused)) void trace_cursor_encode(const trace_cursor_t *c, char *buf, size_t bufsz) {
    snprintf(buf, bufsz, "c1.%c.%s.%016llx.%d.%lld", c->leg, c->generation,
             (unsigned long long)c->qhash, c->hop, (long long)c->node_id);
}

/* Decode + validate. Returns NULL on success, else a static teaching error. */
static __attribute__((unused)) const char *trace_cursor_decode(const char *token, const char *current_generation,
                                       uint64_t expected_qhash, trace_cursor_t *out) {
    memset(out, 0, sizeof(*out));
    if (!token || strncmp(token, "c1.", 3) != 0) {
        return "invalid_cursor: unrecognized token — re-run the original query without 'cursor'";
    }
    const char *p = token + 3;
    if (*p != 'o' && *p != 'i') {
        return "invalid_cursor: unrecognized token — re-run the original query without 'cursor'";
    }
    out->leg = *p;
    p += 2; /* leg + '.' */
    const char *gen_end = strchr(p, '.');
    if (!gen_end || (size_t)(gen_end - p) >= sizeof(out->generation)) {
        return "invalid_cursor: unrecognized token — re-run the original query without 'cursor'";
    }
    memcpy(out->generation, p, (size_t)(gen_end - p));
    out->generation[gen_end - p] = '\0';
    unsigned long long qh = 0;
    long long nid = 0;
    if (sscanf(gen_end + 1, "%16llx.%d.%lld", &qh, &out->hop, &nid) != 3) {
        return "invalid_cursor: unrecognized token — re-run the original query without 'cursor'";
    }
    out->qhash = qh;
    out->node_id = nid;
    if (out->qhash != expected_qhash) {
        return "cursor_params_mismatch: this cursor was issued for different arguments — "
               "pass the cursor back with ALL other arguments identical";
    }
    if (strcmp(out->generation, current_generation) != 0) {
        return "stale_cursor: the project was reindexed since this cursor was issued — "
               "re-run the original query without 'cursor' (node identities changed)";
    }
    return NULL;
}

/* Slice a canonically-ordered traversal at the watermark: index of the first
 * row strictly AFTER (hop, id). */
static __attribute__((unused)) int trace_watermark_index(const cbm_traverse_result_t *tr, int hop, int64_t node_id) {
    for (int i = 0; i < tr->visited_count; i++) {
        if (tr->visited[i].hop > hop ||
            (tr->visited[i].hop == hop && tr->visited[i].node.id > node_id)) {
            return i;
        }
    }
    return tr->visited_count;
}

/* json-stringified tree for one trace leg: same grouped model as the text
 * output — {cols, groups:[{qn_prefix, rows:[[name,hop,...]]}]}. Optional
 * risk/args columns mirror the flags. */
static __attribute__((unused)) yyjson_mut_val *bfs_to_tree_json(yyjson_mut_doc *doc, cbm_traverse_result_t *tr,
                                        bool risk_labels, bool include_tests, bool data_flow,
                                        bool include_evidence) {
    yyjson_mut_val *leg = yyjson_mut_obj(doc);
    yyjson_mut_val *cols = yyjson_mut_arr(doc);
    yyjson_mut_arr_add_str(doc, cols, "name");
    yyjson_mut_arr_add_str(doc, cols, "hop");
    if (risk_labels) {
        yyjson_mut_arr_add_str(doc, cols, "risk");
    }
    /* #1542: include_evidence was implemented on the tree path only, so
     * format:"json" silently returned cols ["name","hop"] while the schema and
     * --help promised two more. A structured caller — the one most likely to
     * ask for json — got no error, just missing fields. */
    if (include_evidence) {
        yyjson_mut_arr_add_str(doc, cols, "strategy");
        yyjson_mut_arr_add_str(doc, cols, "confidence");
    }
    if (data_flow) {
        yyjson_mut_arr_add_str(doc, cols, "args");
    }
    yyjson_mut_obj_add_val(doc, leg, "cols", cols);
    yyjson_mut_val *groups = yyjson_mut_arr(doc);
    yyjson_mut_val *cur_rows = NULL;
    char cur_group[CBM_SZ_1K] = "";
    bool have_group = false;
    for (int i = 0; i < tr->visited_count; i++) {
        if (!include_tests && is_test_file(tr->visited[i].node.file_path)) {
            continue;
        }
        const char *qn =
            tr->visited[i].node.qualified_name ? tr->visited[i].node.qualified_name : "";
        size_t plen = sg_qn_prefix_len(qn);
        if (plen >= sizeof(cur_group)) {
            plen = 0;
        }
        if (!have_group || strncmp(cur_group, qn, plen) != 0 || cur_group[plen] != '\0') {
            snprintf(cur_group, sizeof(cur_group), "%.*s", (int)plen, qn);
            have_group = true;
            yyjson_mut_val *g = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_strcpy(doc, g, "qn_prefix", cur_group);
            cur_rows = yyjson_mut_arr(doc);
            yyjson_mut_obj_add_val(doc, g, "rows", cur_rows);
            yyjson_mut_arr_add_val(groups, g);
        }
        yyjson_mut_val *row = yyjson_mut_arr(doc);
        yyjson_mut_arr_add_strcpy(doc, row, plen ? qn + plen + 1 : qn);
        yyjson_mut_arr_add_int(doc, row, tr->visited[i].hop);
        if (risk_labels) {
            yyjson_mut_arr_add_str(doc, row, cbm_risk_label(cbm_hop_to_risk(tr->visited[i].hop)));
        }
        if (data_flow) {
            size_t alen = 0;
            const char *ea = bfs_edge_args_for_hop(tr, tr->visited[i].node.id, &alen);
            if (ea && alen > 0) {
                yyjson_mut_val *av = yyjson_mut_rawn(doc, ea, alen);
                if (av) {
                    yyjson_mut_arr_add_val(row, av);
                } else {
                    yyjson_mut_arr_add_str(doc, row, "");
                }
            } else {
                yyjson_mut_arr_add_str(doc, row, "");
            }
        }
        if (include_evidence) {
            const char *ev_class = NULL;
            double ev_conf = -1.0;
            if (bfs_edge_evidence_for_hop(tr, tr->visited[i].node.id, &ev_class, &ev_conf)) {
                yyjson_mut_arr_add_strcpy(doc, row, ev_class ? ev_class : "");
                if (ev_conf >= 0.0) {
                    yyjson_mut_arr_add_real(doc, row, ev_conf);
                } else {
                    yyjson_mut_arr_add_null(doc, row);
                }
            } else {
                /* The root hop has no inbound edge and non-CALLS edges record
                 * no strategy. The tree path emits "-" placeholders to keep the
                 * column count fixed; json says null, which is the same promise
                 * in a form a structured caller can test. */
                yyjson_mut_arr_add_null(doc, row);
                yyjson_mut_arr_add_null(doc, row);
            }
        }
        yyjson_mut_arr_add_val(cur_rows, row);
    }
    yyjson_mut_obj_add_val(doc, leg, "groups", groups);
    return leg;
}

/* Tree-format trace leg: rows grouped by qn-prefix (printed once), each row
 * `name hop` — same data as the TOON table, prefix-factored. Test-file rows
 * honor include_tests exactly like bfs_to_toon_table. Rows arrive in
 * canonical (hop,id) order; grouping re-sorts by (prefix, hop, id) so
 * same-module rows are adjacent (Lost-in-Distance) while hop stays visible. */
static int tree_hop_cmp_qn(const void *pa, const void *pb) {
    const cbm_node_hop_t *a = (const cbm_node_hop_t *)pa;
    const cbm_node_hop_t *b = (const cbm_node_hop_t *)pb;
    const char *qa = a->node.qualified_name ? a->node.qualified_name : "";
    const char *qb = b->node.qualified_name ? b->node.qualified_name : "";
    int c = strcmp(qa, qb);
    if (c != 0) {
        return c;
    }
    return a->hop - b->hop;
}

static __attribute__((unused)) void bfs_to_tree_table(cbm_sb_t *sb, const char *key, cbm_traverse_result_t *tr,
                              bool include_tests, bool include_evidence) {
    int visible = 0;
    for (int i = 0; i < tr->visited_count; i++) {
        if (!include_tests && is_test_file(tr->visited[i].node.file_path)) {
            continue;
        }
        visible++;
    }
    char buf[CBM_SZ_256];
    snprintf(buf, sizeof(buf),
             include_evidence
                 ? "%s: %d  (rows: name hop strategy confidence; qn = group prefix + \".\" + "
                   "name)\n"
                 : "%s: %d  (rows: name hop; qn = group prefix + \".\" + name)\n",
             key, visible);
    cbm_sb_append(sb, buf);
    if (tr->visited_count > 1) {
        qsort(tr->visited, (size_t)tr->visited_count, sizeof(cbm_node_hop_t), tree_hop_cmp_qn);
    }
    char cur_group[CBM_SZ_1K] = "";
    for (int i = 0; i < tr->visited_count; i++) {
        if (!include_tests && is_test_file(tr->visited[i].node.file_path)) {
            continue;
        }
        const char *qn =
            tr->visited[i].node.qualified_name ? tr->visited[i].node.qualified_name : "";
        size_t plen = sg_qn_prefix_len(qn);
        if (plen >= sizeof(cur_group)) {
            plen = 0;
        }
        if (strncmp(cur_group, qn, plen) != 0 || cur_group[plen] != '\0') {
            snprintf(cur_group, sizeof(cur_group), "%.*s", (int)plen, qn);
            cbm_sb_append(sb, cur_group);
            cbm_sb_append(sb, ":\n");
        }
        char row[CBM_SZ_512];
        const char *ev_class = NULL;
        double ev_conf = -1.0;
        if (include_evidence &&
            bfs_edge_evidence_for_hop(tr, tr->visited[i].node.id, &ev_class, &ev_conf)) {
            if (ev_conf >= 0.0) {
                snprintf(row, sizeof(row), "  %s %d %s %.2f\n", plen ? qn + plen + 1 : qn,
                         tr->visited[i].hop, ev_class, ev_conf);
            } else {
                snprintf(row, sizeof(row), "  %s %d %s -\n", plen ? qn + plen + 1 : qn,
                         tr->visited[i].hop, ev_class);
            }
        } else if (include_evidence) {
            /* The root hop has no inbound edge, and non-CALLS edges record no
             * strategy. Emit placeholders so the column count stays fixed —
             * a ragged table is worse to parse than an explicit "-". */
            snprintf(row, sizeof(row), "  %s %d - -\n", plen ? qn + plen + 1 : qn,
                     tr->visited[i].hop);
        } else {
            snprintf(row, sizeof(row), "  %s %d\n", plen ? qn + plen + 1 : qn, tr->visited[i].hop);
        }
        cbm_sb_append(sb, row);
    }
}

/* Clamp a client-supplied traversal depth to the MCP ceiling (cbm_mcp_max_depth),
 * WARN-logging when it does so — never a silent truncation (#887). An unclamped
 * `depth` would drive the shared cbm_store_bfs to an arbitrary hop count. */
static __attribute__((unused)) int clamp_mcp_depth(int depth, const char *tool) {
    int cap = cbm_mcp_max_depth();
    if (depth > cap) {
        char req_buf[16];
        char cap_buf[16];
        snprintf(req_buf, sizeof(req_buf), "%d", depth);
        snprintf(cap_buf, sizeof(cap_buf), "%d", cap);
        cbm_log_warn("mcp.depth_capped", "tool", tool, "requested", req_buf, "cap", cap_buf);
        return cap;
    }
    return depth;
}

/* ── Helper: free heap fields of a stack-allocated node ────────── */

static __attribute__((unused)) void free_node_contents(cbm_node_t *n) {
    safe_str_free(&n->project);
    safe_str_free(&n->label);
    safe_str_free(&n->name);
    safe_str_free(&n->qualified_name);
    safe_str_free(&n->file_path);
    safe_str_free(&n->properties_json);
    memset(n, 0, sizeof(*n));
}

/* ── Helper: read lines [start, end] from a file ─────────────── */

static char *read_file_lines(const char *path, int start, int end) {
    FILE *fp = cbm_fopen(path, "r");
    if (!fp) {
        return NULL;
    }

    size_t cap = CBM_SZ_4K;
    char *buf = malloc(cap);
    size_t len = 0;
    buf[0] = '\0';

    char line[CBM_SZ_2K];
    int lineno = 0;
    while (fgets(line, sizeof(line), fp)) {
        lineno++;
        if (lineno < start) {
            continue;
        }
        if (lineno > end) {
            break;
        }
        size_t ll = strlen(line);
        while (len + ll + SKIP_ONE > cap) {
            cap *= PAIR_LEN;
            buf = safe_realloc(buf, cap);
        }
        memcpy(buf + len, line, ll);
        len += ll;
        buf[len] = '\0';
    }

    (void)fclose(fp);
    if (len == 0) {
        free(buf);
        return NULL;
    }
    return buf;
}

/* ── Helper: get project root_path from store ─────────────────── */

static char *project_root_from_store(cbm_store_t *store, const char *project) {
    if (!store || !project) {
        return NULL;
    }
    cbm_project_t proj = {0};
    if (cbm_store_get_project(store, project, &proj) != CBM_STORE_OK) {
        return NULL;
    }
    char *root = heap_strdup(proj.root_path);
    safe_str_free(&proj.name);
    safe_str_free(&proj.indexed_at);
    safe_str_free(&proj.root_path);
    return root;
}

static char *get_project_root(cbm_mcp_server_t *srv, const char *project) {
    return project_root_from_store(resolve_store(srv, project), project);
}

/* ── index_repository ─────────────────────────────────────────── */

static int cross_repo_project_key_compare(const void *left, const void *right) {
    const char *const *left_key = left;
    const char *const *right_key = right;
    return strcmp(*left_key, *right_key);
}

static unsigned char cross_repo_project_lock_fold(unsigned char ch) {
    return ch >= 'A' && ch <= 'Z' ? (unsigned char)(ch + ('a' - 'A')) : ch;
}

/* Match daemon/project_lock.c's OS-key identity exactly: only ASCII A-Z folds.
 * The raw strcmp tie-break gives qsort a total, input-order-independent order
 * while keeping the caller's original project spelling as the lease value. */
static int cross_repo_project_lock_key_compare_values(const char *left, const char *right) {
    const unsigned char *left_cursor = (const unsigned char *)left;
    const unsigned char *right_cursor = (const unsigned char *)right;
    while (*left_cursor && *right_cursor) {
        unsigned char left_folded = cross_repo_project_lock_fold(*left_cursor);
        unsigned char right_folded = cross_repo_project_lock_fold(*right_cursor);
        if (left_folded != right_folded) {
            return left_folded < right_folded ? -1 : 1;
        }
        left_cursor++;
        right_cursor++;
    }
    if (*left_cursor != *right_cursor) {
        return *left_cursor ? 1 : -1;
    }
    return strcmp(left, right);
}

static int cross_repo_project_lock_key_compare(const void *left, const void *right) {
    const char *const *left_key = left;
    const char *const *right_key = right;
    return cross_repo_project_lock_key_compare_values(*left_key, *right_key);
}

static bool cross_repo_project_lock_keys_equivalent(const char *left, const char *right) {
    const unsigned char *left_cursor = (const unsigned char *)left;
    const unsigned char *right_cursor = (const unsigned char *)right;
    while (*left_cursor && *right_cursor) {
        if (cross_repo_project_lock_fold(*left_cursor) !=
            cross_repo_project_lock_fold(*right_cursor)) {
            return false;
        }
        left_cursor++;
        right_cursor++;
    }
    return *left_cursor == *right_cursor;
}

/* Handle mode="cross-repo-intelligence" — extract to reduce complexity. */
static char *handle_cross_repo_mode(cbm_mcp_server_t *srv, const char *repo_path,
                                    const char *name_override, const char *args) {
    if (name_override && name_override[0] && !cbm_validate_project_name(name_override)) {
        return cbm_mcp_text_result("invalid project name", true);
    }
    char *project = name_override && name_override[0] ? heap_strdup(name_override)
                                                      : cbm_project_name_from_path(repo_path);
    if (!project) {
        return cbm_mcp_text_result("cannot derive project name", true);
    }

    yyjson_doc *jdoc = yyjson_read(args, strlen(args), 0);
    yyjson_val *jroot = jdoc ? yyjson_doc_get_root(jdoc) : NULL;
    yyjson_val *tp_arr = jroot ? yyjson_obj_get(jroot, "target_projects") : NULL;

    if (!tp_arr || !yyjson_is_arr(tp_arr) || yyjson_arr_size(tp_arr) == 0) {
        yyjson_doc_free(jdoc);
        free(project);
        return cbm_mcp_text_result(
            "{\"error\":\"target_projects is required for cross-repo-intelligence mode. "
            "Use [\\\"*\\\"] for all projects. Run list_projects to see available.\"}",
            true);
    }

    size_t target_count = yyjson_arr_size(tp_arr);
    if (target_count > MCP_MAX_CROSS_REPO_TARGETS) {
        yyjson_doc_free(jdoc);
        free(project);
        return cbm_mcp_text_result("too many cross-repo target projects", true);
    }
    int tp_count = (int)target_count;
    const char **targets = malloc((size_t)tp_count * sizeof(*targets));
    const char **lease_keys = malloc(((size_t)tp_count + 1U) * sizeof(*lease_keys));
    if (!targets || !lease_keys) {
        free(targets);
        free(lease_keys);
        yyjson_doc_free(jdoc);
        free(project);
        return cbm_mcp_text_result("failed to allocate cross-repo project leases", true);
    }
    size_t idx;
    size_t max;
    yyjson_val *val;
    int ti = 0;
    bool all_projects = false;
    bool invalid_target = false;
    yyjson_arr_foreach(tp_arr, idx, max, val) {
        const char *target = yyjson_is_str(val) ? yyjson_get_str(val) : NULL;
        if (!target || !target[0] || strlen(target) >= CBM_SZ_256 ||
            (strcmp(target, "*") != 0 && !cbm_validate_project_name(target))) {
            invalid_target = true;
            break;
        }
        targets[ti++] = target;
        all_projects = all_projects || strcmp(target, "*") == 0;
    }
    if (invalid_target || ti != tp_count) {
        free(targets);
        free(lease_keys);
        yyjson_doc_free(jdoc);
        free(project);
        return cbm_mcp_text_result("target_projects must contain valid project names or '*'", true);
    }
    if (all_projects && tp_count != 1) {
        free(targets);
        free(lease_keys);
        yyjson_doc_free(jdoc);
        free(project);
        return cbm_mcp_text_result("target_projects wildcard '*' must be the only entry", true);
    }
    if (!all_projects) {
        qsort(targets, (size_t)tp_count, sizeof(*targets), cross_repo_project_key_compare);
        int unique_count = 0;
        for (int i = 0; i < tp_count; i++) {
            if (unique_count == 0 || strcmp(targets[i], targets[unique_count - 1]) != 0) {
                targets[unique_count++] = targets[i];
            }
        }
        tp_count = unique_count;
    }

    int lease_count = 0;
    if (all_projects) {
        lease_keys[lease_count++] = "*";
    } else {
        lease_keys[lease_count++] = project;
        for (int i = 0; i < tp_count; i++) {
            lease_keys[lease_count++] = targets[i];
        }
        qsort(lease_keys, (size_t)lease_count, sizeof(*lease_keys),
              cross_repo_project_lock_key_compare);
        int unique_count = 0;
        for (int i = 0; i < lease_count; i++) {
            if (unique_count == 0 || !cross_repo_project_lock_keys_equivalent(
                                         lease_keys[i], lease_keys[unique_count - 1])) {
                lease_keys[unique_count++] = lease_keys[i];
            }
        }
        lease_count = unique_count;
    }

    int held_count = 0;
    while (held_count < lease_count && mcp_project_mutation_begin(srv, lease_keys[held_count])) {
        held_count++;
    }
    bool cancelled =
        atomic_load_explicit(&srv->pipeline_cancel_requested, memory_order_acquire) != 0;
    if (held_count != lease_count || cancelled) {
        while (held_count > 0) {
            held_count--;
            mcp_project_mutation_end(srv, lease_keys[held_count]);
        }
        free(targets);
        free(lease_keys);
        yyjson_doc_free(jdoc);
        free(project);
        return cbm_mcp_text_result("cross-repo operation cancelled or blocked by active indexing",
                                   true);
    }

    cbm_cross_repo_result_t result = cbm_cross_repo_match_cancellable(
        project, targets, tp_count, &srv->pipeline_cancel_requested);
    while (held_count > 0) {
        held_count--;
        mcp_project_mutation_end(srv, lease_keys[held_count]);
    }
    free(targets);
    free(lease_keys);
    yyjson_doc_free(jdoc);

    if (result.failed) {
        free(project);
        return cbm_mcp_text_result(
            "cross-repo source or target project is missing, invalid, or not indexed", true);
    }

    int total = result.http_edges + result.async_edges + result.channel_edges + result.grpc_edges +
                result.graphql_edges + result.trpc_edges;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", result.cancelled ? "cancelled" : "success");
    yyjson_mut_obj_add_str(doc, root, "mode", "cross-repo-intelligence");
    yyjson_mut_obj_add_strcpy(doc, root, "project", project);
    if (result.cancelled) {
        yyjson_mut_obj_add_bool(doc, root, "partial_results", result.partial_results);
        yyjson_mut_obj_add_str(
            doc, root, "message",
            result.partial_results
                ? "cross-repo operation cancelled with partial results; completed database "
                  "writes were retained"
                : "cross-repo operation cancelled before database writes");
    }
    yyjson_mut_obj_add_int(doc, root, "projects_scanned", result.projects_scanned);
    yyjson_mut_obj_add_int(doc, root, "cross_http_calls", result.http_edges);
    yyjson_mut_obj_add_int(doc, root, "cross_async_calls", result.async_edges);
    yyjson_mut_obj_add_int(doc, root, "cross_channel", result.channel_edges);
    yyjson_mut_obj_add_int(doc, root, "cross_grpc_calls", result.grpc_edges);
    yyjson_mut_obj_add_int(doc, root, "cross_graphql_calls", result.graphql_edges);
    yyjson_mut_obj_add_int(doc, root, "cross_trpc_calls", result.trpc_edges);
    yyjson_mut_obj_add_int(doc, root, "total_cross_edges", total);
    yyjson_mut_obj_add_real(doc, root, "elapsed_ms", result.elapsed_ms);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(project);
    char *out = cbm_mcp_text_result(json, result.cancelled);
    free(json);
    return out;
}

/* Bootstrap from artifact if no local DB exists for this project. */
static void try_artifact_bootstrap(const char *project_name, const char *repo_path) {
    char db_buf[CBM_SZ_1K];
    project_db_path(project_name, db_buf, sizeof(db_buf));
    if (cbm_file_size(db_buf) < 0 && cbm_artifact_exists(repo_path)) {
        cbm_log_info("index.artifact_bootstrap", "project", project_name);
        /* An imported artifact is trusted for graph CONTENT as-is — nothing
         * verifies that its nodes/edges describe the code they claim to. What
         * has been limiting the blast radius is mechanical, not a check: every
         * imported row carries the EXPORTER's mtime, so the first incremental
         * run re-parses ~everything and auto-scrubs a poisoned artifact at a
         * clone time the producer cannot predict. That exposure is transient
         * and self-healing.
         *
         * cbm_artifact_reconcile_hashes deliberately trades part of that away
         * for the speed the artifact is supposed to deliver (#885): rows it
         * restamps are no longer re-parsed, so poisoned nodes for those files
         * persist until the file changes — a DURABLE exposure gated on a
         * producer-written marker. It stays acceptable only because the marker
         * alone never suffices: each restamped row must additionally be proven
         * unchanged by the LOCAL git against a commit present in this clone.
         * Read the trade-off note in artifact.h before widening it.
         *
         * Best-effort: a -1 return leaves every row foreign and falls back to
         * today's behavior, so a failure here can never fail the import. */
        if (cbm_artifact_import(repo_path, db_buf) == 0) {
            (void)cbm_artifact_reconcile_hashes(repo_path, db_buf, project_name);
        }
    }
}

/* Cap on excluded dir paths listed in the response — keep it compact on large
 * repos (node_modules / vendor / etc. can produce many skip points). The full
 * count is still reported via "count" + "truncated". */
enum { INDEX_EXCLUDED_DIR_CAP = 5 }; /* examples only — see INDEX_SKIPPED_FILE_CAP note */

/* Attach a compact summary of directory subtrees skipped during discovery (#411).
 * Shape: "excluded": {"dirs": [up to 25 rel-paths], "count": <total>, "truncated": <bool>}.
 * No-op when nothing was excluded. excluded_dirs[] is borrowed (copied into doc). */
static void add_excluded_summary(yyjson_mut_doc *doc, yyjson_mut_val *root, char **excluded_dirs,
                                 int excluded_count) {
    if (!excluded_dirs || excluded_count <= 0) {
        return;
    }
    yyjson_mut_val *excluded = yyjson_mut_obj(doc);
    yyjson_mut_val *dirs = yyjson_mut_arr(doc);
    int shown = excluded_count < INDEX_EXCLUDED_DIR_CAP ? excluded_count : INDEX_EXCLUDED_DIR_CAP;
    for (int i = 0; i < shown; i++) {
        if (excluded_dirs[i]) {
            yyjson_mut_arr_add_strcpy(doc, dirs, excluded_dirs[i]);
        }
    }
    yyjson_mut_obj_add_val(doc, excluded, "dirs", dirs);
    yyjson_mut_obj_add_int(doc, excluded, "count", excluded_count);
    yyjson_mut_obj_add_bool(doc, excluded, "truncated", excluded_count > INDEX_EXCLUDED_DIR_CAP);
    yyjson_mut_obj_add_val(doc, root, "excluded", excluded);
}

/* Cap on per-file skips embedded in the JSON response — keep it compact on
 * large repos. The FULL, uncapped list always goes to the per-run logfile;
 * the JSON carries "count" + "truncated" so nothing is silently hidden. */
/* In-response coverage lists are EXAMPLES, not the record: the full uncapped
 * lists live in the per-run logfile (path in the same response) and are
 * queryable via index_status (scope_limit) / query_graph(graph="missed").
 * Five examples orient the agent; anything more duplicates the logfile into
 * every index response (53 KB observed on a large repo). */
enum { INDEX_SKIPPED_FILE_CAP = 5 };

/* Attach the by-design ignored-FILES summary (#963 "purposely not indexed").
 * Individual files dropped by ignore rules — deliberate, not failures; whole
 * excluded subtrees are reported separately via "excluded". Always emits
 * "not_indexed_files_count" (the uncapped total); the list itself is capped
 * like skipped[] and marked truncated when discovery hit its storage cap. */
static void add_not_indexed_files_summary(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                          cbm_pipeline_t *p) {
    cbm_ignored_file_t *ignored = NULL;
    int stored = 0;
    int total = 0;
    cbm_pipeline_get_ignored(p, &ignored, &stored, &total);
    yyjson_mut_obj_add_int(doc, root, "not_indexed_files_count", total);
    if (!ignored || stored <= 0) {
        return;
    }
    yyjson_mut_val *ni = yyjson_mut_obj(doc);
    yyjson_mut_val *files = yyjson_mut_arr(doc);
    int shown = stored < INDEX_SKIPPED_FILE_CAP ? stored : INDEX_SKIPPED_FILE_CAP;
    for (int i = 0; i < shown; i++) {
        yyjson_mut_val *fe = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, fe, "path", ignored[i].rel_path ? ignored[i].rel_path : "");
        yyjson_mut_obj_add_strcpy(doc, fe, "reason", ignored[i].reason ? ignored[i].reason : "");
        yyjson_mut_arr_add_val(files, fe);
    }
    yyjson_mut_obj_add_val(doc, ni, "files", files);
    yyjson_mut_obj_add_int(doc, ni, "count", total);
    yyjson_mut_obj_add_bool(doc, ni, "truncated", total > shown);
    yyjson_mut_obj_add_str(doc, ni, "note",
                           "Excluded by design (gitignore/.cbmignore/skip-lists); examples only — "
                           "full list in 'logfile'.");
    yyjson_mut_obj_add_val(doc, root, "not_indexed_files", ni);
}

/* True when a recorded per-file entry is the parse-partial coverage signal
 * (#963) rather than a genuine skip. Kept out of skipped[]/skipped_count so
 * the "skipped" contract (file NOT indexed) stays exact. */
static bool is_parse_partial(const cbm_file_error_t *e) {
    return e->phase && strcmp(e->phase, "parse_partial") == 0;
}

/* Attach a summary of per-file skips (Stage 2 / Track B). Always emits a
 * top-level "skipped_count" (0 on clean runs) so consumers can rely on it.
 * When there are skips, also emits:
 *   "skipped": {"files":[{path,reason,phase}..(<=50)], "count":N, "truncated":bool}
 * and, if a per-run logfile was written, "logfile": "<path>".
 * The run status stays "indexed" — a skipped file is the expected handled
 * outcome, not a failure. errs[] is borrowed (copied into doc) and may contain
 * parse_partial entries, which are filtered out here (reported separately by
 * add_parse_partial_summary). */
static void add_skipped_summary(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                const cbm_file_error_t *errs, int count, const char *logfile) {
    int skips = 0;
    for (int i = 0; i < count; i++) {
        if (!is_parse_partial(&errs[i])) {
            skips++;
        }
    }
    yyjson_mut_obj_add_int(doc, root, "skipped_count", skips);
    if (logfile && logfile[0]) {
        yyjson_mut_obj_add_strcpy(doc, root, "logfile", logfile);
    }
    if (!errs || skips <= 0) {
        return;
    }
    yyjson_mut_val *skipped = yyjson_mut_obj(doc);
    yyjson_mut_val *files = yyjson_mut_arr(doc);
    int shown = 0;
    for (int i = 0; i < count && shown < INDEX_SKIPPED_FILE_CAP; i++) {
        if (is_parse_partial(&errs[i])) {
            continue;
        }
        yyjson_mut_val *fe = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, fe, "path", errs[i].path ? errs[i].path : "");
        yyjson_mut_obj_add_strcpy(doc, fe, "reason", errs[i].reason ? errs[i].reason : "");
        yyjson_mut_obj_add_strcpy(doc, fe, "phase", errs[i].phase ? errs[i].phase : "");
        yyjson_mut_arr_add_val(files, fe);
        shown++;
    }
    yyjson_mut_obj_add_val(doc, skipped, "files", files);
    yyjson_mut_obj_add_int(doc, skipped, "count", skips);
    yyjson_mut_obj_add_bool(doc, skipped, "truncated", skips > INDEX_SKIPPED_FILE_CAP);
    yyjson_mut_obj_add_val(doc, root, "skipped", skipped);
}

/* Attach the best-effort parse-coverage summary (#963). Always emits a
 * top-level "parse_partial_count" (0 on clean runs). When files were flagged:
 *   "parse_partial": {"files":[{path,error_ranges}..(<=50)], "count":N,
 *                     "truncated":bool, "note":"..."}
 * These files WERE indexed — constructs inside the listed 1-based line ranges
 * are missing from the graph because tree-sitter could not parse them. The
 * note spells out the best-effort framing: absence from this list is NOT a
 * completeness guarantee. */
static void add_parse_partial_summary(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                      const cbm_file_error_t *errs, int count) {
    int partials = 0;
    for (int i = 0; i < count; i++) {
        if (is_parse_partial(&errs[i])) {
            partials++;
        }
    }
    yyjson_mut_obj_add_int(doc, root, "parse_partial_count", partials);
    if (!errs || partials <= 0) {
        return;
    }
    yyjson_mut_val *pp = yyjson_mut_obj(doc);
    yyjson_mut_val *files = yyjson_mut_arr(doc);
    int shown = 0;
    for (int i = 0; i < count && shown < INDEX_SKIPPED_FILE_CAP; i++) {
        if (!is_parse_partial(&errs[i])) {
            continue;
        }
        yyjson_mut_val *fe = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, fe, "path", errs[i].path ? errs[i].path : "");
        yyjson_mut_obj_add_strcpy(doc, fe, "error_ranges", errs[i].reason ? errs[i].reason : "");
        yyjson_mut_arr_add_val(files, fe);
        shown++;
    }
    yyjson_mut_obj_add_val(doc, pp, "files", files);
    yyjson_mut_obj_add_int(doc, pp, "count", partials);
    yyjson_mut_obj_add_bool(doc, pp, "truncated", partials > INDEX_SKIPPED_FILE_CAP);
    yyjson_mut_obj_add_str(doc, pp, "note",
                           "Indexed, but constructs in these line ranges may be missing (best-"
                           "effort signal); examples only — full list via index_status or "
                           "'logfile'.");
    yyjson_mut_obj_add_val(doc, root, "parse_partial", pp);
}

/* The pipeline persists the complete current coverage set before this
 * response is built. Prefer that set over the per-run errors so incremental
 * runs that do not revisit a flagged file, and artifact bootstraps, do not
 * make existing gaps appear to have vanished. By-design exclusions have
 * their own response surface and are not failures. */
static bool add_persisted_failure_summaries(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                            cbm_store_t *store, const char *project,
                                            const char *logfile) {
    cbm_coverage_row_t *rows = NULL;
    int row_count = 0;
    if (cbm_store_coverage_get(store, project, &rows, &row_count) != CBM_STORE_OK) {
        return false;
    }

    int failure_count = 0;
    for (int i = 0; i < row_count; i++) {
        const char *kind = rows[i].kind ? rows[i].kind : "";
        if (strcmp(kind, "not_indexed_dir") != 0 && strcmp(kind, "not_indexed_file") != 0) {
            failure_count++;
        }
    }

    cbm_file_error_t *failures =
        failure_count > 0 ? calloc((size_t)failure_count, sizeof(*failures)) : NULL;
    if (failure_count > 0 && !failures) {
        cbm_store_free_coverage(rows, row_count);
        return false;
    }

    int n = 0;
    for (int i = 0; i < row_count; i++) {
        const char *kind = rows[i].kind ? rows[i].kind : "";
        if (strcmp(kind, "not_indexed_dir") == 0 || strcmp(kind, "not_indexed_file") == 0) {
            continue;
        }
        failures[n].path = (char *)rows[i].rel_path;
        failures[n].reason = (char *)rows[i].detail;
        failures[n].phase = (char *)rows[i].kind;
        n++;
    }

    add_skipped_summary(doc, root, failures, failure_count, logfile);
    add_parse_partial_summary(doc, root, failures, failure_count);
    free(failures);
    cbm_store_free_coverage(rows, row_count);
    return true;
}

/* Write the FULL (uncapped) skip list to a per-run logfile — ONLY when >=1 file
 * was skipped (no logfile on a clean run). Location:
 *   $CBM_INDEX_LOG (override) else <cache_dir>/logs/<project>-<epoch>.log
 * Returns true and fills out_path on success. */
static bool write_skip_logfile(const char *project, const cbm_file_error_t *errs, int count,
                               char *out_path, size_t out_sz) {
    if (!errs || count <= 0) {
        return false;
    }
    char path[CBM_SZ_1K];
    const char *override = getenv("CBM_INDEX_LOG");
    if (override && override[0]) {
        snprintf(path, sizeof(path), "%s", override);
    } else {
        const char *cdir = cbm_resolve_cache_dir();
        if (!cdir) {
            return false;
        }
        char logdir[CBM_SZ_1K];
        snprintf(logdir, sizeof(logdir), "%s/logs", cdir);
        cbm_mkdir_p(logdir, 0755);
        snprintf(path, sizeof(path), "%s/%s-%lld.log", logdir, project ? project : "index",
                 (long long)time(NULL));
    }
    FILE *f = cbm_fopen(path, "wb");
    if (!f) {
        cbm_log_warn("index.logfile_open_fail", "path", path);
        return false;
    }
    int partials = 0;
    for (int i = 0; i < count; i++) {
        if (is_parse_partial(&errs[i])) {
            partials++;
        }
    }
    (void)fprintf(f, "# codebase-memory-cli index coverage report\n");
    (void)fprintf(f, "# project=%s skipped=%d parse_partial=%d\n", project ? project : "",
                  count - partials, partials);
    (void)fprintf(f, "# columns: phase\treason\tpath\n");
    for (int i = 0; i < count; i++) {
        (void)fprintf(f, "%s\t%s\t%s\n", errs[i].phase ? errs[i].phase : "",
                      errs[i].reason ? errs[i].reason : "", errs[i].path ? errs[i].path : "");
    }
    (void)fclose(f);
    if (out_path && out_sz) {
        snprintf(out_path, out_sz, "%s", path);
    }
    return true;
}

/* Build the success portion of the index_repository response.
 * Returns true when status should be "degraded" (#334 plausibility gate). */
static bool build_index_success_response(cbm_mcp_server_t *srv, yyjson_mut_doc *doc,
                                         yyjson_mut_val *root, const char *project_name,
                                         const char *repo_path, bool persistence, cbm_pipeline_t *p,
                                         char **excluded_dirs, int excluded_count,
                                         const cbm_file_error_t *file_errors, int file_error_count,
                                         const char *logfile) {
    add_excluded_summary(doc, root, excluded_dirs, excluded_count);
    add_not_indexed_files_summary(doc, root, p);

    int exp_nodes = -1;
    int exp_edges = -1;
    cbm_pipeline_get_committed_counts(p, &exp_nodes, &exp_edges);

    const double ratio = cbm_dump_verify_min_ratio();
    const int min_floor = CBM_DUMP_VERIFY_MIN_FLOOR;

    cbm_store_t *store = resolve_store(srv, project_name);
    if (!store || !add_persisted_failure_summaries(doc, root, store, project_name, logfile)) {
        add_skipped_summary(doc, root, file_errors, file_error_count, logfile);
        add_parse_partial_summary(doc, root, file_errors, file_error_count);
    }
    int nodes = 0;
    int edges = 0;
    bool degraded = false;

    if (!store) {
        degraded = true;
    } else {
        nodes = cbm_store_count_nodes(store, project_name);
        edges = cbm_store_count_edges(store, project_name);
        if (nodes < 0) {
            degraded = true;
            nodes = 0;
            edges = edges >= 0 ? edges : 0;
        } else if (cbm_dump_verify_is_degraded(exp_nodes, nodes, ratio, min_floor)) {
            (void)cbm_store_checkpoint(store);
            int nodes2 = cbm_store_count_nodes(store, project_name);
            int edges2 = cbm_store_count_edges(store, project_name);
            if (nodes2 >= 0) {
                nodes = nodes2;
            }
            if (edges2 >= 0) {
                edges = edges2;
            }
            degraded = cbm_dump_verify_is_degraded(exp_nodes, nodes, ratio, min_floor);
        }
    }

    yyjson_mut_obj_add_int(doc, root, "nodes", nodes);
    yyjson_mut_obj_add_int(doc, root, "edges", edges);
    if (exp_nodes >= 0) {
        yyjson_mut_obj_add_int(doc, root, "expected_nodes", exp_nodes);
        yyjson_mut_obj_add_int(doc, root, "expected_edges", exp_edges);
    }

    if (degraded) {
        if (!store) {
            yyjson_mut_obj_add_str(doc, root, "hint",
                                   "Index database failed integrity check and was removed. "
                                   "Re-run index_repository(repo_path=...) to rebuild.");
            cbm_log_warn("dump.verify", "reason", "store_missing", "expected_nodes",
                         exp_nodes >= 0 ? "set" : "unknown");
        } else {
            char exp_buf[MCP_FIELD_SIZE];
            char got_buf[MCP_FIELD_SIZE];
            snprintf(exp_buf, sizeof(exp_buf), "%d", exp_nodes);
            snprintf(got_buf, sizeof(got_buf), "%d", nodes);
            yyjson_mut_obj_add_str(
                doc, root, "hint",
                "Persisted far fewer nodes than indexed — likely durability loss from a "
                "hard-killed sibling process. Re-run index_repository(repo_path=...) to rebuild.");
            cbm_log_warn("dump.verify", "expected_nodes", exp_buf, "persisted_nodes", got_buf);
        }
    }

    bool adr_exists = project_has_adr(store, project_name, repo_path);
    yyjson_mut_obj_add_bool(doc, root, "adr_present", adr_exists);
    if (!adr_exists && !degraded) {
        yyjson_mut_obj_add_str(
            doc, root, "adr_hint",
            "Project indexed. Consider creating an Architecture Decision Record: "
            "explore the codebase with get_architecture(aspects=['all']), then use "
            "manage_adr(mode='update') to persist architectural insights across sessions.");
    }

    bool has_artifact = cbm_artifact_exists(repo_path);
    yyjson_mut_obj_add_bool(doc, root, "artifact_present", has_artifact);
    if (persistence && has_artifact) {
        yyjson_mut_obj_add_str(doc, root, "artifact_hint",
                               "Persistent artifact written to .codebase-memory/graph.db.zst. "
                               "Commit this file to share the index with teammates.");
    }

    return degraded;
}

/* Build the response for a worker that crashed/hung/failed without producing a
 * result. The crash is already contained (this process survived); we report it
 * rather than dying. Precise skip-and-continue (quarantine the culprit, index the
 * rest) is layered on in the probe stage. */
static char *build_worker_failure_response(const char *args, cbm_proc_outcome_t outcome) {
    char *repo_path = cbm_mcp_get_string_arg(args, "repo_path");
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", "error");
    yyjson_mut_obj_add_str(doc, root, "outcome", cbm_proc_outcome_str(outcome));
    const char *hint = NULL;
    if (outcome == CBM_PROC_SPAWN_FAILED) {
        hint = "Indexing worker could not be started. Supervision is mandatory in a CBM "
               "host, so no in-process fallback was attempted.";
    } else if (outcome == CBM_PROC_HANG) {
        hint = "Indexing worker timed out (a file made no progress). The worker was "
               "terminated and the server survived. Re-run to retry.";
    } else if (outcome == CBM_PROC_CRASH) {
        hint = "Indexing worker crashed on a file. The crash was contained (the server "
               "survived). Re-run to retry; a future release isolates the culprit file.";
    } else if (outcome == CBM_PROC_CLEAN) {
        hint = "Indexing worker exited without a valid response. No in-process fallback "
               "was attempted.";
    } else {
        hint = "Indexing worker did not complete successfully. The failure was contained "
               "and no in-process fallback was attempted.";
    }
    yyjson_mut_obj_add_str(doc, root, "hint", hint);
    if (repo_path) {
        yyjson_mut_obj_add_strcpy(doc, root, "repo_path", repo_path);
    }
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(repo_path);
    char *result = cbm_mcp_text_result(json, true);
    free(json);
    return result;
}

static char *build_worker_unsafe_terminal_response(const char *args, cbm_proc_outcome_t outcome,
                                                   bool cancellation_requested) {
    char *repo_path = cbm_mcp_get_string_arg(args, "repo_path");
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", "error");
    yyjson_mut_obj_add_str(doc, root, "outcome", cbm_proc_outcome_str(outcome));
    yyjson_mut_obj_add_str(
        doc, root, "hint",
        cancellation_requested
            ? "Indexing worker was cancelled. No in-process retry was started."
            : "Indexing worker process-tree containment failed. No in-process retry was "
              "started; inspect daemon logs.");
    if (repo_path) {
        yyjson_mut_obj_add_strcpy(doc, root, "repo_path", repo_path);
    }
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    free(repo_path);
    char *response = cbm_mcp_text_result(json, true);
    free(json);
    return response;
}

/* Drop the cached store so the next query reopens whatever the worker wrote (each
 * worker is a fresh process that deletes + recreates the .db). NULL-safe: the
 * background watcher path (main.c) has no MCP server / cached store — the child
 * writes the DB and the parent only needs the return code, so there is nothing
 * to invalidate. */
static void invalidate_cached_store(cbm_mcp_server_t *srv) {
    if (!srv) {
        return;
    }
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
        srv->store = NULL;
    }
    free(srv->current_project);
    srv->current_project = NULL;
}

/* Resolve a per-supervisor-run temp path <cache_dir>/logs/.supervisor-<pid><suffix>
 * (falls back to the CWD if the cache dir is unresolvable). Used for the crash-
 * attribution marker and the quarantine list during the recovery re-run. */
static void supervisor_tmp_path(char *out, size_t out_sz, const char *suffix) {
    const char *cdir = cbm_resolve_cache_dir();
    if (cdir && cdir[0]) {
        char logdir[CBM_SZ_1K];
        snprintf(logdir, sizeof(logdir), "%s/logs", cdir);
        cbm_mkdir_p(logdir, 0755);
        snprintf(out, out_sz, "%s/.supervisor-%d%s", logdir, (int)getpid(), suffix);
    } else {
        snprintf(out, out_sz, ".supervisor-%d%s", (int)getpid(), suffix);
    }
}

/* Parse the worker's marker JOURNAL ("S <rel>" / "D <rel>" lines, one event
 * per line — see cbm_index_mark_start/done) into the crash/hang SUSPECT set:
 * files whose last event is an S with no closing D, i.e. the in-flight set
 * at kill time. Recovery runs are PARALLEL, so there are up to worker_count
 * suspects; a torn final line (no trailing newline) is discarded by design.
 * Returns a malloc'd array of malloc'd rel paths, OLDEST OPEN S FIRST (for a
 * hang, the oldest still-open file IS the stuck one). Caller frees via
 * supervisor_free_suspects. */
static char **supervisor_read_suspects(const char *path, int *out_n) {
    *out_n = 0;
    FILE *f = cbm_fopen(path, "rb");
    if (!f) {
        return NULL;
    }
    char **open_paths = NULL; /* open (S-without-D) files in first-S order */
    int open_n = 0;
    int open_cap = 0;
    char line[CBM_SZ_1K];
    while (fgets(line, sizeof(line), f)) {
        size_t len = strlen(line);
        if (len == 0 || line[len - 1] != '\n') {
            break; /* torn final line — discard and stop */
        }
        line[--len] = '\0';
        if (len > 0 && line[len - 1] == '\r') {
            line[--len] = '\0';
        }
        if (len < 3 || (line[0] != 'S' && line[0] != 'D') || line[1] != ' ') {
            continue;
        }
        const char *rel = line + 2;
        if (line[0] == 'S') {
            bool already = false;
            for (int i = 0; i < open_n && !already; i++) {
                already = strcmp(open_paths[i], rel) == 0;
            }
            if (already) {
                continue;
            }
            if (open_n == open_cap) {
                int ncap = open_cap ? open_cap * 2 : 16;
                char **np = (char **)realloc(open_paths, (size_t)ncap * sizeof(char *));
                if (!np) {
                    break;
                }
                open_paths = np;
                open_cap = ncap;
            }
            open_paths[open_n++] = cbm_strdup(rel);
        } else {
            for (int i = 0; i < open_n; i++) {
                if (strcmp(open_paths[i], rel) == 0) {
                    free(open_paths[i]);
                    memmove(&open_paths[i], &open_paths[i + 1],
                            (size_t)(open_n - i - 1) * sizeof(char *));
                    open_n--;
                    break;
                }
            }
        }
    }
    (void)fclose(f);
    if (open_n == 0) {
        free(open_paths);
        return NULL;
    }
    *out_n = open_n;
    return open_paths;
}

static void supervisor_free_suspects(char **s, int n) {
    if (!s) {
        return;
    }
    for (int i = 0; i < n; i++) {
        free(s[i]);
    }
    free(s);
}

static bool supervisor_suspect_contains(char **s, int n, const char *rel) {
    for (int i = 0; i < n; i++) {
        if (s[i] && strcmp(s[i], rel) == 0) {
            return true;
        }
    }
    return false;
}

/* Append one quarantine entry "rel\tphase\n" (phase = "crash"|"hang"|"error") to the
 * quarantine list. The worker's loader parses this back and reports the skip's
 * phase in skipped[]; a bare "rel" line is still tolerated there (defaults crash). */
static bool supervisor_append_quarantine(const char *path, const char *rel, const char *phase) {
    FILE *f = cbm_fopen(path, "ab");
    if (!f) {
        return false;
    }
    (void)fprintf(f, "%s\t%s\n", rel, phase);
    (void)fclose(f);
    return true;
}

cbm_mcp_supervised_result_disposition_t cbm_mcp_supervised_result_disposition(
    int spawn_result, const cbm_index_worker_result_t *worker_result) {
    if (spawn_result != 0 || !worker_result || worker_result->outcome == CBM_PROC_SPAWN_FAILED) {
        return CBM_MCP_SUPERVISED_RESULT_FALLBACK;
    }
    if (worker_result->cancellation_requested || !worker_result->tree_quiesced ||
        worker_result->supervision_failed) {
        return CBM_MCP_SUPERVISED_RESULT_UNSAFE_TERMINAL;
    }
    if (worker_result->outcome == CBM_PROC_CLEAN) {
        return worker_result->response ? CBM_MCP_SUPERVISED_RESULT_SUCCESS
                                       : CBM_MCP_SUPERVISED_RESULT_FALLBACK;
    }
    return CBM_MCP_SUPERVISED_RESULT_CONTAINED_FAILURE;
}

/* Run index_repository in a supervised worker subprocess with skip-and-continue
 * (Stage 3c). Returns the response string (caller frees):
 *   - the worker's own response on a clean first run (the common path);
 *   - after a crash/hang, the response from a clean single-threaded RECOVERY run
 *     that quarantines the culprit file(s) — status="indexed" with them listed in
 *     skipped[] as phase="crash"/"hang"/"error", and the good files indexed;
 *   - a best-effort PARTIAL index (one final quarantine-only run) if the recovery
 *     loop cannot converge but at least one file was quarantined;
 *   - a contained-failure response only if even that cannot produce a clean run.
 * A physical CBM host never falls back to its in-process pipeline: an initial
 * start/protocol failure is returned as an explicit error response. */
static char *index_run_supervised(cbm_mcp_server_t *srv, const char *args) {
    invalidate_cached_store(srv);

    /* First attempt: normal parallel run. */
    cbm_index_worker_result_t wr;
    int rc = cbm_index_spawn_worker_with_log_cancel(
        args, false, NULL, NULL, srv ? srv->index_log_callback : NULL,
        srv ? srv->index_log_context : NULL, srv ? &srv->pipeline_cancel_requested : NULL, &wr);
    cbm_mcp_supervised_result_disposition_t disposition =
        cbm_mcp_supervised_result_disposition(rc, &wr);

    if (disposition == CBM_MCP_SUPERVISED_RESULT_FALLBACK) {
        cbm_proc_outcome_t outcome = wr.outcome;
        cbm_index_worker_result_free(&wr);
        invalidate_cached_store(srv);
        return build_worker_failure_response(args, outcome);
    }
    if (disposition == CBM_MCP_SUPERVISED_RESULT_UNSAFE_TERMINAL) {
        char *failure =
            build_worker_unsafe_terminal_response(args, wr.outcome, wr.cancellation_requested);
        cbm_index_worker_result_free(&wr);
        invalidate_cached_store(srv);
        return failure;
    }
    if (disposition == CBM_MCP_SUPERVISED_RESULT_SUCCESS) {
        /* Clean exit → transfer the worker's response (the common path). */
        char *resp = wr.response; /* transfer ownership to caller (may be NULL) */
        wr.response = NULL;
        cbm_index_worker_result_free(&wr);
        invalidate_cached_store(srv);
        return resp;
    }

    /* Crash / hang / nonzero exit → skip-and-continue recovery. Re-run the
     * worker PARALLEL (there are no sequential production runs) with the
     * per-file marker JOURNAL armed; after each failed run the journal's
     * open-S set is the in-flight SUSPECT set. A file is quarantined only
     * when it appears in the suspect sets of TWO CONSECUTIVE failed runs
     * (intersection — a stale or merely unlucky in-flight file rotates out),
     * and only ONE file per round: the OLDEST open S in the intersection
     * (for a hang the oldest still-open file IS the stuck one; for a crash
     * it is the longest-running suspect — the best single deterministic
     * pick). A clean run then indexes the good files and reports the
     * quarantined ones as phase="crash"/"hang"/"error" skips via the ordinary
     * Stage-2 skip plumbing. The old design re-ran SINGLE-THREADED to keep
     * one exact marker; at scale that fell into the sequential crawl, went
     * quiet, was killed as a hang mid-pass, and the stale marker got FOUR
     * innocent ms-typescript fixtures quarantined one 15-minute retry at a
     * time. */
    cbm_proc_outcome_t last_outcome = wr.outcome;
    cbm_index_worker_result_free(&wr);

    char marker_path[CBM_SZ_1K];
    char quarantine_path[CBM_SZ_1K];
    supervisor_tmp_path(marker_path, sizeof(marker_path), ".marker");
    supervisor_tmp_path(quarantine_path, sizeof(quarantine_path), ".quarantine");
    (void)remove(marker_path);
    /* Start the quarantine list empty (truncate any stale file). */
    FILE *qinit = cbm_fopen(quarantine_path, "wb");
    if (qinit) {
        (void)fclose(qinit);
    }

    int cap = 100;
    const char *cap_env = getenv("CBM_INDEX_MAX_RESTARTS");
    if (cap_env && cap_env[0]) {
        int v = atoi(cap_env);
        if (v > 0) {
            cap = v;
        }
    }

    char *resp = NULL;
    int quarantined = 0;         /* files pinned + added to the quarantine list so far */
    char **prev_suspects = NULL; /* previous failed round's in-flight set */
    int prev_n = 0;
    bool unsafe_terminal = false;
    bool terminal_cancelled = false;
    for (int i = 0; i < cap; i++) {
        cbm_index_worker_result_t wr2;
        int rc2 = cbm_index_spawn_worker_with_log_cancel(
            args, /*single_thread=*/false, marker_path, quarantine_path,
            srv ? srv->index_log_callback : NULL, srv ? srv->index_log_context : NULL,
            srv ? &srv->pipeline_cancel_requested : NULL, &wr2);
        cbm_mcp_supervised_result_disposition_t recovery_disposition =
            cbm_mcp_supervised_result_disposition(rc2, &wr2);
        if (recovery_disposition == CBM_MCP_SUPERVISED_RESULT_FALLBACK) {
            last_outcome = wr2.outcome;
            cbm_index_worker_result_free(&wr2);
            break; /* spawn failed mid-recovery — give up */
        }
        if (recovery_disposition == CBM_MCP_SUPERVISED_RESULT_UNSAFE_TERMINAL) {
            last_outcome = wr2.outcome;
            unsafe_terminal = true;
            terminal_cancelled = wr2.cancellation_requested;
            cbm_index_worker_result_free(&wr2);
            break;
        }
        if (recovery_disposition == CBM_MCP_SUPERVISED_RESULT_SUCCESS) {
            resp = wr2.response; /* transfer ownership to caller */
            wr2.response = NULL;
            cbm_index_worker_result_free(&wr2);
            break; /* good files indexed; quarantined files reported as crash/hang */
        }
        if (wr2.outcome == CBM_PROC_CRASH || wr2.outcome == CBM_PROC_HANG ||
            wr2.outcome == CBM_PROC_EXIT_NONZERO) {
            last_outcome = wr2.outcome;
            cbm_index_worker_result_free(&wr2);
            /* crash vs hang vs nonzero-exit: the phase this file is quarantined
             * under and reported as in skipped[]. A fault signal → "crash"; a
             * no-progress kill → "hang"; a graceful nonzero exit (e.g. an
             * internal parse-limit/abort on a pathological file) → "error".
             * EXIT_NONZERO is attributed via the SAME marker-journal suspect
             * mechanism as a crash: the two-consecutive-strikes intersection
             * below still guards against quarantining an innocent file, and a
             * SYSTEMIC nonzero exit (e.g. a bad arg) produces no recurring
             * suspect → the intersection is empty → give_up (correct). This
             * makes a single pathological file skip-and-continue instead of
             * aborting the whole chunk.
             *
             * Note: A deterministic first-file failure (e.g. worker crashing/exiting
             * on the very first file every run) will progressively quarantine in-flight
             * files until reaching the culprit. This is an existing property accepted
             * by the crash/hang path, accepted here as a considered tradeoff. */
            const char *phase;
            if (last_outcome == CBM_PROC_HANG) {
                phase = "hang";
            } else if (last_outcome == CBM_PROC_EXIT_NONZERO) {
                phase = "error";
            } else {
                phase = "crash";
            }
            int sus_n = 0;
            char **suspects = supervisor_read_suspects(marker_path, &sus_n);
            (void)remove(marker_path); /* fresh journal for the next re-run */
            if (!suspects || sus_n == 0) {
                supervisor_free_suspects(suspects, sus_n);
                cbm_log_warn("index.supervisor.unattributable", "action", "give_up");
                break;
            }
            if (prev_suspects) {
                /* Two-consecutive-strikes: quarantine the OLDEST open S that
                 * was also in flight in the previous failed round. */
                const char *pick = NULL;
                for (int k = 0; k < sus_n && !pick; k++) {
                    if (supervisor_suspect_contains(prev_suspects, prev_n, suspects[k])) {
                        pick = suspects[k];
                    }
                }
                if (!pick) {
                    /* Disjoint consecutive in-flight sets: the failure is not
                     * attributable to a recurring file (systemic) — stop
                     * rather than quarantine an innocent. */
                    supervisor_free_suspects(suspects, sus_n);
                    cbm_log_warn("index.supervisor.unattributable", "action", "give_up");
                    break;
                }
                if (!supervisor_append_quarantine(quarantine_path, pick, phase)) {
                    cbm_log_warn("index.supervisor.quarantine_write_fail", "path", pick);
                    supervisor_free_suspects(suspects, sus_n);
                    break;
                }
                quarantined++;
                char attempt_buf[MCP_FIELD_SIZE];
                snprintf(attempt_buf, sizeof(attempt_buf), "%d", i + 1);
                cbm_log_warn("index.file_quarantined", "path", pick, "outcome", phase, "attempt",
                             attempt_buf);
            }
            supervisor_free_suspects(prev_suspects, prev_n);
            prev_suspects = suspects;
            prev_n = sus_n;
            continue;
        }
        /* SPAWN_FAILED / nonzero exit / non-fault kill → not a crash we can
         * attribute; stop and report a contained failure. */
        last_outcome = wr2.outcome;
        cbm_index_worker_result_free(&wr2);
        break;
    }
    supervisor_free_suspects(prev_suspects, prev_n);

    (void)remove(marker_path); /* marker no longer needed */

    /* Terminal best-effort-partial: the loop exited WITHOUT a clean run (cap
     * exhausted, or an unattributable failure) but at least one file was already
     * quarantined. Try ONE final PARALLEL spawn with the accumulated quarantine
     * and NO marker — every known-bad file short-circuits, so a clean run yields
     * a PARTIAL index (all good files indexed, all known crashers/hangs reported
     * as skips) rather than a hard failure. Bounded by the same quiet-timeout,
     * so it cannot itself hang. Rare given monotonic progress. */
    if (!resp && !unsafe_terminal && quarantined > 0) {
        cbm_index_worker_result_t wrp;
        int rcp = cbm_index_spawn_worker_with_log_cancel(
            args, /*single_thread=*/false, NULL, quarantine_path,
            srv ? srv->index_log_callback : NULL, srv ? srv->index_log_context : NULL,
            srv ? &srv->pipeline_cancel_requested : NULL, &wrp);
        cbm_mcp_supervised_result_disposition_t partial_disposition =
            cbm_mcp_supervised_result_disposition(rcp, &wrp);
        if (partial_disposition == CBM_MCP_SUPERVISED_RESULT_SUCCESS) {
            resp = wrp.response; /* transfer ownership to caller */
            wrp.response = NULL;
            char qn[MCP_FIELD_SIZE];
            snprintf(qn, sizeof(qn), "%d", quarantined);
            cbm_log_error("index.supervisor.partial", "quarantined", qn, "outcome",
                          cbm_proc_outcome_str(last_outcome));
        } else if (partial_disposition == CBM_MCP_SUPERVISED_RESULT_UNSAFE_TERMINAL) {
            last_outcome = wrp.outcome;
            unsafe_terminal = true;
            terminal_cancelled = wrp.cancellation_requested;
        }
        cbm_index_worker_result_free(&wrp);
    }

    (void)remove(quarantine_path);
    invalidate_cached_store(srv);

    if (resp) {
        return resp;
    }
    if (unsafe_terminal) {
        return build_worker_unsafe_terminal_response(args, last_outcome, terminal_cancelled);
    }
    return build_worker_failure_response(args, last_outcome);
}

/* Build a minimal {"repo_path": "<root>"} args object (path safely escaped) and
 * run it through index_run_supervised. Shared by the session auto-index (srv
 * present → its cached store is invalidated) and the watcher re-index (srv NULL).
 * Returns the worker's response string (caller frees) or NULL to degrade. */
static char *index_run_supervised_path(cbm_mcp_server_t *srv, const char *root_path) {
    if (!root_path || !root_path[0]) {
        return NULL;
    }
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "repo_path", root_path);
    char *args = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    if (!args) {
        return NULL;
    }
    char *resp = index_run_supervised(srv, args);
    free(args);
    return resp;
}

/* Public entry (see mcp.h): the watcher re-index in main.c has no MCP server, so
 * it reaches the supervised runner through this srv-less wrapper. */
char *cbm_mcp_index_run_supervised_path(const char *root_path) {
    return index_run_supervised_path(NULL, root_path);
}

bool cbm_path_within_root(const char *root_path, const char *abs_path); /* defined below */

/* Resolve relative index requests against an explicitly supplied MCP session
 * root, never against the long-lived daemon process cwd. */
static bool resolve_session_repo_path(cbm_mcp_server_t *srv, char **repo_path) {
    if (!srv || !repo_path || !*repo_path || !srv->allowed_root_policy_set ||
        srv->session_root[0] == '\0' || repo_path_is_absolute(*repo_path)) {
        return true;
    }

    size_t root_len = strlen(srv->session_root);
    size_t path_len = strlen(*repo_path);
    bool needs_separator = root_len > 0 && srv->session_root[root_len - 1] != '/';
    if (root_len > SIZE_MAX - path_len - (needs_separator ? 2U : 1U)) {
        return false;
    }

    size_t joined_size = root_len + (needs_separator ? 1U : 0U) + path_len + 1U;
    char *joined = malloc(joined_size);
    if (!joined) {
        return false;
    }
    (void)snprintf(joined, joined_size, "%s%s%s", srv->session_root, needs_separator ? "/" : "",
                   *repo_path);
    cbm_normalize_path_sep(joined);
    free(*repo_path);
    *repo_path = joined;
    return true;
}

/* Preserve every index option while replacing all caller-supplied repo_path
 * keys with the one canonical path that was actually authorized. */
static char *index_args_with_repo_path(const char *args, const char *canonical_repo_path) {
    if (!args || !canonical_repo_path) {
        return NULL;
    }
    yyjson_doc *source = yyjson_read(args, strlen(args), 0);
    yyjson_val *source_root = source ? yyjson_doc_get_root(source) : NULL;
    if (!source_root || !yyjson_is_obj(source_root)) {
        yyjson_doc_free(source);
        return NULL;
    }

    yyjson_mut_doc *copy = yyjson_doc_mut_copy(source, NULL);
    yyjson_doc_free(source);
    yyjson_mut_val *copy_root = copy ? yyjson_mut_doc_get_root(copy) : NULL;
    if (!copy_root || !yyjson_mut_is_obj(copy_root)) {
        yyjson_mut_doc_free(copy);
        return NULL;
    }
    (void)yyjson_mut_obj_remove_key(copy_root, "repo_path");
    if (!yyjson_mut_obj_add_strcpy(copy, copy_root, "repo_path", canonical_repo_path)) {
        yyjson_mut_doc_free(copy);
        return NULL;
    }
    char *rewritten = yy_doc_to_str(copy);
    yyjson_mut_doc_free(copy);
    return rewritten;
}

/* #1211: index_repository requires repo_path, but list_projects only ever
 * advertises the project NAME, and every read tool accepts that name back via
 * get_project_arg's "project"/"project_name"/"project_id"/"projectName"
 * aliases (mcp.c:1385). Re-indexing an already-indexed project by that same
 * name had no resolution path and fell straight to "repo_path is required".
 * Look up the project's own stored root_path (list_projects proves it's on
 * file) before giving up. Query-only open, always closed here: this never
 * creates a store or touches srv->store/srv->current_project, so it cannot
 * disturb whatever project the server has cached. */
static char *resolved_repo_path_from_project_arg(const char *args) {
    char *project = get_project_arg(args);
    if (!project) {
        return NULL;
    }
    char db_path[CBM_SZ_1K];
    project_db_path(project, db_path, sizeof(db_path));
    cbm_store_t *store = db_path[0] ? cbm_store_open_path_query(db_path) : NULL;
    char *root_path = NULL;
    if (store) {
        cbm_project_t proj = {0};
        if (cbm_store_get_project(store, project, &proj) == CBM_STORE_OK) {
            root_path = proj.root_path ? heap_strdup(proj.root_path) : NULL;
            cbm_project_free_fields(&proj);
        }
        cbm_store_close(store);
    }
    free(project);
    return root_path;
}

static char *handle_index_repository(cbm_mcp_server_t *srv, const char *args) {
    char *repo_path = cbm_mcp_get_string_arg(args, "repo_path");
    char *mode_str = cbm_mcp_get_string_arg(args, "mode");
    char *name_override = cbm_mcp_get_string_arg(args, "name");
    cbm_normalize_path_sep(repo_path);

    if (!repo_path) {
        repo_path = resolved_repo_path_from_project_arg(args);
        cbm_normalize_path_sep(repo_path);
    }

    if (!repo_path) {
        free(mode_str);
        free(name_override);
        return cbm_mcp_text_result("repo_path is required", true);
    }

    if (!resolve_session_repo_path(srv, &repo_path)) {
        free(mode_str);
        free(name_override);
        free(repo_path);
        return cbm_mcp_text_result("failed to resolve repo_path", true);
    }

    repo_path = canonicalize_repo_path_if_exists(repo_path);

    /* Workspace boundary. Embedded/daemon sessions supply their explicit policy,
     * including an explicit NULL meaning unrestricted; a standalone server falls
     * back to the process-wide CBM_ALLOWED_ROOT. The decision itself lives in one
     * shared function so this handler and the HTTP UI indexing route cannot drift
     * apart — they had, and the divergence was the defect. */
    const char *allowed_root =
        srv->allowed_root_policy_set ? srv->allowed_root : getenv("CBM_ALLOWED_ROOT");
    /* repo_path is legitimately absent when the caller names an already-known
     * project instead; the root is resolved downstream. Only a path supplied here
     * is classified here — the previous check had the same tolerance. */
    char boundary_err[CBM_SZ_1K];
    if (repo_path && repo_path[0] &&
        !cbm_workspace_root_allowed(repo_path, cbm_workspace_home_dir(), cbm_workspace_cache_dir(),
                                    allowed_root, boundary_err, sizeof(boundary_err))) {
        free(mode_str);
        free(name_override);
        free(repo_path);
        return cbm_mcp_text_result(boundary_err, true);
    }

    if (mode_str && strcmp(mode_str, "cross-repo-intelligence") == 0) {
        free(mode_str);
        char *result = handle_cross_repo_mode(srv, repo_path, name_override, args);
        free(name_override);
        free(repo_path);
        return result;
    }

    /* A daemon session delegates the one physical write to its shared job
     * registry only after path canonicalization and workspace authorization. */
    if (srv->index_executor) {
        char *worker_args = index_args_with_repo_path(args, repo_path);
        char *coordinated =
            worker_args ? srv->index_executor(srv->index_executor_context, repo_path, worker_args)
                        : NULL;
        free(worker_args);
        free(repo_path);
        free(mode_str);
        free(name_override);
        return coordinated ? coordinated
                           : cbm_mcp_text_result(
                                 "daemon index coordinator could not start the operation", true);
    }

    /* Resolve the exact project key before choosing supervised or in-process
     * execution. A supervised worker owns the OS mutation lease itself: if the
     * CLI parent is killed, the worker must keep project exclusion until its
     * parent-death watchdog reaps the complete worker tree. */
    char *mutation_project =
        cbm_project_name_from_path(name_override && name_override[0] ? name_override : repo_path);
    if (!mutation_project) {
        free(repo_path);
        free(mode_str);
        free(name_override);
        return cbm_mcp_text_result("could not resolve index project name", true);
    }

    /* Supervisor gate: validate the canonical path and the host session's
     * workspace policy before handing work to a crash/hang-isolating worker.
     * The parent deliberately owns no project lease on this path; the worker
     * installs the same guard before running the in-process pipeline. A marked
     * host fails closed if preparation or worker startup cannot complete. */
    if (cbm_index_supervisor_should_wrap()) {
        char *worker_args = index_args_with_repo_path(args, repo_path);
        if (!worker_args) {
            free(mutation_project);
            free(repo_path);
            free(mode_str);
            free(name_override);
            return cbm_mcp_text_result("failed to prepare supervised index request", true);
        }
        char *supervised = index_run_supervised(srv, worker_args);
        free(worker_args);
        if (supervised) {
            free(mutation_project);
            free(repo_path);
            free(mode_str);
            free(name_override);
            return supervised;
        }
        free(mutation_project);
        free(repo_path);
        free(mode_str);
        free(name_override);
        return cbm_mcp_text_result(
            "index supervision failed before a contained worker could start; no "
            "in-process fallback was attempted",
            true);
    }

    if (!mcp_project_mutation_begin(srv, mutation_project)) {
        free(mutation_project);
        free(repo_path);
        free(mode_str);
        free(name_override);
        return cbm_mcp_text_result("index operation blocked by another mutation for this project",
                                   true);
    }
    if (mcp_request_cancelled(srv)) {
        mcp_project_mutation_end(srv, mutation_project);
        free(mutation_project);
        free(repo_path);
        free(mode_str);
        free(name_override);
        return cbm_mcp_text_result("index operation cancelled for this request", true);
    }

    cbm_index_mode_t mode = CBM_MODE_FULL;
    if (mode_str && strcmp(mode_str, "fast") == 0) {
        mode = CBM_MODE_FAST;
    } else if (mode_str && strcmp(mode_str, "moderate") == 0) {
        mode = CBM_MODE_MODERATE;
    }
    free(mode_str);

    bool persistence = cbm_mcp_get_bool_arg(args, "persistence");

    cbm_pipeline_t *p = cbm_pipeline_new(repo_path, NULL, mode);
    if (!p) {
        mcp_project_mutation_end(srv, mutation_project);
        free(mutation_project);
        free(name_override);
        free(repo_path);
        return cbm_mcp_text_result("failed to create pipeline", true);
    }
    if (name_override && name_override[0] && !cbm_pipeline_set_project_name(p, name_override)) {
        cbm_pipeline_free(p);
        mcp_project_mutation_end(srv, mutation_project);
        free(mutation_project);
        free(name_override);
        free(repo_path);
        return cbm_mcp_text_result("invalid project name", true);
    }
    free(name_override);
    cbm_pipeline_set_persistence(p, persistence);

    char *project_name = heap_strdup(cbm_pipeline_project_name(p));

    /* Bootstrap from artifact if no local DB exists */
    try_artifact_bootstrap(project_name, repo_path);

    /* Close cached store — pipeline will delete + recreate the .db file */
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
        srv->store = NULL;
    }
    free(srv->current_project);
    srv->current_project = NULL;

    /* Serialize pipeline runs to prevent concurrent writes.
     * Track active pipeline so signal handler and notifications/cancelled
     * can cancel it mid-run. */
    cbm_pipeline_lock();
    cbm_pipeline_bind_cancel_flag(p, &srv->pipeline_cancel_requested);
    srv->active_pipeline = p;
    int rc = cbm_pipeline_run(p);
    srv->active_pipeline = NULL;
    cbm_pipeline_unlock();

    /* Capture the excluded-subtree list (#411) while the pipeline (which owns
     * the strings) is still alive — the response builder copies them into the
     * JSON doc, so they need only outlive that call, not cbm_pipeline_free. */
    char **excluded_dirs = NULL;
    int excluded_count = 0;
    cbm_pipeline_get_excluded(p, &excluded_dirs, &excluded_count);

    /* Capture the per-file skip list (Stage 2 / Track B) while the pipeline
     * still owns the strings; the response builder copies them into the doc. */
    cbm_file_error_t *file_errors = NULL;
    int file_error_count = 0;
    cbm_pipeline_get_file_errors(p, &file_errors, &file_error_count);

    cbm_mem_collect(); /* return mimalloc pages to OS after large indexing */

    /* Invalidate cached store so next query reopens the fresh database */
    if (srv->owns_store && srv->store) {
        cbm_store_close(srv->store);
        srv->store = NULL;
    }
    free(srv->current_project);
    srv->current_project = NULL;

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "project", project_name);

    if (rc == 0) {
        /* Write the per-run logfile ONLY when there were skips (no logfile on a
         * clean run). The FULL list goes to the file; the JSON caps at 50. */
        char logfile_path[CBM_SZ_1K];
        logfile_path[0] = '\0';
        bool has_logfile = write_skip_logfile(project_name, file_errors, file_error_count,
                                              logfile_path, sizeof(logfile_path));
        bool degraded = build_index_success_response(
            srv, doc, root, project_name, repo_path, persistence, p, excluded_dirs, excluded_count,
            file_errors, file_error_count, has_logfile ? logfile_path : NULL);
        yyjson_mut_obj_add_str(doc, root, "status", degraded ? "degraded" : "indexed");
        if (cbm_pipeline_had_format_migration(p)) {
            yyjson_mut_obj_add_bool(doc, root, "format_migration", true);
        }
    } else {
        yyjson_mut_obj_add_str(doc, root, "status", "error");
        yyjson_mut_obj_add_str(doc, root, "hint",
                               "Pipeline failed. Check repo_path exists and contains source files. "
                               "Try mode='fast' for a quicker diagnostic run.");
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    /* Free the pipeline only after the response doc copied the excluded list.
     * Supervised worker: skip the deep free — the process exits right after
     * handing over the response (main.c fast-exits), and piecemeal-freeing a
     * multi-GB graph before process death costs minutes on kernel-scale repos;
     * the OS reclaims it wholesale at exit. In-process paths (tests, kill
     * switch, degrade) still free normally. */
    if (cbm_index_worker_active()) {
        cbm_log_info("index.worker.fast_exit", "skip", "pipeline_free");
    } else {
        cbm_pipeline_free(p);
    }
    free(project_name);
    free(repo_path);

    mcp_project_mutation_end(srv, mutation_project);
    free(mutation_project);

    char *result = cbm_mcp_text_result(json, rc != 0);
    free(json);
    return result;
}

/* ── get_code_snippet ─────────────────────────────────────────── */

/* Copy a node from an array into a heap-allocated standalone node. */
static __attribute__((unused)) void copy_node(const cbm_node_t *src, cbm_node_t *dst) {
    dst->id = src->id;
    dst->project = heap_strdup(src->project);
    dst->label = heap_strdup(src->label);
    dst->name = heap_strdup(src->name);
    dst->qualified_name = heap_strdup(src->qualified_name);
    dst->file_path = heap_strdup(src->file_path);
    dst->start_line = src->start_line;
    dst->end_line = src->end_line;
    dst->properties_json = src->properties_json ? heap_strdup(src->properties_json) : NULL;
}

/* Build a JSON suggestions response for ambiguous or fuzzy results. */
static __attribute__((unused)) char *snippet_suggestions(const char *input, cbm_node_t *nodes, int count) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "status", "ambiguous");

    char msg[CBM_SZ_512];
    snprintf(msg, sizeof(msg),
             "%d matches for \"%s\". Pick a qualified_name from suggestions below, "
             "or use search_graph(name_pattern=\"...\") to narrow results.",
             count, input);
    yyjson_mut_obj_add_str(doc, root, "message", msg);

    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    for (int i = 0; i < count; i++) {
        yyjson_mut_val *s = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, s, "qualified_name",
                               nodes[i].qualified_name ? nodes[i].qualified_name : "");
        yyjson_mut_obj_add_str(doc, s, "name", nodes[i].name ? nodes[i].name : "");
        yyjson_mut_obj_add_str(doc, s, "label", nodes[i].label ? nodes[i].label : "");
        yyjson_mut_obj_add_str(doc, s, "file_path", nodes[i].file_path ? nodes[i].file_path : "");
        yyjson_mut_arr_append(arr, s);
    }
    yyjson_mut_obj_add_val(doc, root, "suggestions", arr);

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* Resolve an absolute path from root_path + file_path, verify containment,
 * and read source lines. Sets *out_abs_path (caller frees). Returns source
 * string (caller frees) or NULL if path is invalid/unreadable. */
/* True only when abs_path, after realpath/_fullpath resolution (which collapses
 * `..` and resolves symlinks/junctions), stays within root_path. This is the
 * single containment guard every MCP file-read sink must pass before reading a
 * file into a tool response: both the snippet path (resolve_snippet_source) and
 * the search path (attach_result_source) route through it, so a result whose
 * indexed path escapes the project root — via a `..` segment, or a symlink /
 * Windows junction picked up during discovery — is never read back out. */
/* Canonicalize `path` (resolve symlinks/junctions and `..`) into `out`
 * (>= CBM_SZ_4K bytes); returns true on success. Isolating the per-OS resolver
 * keeps cbm_path_within_root's control flow unconditional: the previous `#ifdef`
 * opened the `if (...) {` brace in one branch and a different one in the other,
 * sharing a single close brace — legal C, but it splits the function's braces
 * across preprocessor branches, which defeats source-level tooling that parses
 * without the preprocessor (and left this function unindexed in the graph). */

static char *resolve_snippet_source(const char *root_path, const char *file_path, int start,
                                    int end, char **out_abs_path) {
    *out_abs_path = NULL;
    if (!root_path || !file_path) {
        return NULL;
    }
    size_t apsz = strlen(root_path) + strlen(file_path) + MCP_SEPARATOR;
    char *abs_path = malloc(apsz);
    snprintf(abs_path, apsz, "%s/%s", root_path, file_path);

    *out_abs_path = abs_path;
    if (cbm_path_within_root(root_path, abs_path)) {
        return read_file_lines(abs_path, start, end);
    }
    return NULL;
}

static bool utf8_is_cont(unsigned char c) {
    return (c & 0xC0) == 0x80;
}

static char *sanitize_utf8_lossy(const char *s) {
    enum {
        UTF8_REPLACEMENT_LEN = 3,
        UTF8_THREE_BYTE_LEN = 3,
        UTF8_FOUR_BYTE_LEN = 4,
        UTF8_FOURTH_BYTE = 3,
    };
    if (!s) {
        return NULL;
    }
    size_t len = strlen(s);
    if (len > (((size_t)-1) - SKIP_ONE) / UTF8_REPLACEMENT_LEN) {
        return NULL;
    }
    char *out = malloc(len * UTF8_REPLACEMENT_LEN + SKIP_ONE);
    if (!out) {
        return NULL;
    }

    const unsigned char *p = (const unsigned char *)s;
    const unsigned char *end = p + len;
    unsigned char *dst = (unsigned char *)out;
    while (p < end) {
        unsigned char c = *p;
        size_t n = 0;
        if (c < 0x80) {
            n = 1;
        } else if (c >= 0xC2 && c <= 0xDF && p + 1 < end && utf8_is_cont(p[1])) {
            n = 2;
        } else if (c == 0xE0 && p + 2 < end && p[1] >= 0xA0 && p[1] <= 0xBF && utf8_is_cont(p[2])) {
            n = UTF8_THREE_BYTE_LEN;
        } else if (c >= 0xE1 && c <= 0xEC && p + 2 < end && utf8_is_cont(p[1]) &&
                   utf8_is_cont(p[2])) {
            n = UTF8_THREE_BYTE_LEN;
        } else if (c == 0xED && p + 2 < end && p[1] >= 0x80 && p[1] <= 0x9F && utf8_is_cont(p[2])) {
            n = UTF8_THREE_BYTE_LEN;
        } else if (c >= 0xEE && c <= 0xEF && p + 2 < end && utf8_is_cont(p[1]) &&
                   utf8_is_cont(p[2])) {
            n = UTF8_THREE_BYTE_LEN;
        } else if (c == 0xF0 && p + UTF8_FOURTH_BYTE < end && p[1] >= 0x90 && p[1] <= 0xBF &&
                   utf8_is_cont(p[2]) && utf8_is_cont(p[UTF8_FOURTH_BYTE])) {
            n = UTF8_FOUR_BYTE_LEN;
        } else if (c >= 0xF1 && c <= 0xF3 && p + UTF8_FOURTH_BYTE < end && utf8_is_cont(p[1]) &&
                   utf8_is_cont(p[2]) && utf8_is_cont(p[UTF8_FOURTH_BYTE])) {
            n = UTF8_FOUR_BYTE_LEN;
        } else if (c == 0xF4 && p + UTF8_FOURTH_BYTE < end && p[1] >= 0x80 && p[1] <= 0x8F &&
                   utf8_is_cont(p[2]) && utf8_is_cont(p[UTF8_FOURTH_BYTE])) {
            n = UTF8_FOUR_BYTE_LEN;
        }

        if (n > 0) {
            memcpy(dst, p, n);
            dst += n;
            p += n;
        } else {
            *dst++ = 0xEF;
            *dst++ = 0xBF;
            *dst++ = 0xBD;
            p++;
        }
    }
    *dst = '\0';
    return out;
}

/* Build an enriched snippet response for a resolved node. */
/* Add a string array to a JSON object (no-op if count == 0). */
static void add_string_array(yyjson_mut_doc *doc, yyjson_mut_val *obj, const char *key,
                             char **strings, int count) {
    if (count <= 0) {
        return;
    }
    yyjson_mut_val *arr = yyjson_mut_arr(doc);
    for (int i = 0; i < count; i++) {
        yyjson_mut_arr_add_str(doc, arr, strings[i]);
    }
    yyjson_mut_obj_add_val(doc, obj, key, arr);
}

/* get_code_snippet coverage note (#963): if the resolved node's file is
 * flagged parse_partial, warn that the graph may under-report this file.
 * Correlated by construction — the result names its file. (An entirely-
 * skipped file cannot appear here: it has no nodes to resolve a snippet
 * from.) */
static void add_snippet_coverage_note(yyjson_mut_doc *doc, yyjson_mut_val *root_obj,
                                      cbm_store_t *store, const cbm_node_t *node) {
    if (!node->file_path || !node->file_path[0] || !node->project) {
        return;
    }
    cbm_coverage_row_t *rows = NULL;
    int count = 0;
    if (cbm_store_coverage_get_path(store, node->project, node->file_path, &rows, &count) !=
        CBM_STORE_OK) {
        return;
    }
    for (int i = 0; i < count; i++) {
        if (rows[i].rel_path && strcmp(rows[i].rel_path, node->file_path) == 0 && rows[i].kind &&
            strcmp(rows[i].kind, "parse_partial") == 0) {
            char note[CBM_SZ_1K];
            snprintf(note, sizeof(note),
                     "This file was only PARTIALLY indexed — line range(s) %s could not be "
                     "parsed, so constructs there may be missing from the graph (callers/callees "
                     "and search results can under-report this file). The source above is ground "
                     "truth. (best-effort signal)",
                     rows[i].detail && rows[i].detail[0] ? rows[i].detail : "?");
            yyjson_mut_obj_add_strcpy(doc, root_obj, "coverage_note", note);
            break;
        }
    }
    cbm_store_free_coverage(rows, count);
}

static __attribute__((unused)) char *build_snippet_response(cbm_mcp_server_t *srv, cbm_node_t *node,
                                    const char *match_method, bool include_neighbors,
                                    cbm_node_t *alternatives, int alt_count) {
    char *root_path = get_project_root(srv, node->project);

    int start = node->start_line > 0 ? node->start_line : SKIP_ONE;
    int end = node->end_line > start ? node->end_line : start + SNIPPET_DEFAULT_LINES;
    /* Context-bomb guard: a structural node (Module/File) spans its whole file,
     * so an unclipped read returned the ENTIRE source — a field-eval agent that
     * fell back to a Module snippet pulled 400KB in one call. Cap the line span
     * (far above any real function) and flag it; the exact range is still in
     * start_line/end_line for a targeted re-read. */
    bool snippet_clipped = false;
    if (end - start + 1 > MCP_SNIPPET_MAX_LINES) {
        end = start + MCP_SNIPPET_MAX_LINES - 1;
        snippet_clipped = true;
    }
    char *abs_path = NULL;
    char *source = resolve_snippet_source(root_path, node->file_path, start, end, &abs_path);

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    yyjson_mut_obj_add_str(doc, root_obj, "name", node->name ? node->name : "");
    yyjson_mut_obj_add_str(doc, root_obj, "qualified_name",
                           node->qualified_name ? node->qualified_name : "");
    yyjson_mut_obj_add_str(doc, root_obj, "label", node->label ? node->label : "");

    const char *display_path = "";
    if (abs_path) {
        display_path = abs_path;
    } else if (node->file_path) {
        display_path = node->file_path;
    }
    yyjson_mut_obj_add_str(doc, root_obj, "file_path", display_path);
    yyjson_mut_obj_add_int(doc, root_obj, "start_line", start);
    yyjson_mut_obj_add_int(doc, root_obj, "end_line", end);
    if (snippet_clipped) {
        yyjson_mut_obj_add_bool(doc, root_obj, "source_clipped", true);
        yyjson_mut_obj_add_int(doc, root_obj, "clipped_at_lines", MCP_SNIPPET_MAX_LINES);
    }

    if (source) {
        char *safe_source = sanitize_utf8_lossy(source);
        if (safe_source) {
            yyjson_mut_obj_add_strcpy(doc, root_obj, "source", safe_source);
            free(safe_source);
        } else {
            yyjson_mut_obj_add_str(doc, root_obj, "source", "(source not available)");
        }
    } else {
        yyjson_mut_obj_add_str(doc, root_obj, "source", "(source not available)");
    }

    /* match_method — omitted for exact matches */
    if (match_method) {
        yyjson_mut_obj_add_str(doc, root_obj, "match_method", match_method);
    }

    /* No property-blob enrichment: the verbatim source IS the payload here —
     * signature/docstring are literally in it, and the similarity internals
     * (fp/sp/bt) plus metric fields were 41% of the response for zero agent
     * value. Metrics stay reachable via search_graph fields=[...]. */
    yyjson_doc *props_doc = NULL;

    /* Caller/callee counts — store already resolved by calling handler */
    cbm_store_t *store = srv->store;
    int in_deg = 0;
    int out_deg = 0;
    cbm_store_node_degree(store, node->id, &in_deg, &out_deg);
    yyjson_mut_obj_add_int(doc, root_obj, "callers", in_deg);
    yyjson_mut_obj_add_int(doc, root_obj, "callees", out_deg);

    add_snippet_coverage_note(doc, root_obj, store, node);

    char **nb_callers = NULL;
    int nb_caller_count = 0;
    char **nb_callees = NULL;
    int nb_callee_count = 0;
    if (include_neighbors) {
        cbm_store_node_neighbor_names(store, node->id, MCP_DEFAULT_LIMIT, &nb_callers,
                                      &nb_caller_count, &nb_callees, &nb_callee_count);
        add_string_array(doc, root_obj, "caller_names", nb_callers, nb_caller_count);
        add_string_array(doc, root_obj, "callee_names", nb_callees, nb_callee_count);
    }

    /* Alternatives (when auto-resolved from ambiguous) */
    if (alternatives && alt_count > 0) {
        yyjson_mut_val *arr = yyjson_mut_arr(doc);
        for (int i = 0; i < alt_count; i++) {
            yyjson_mut_val *a = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_str(doc, a, "qualified_name",
                                   alternatives[i].qualified_name ? alternatives[i].qualified_name
                                                                  : "");
            yyjson_mut_obj_add_str(doc, a, "file_path",
                                   alternatives[i].file_path ? alternatives[i].file_path : "");
            yyjson_mut_arr_append(arr, a);
        }
        yyjson_mut_obj_add_val(doc, root_obj, "alternatives", arr);
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    yyjson_doc_free(props_doc); /* safe if NULL */
    for (int i = 0; i < nb_caller_count; i++) {
        free(nb_callers[i]);
    }
    for (int i = 0; i < nb_callee_count; i++) {
        free(nb_callees[i]);
    }
    free(nb_callers);
    free(nb_callees);
    free(root_path);
    free(abs_path);
    free(source);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* ── manage_adr ───────────────────────────────────────────────── */

typedef struct {
    yyjson_mut_doc *doc;
    yyjson_mut_val *arr;
} adr_sections_ctx_t;

static void adr_sections_cb(void *ctx, const cbm_adr_heading_t *h) {
    adr_sections_ctx_t *c = (adr_sections_ctx_t *)ctx;
    char hdr[CBM_SZ_1K];
    snprintf(hdr, sizeof(hdr), "## %.*s", h->name_len, h->name);
    yyjson_mut_arr_add_strcpy(c->doc, c->arr, hdr);
}

/* ADR "sections" mode: list the section headings of the ADR.
 *
 * This uses cbm_adr_scan_headings(), the SAME classifier the section-write
 * path splices with. It used to list any '#'-prefixed line, so a '## Foo' in
 * prose — or inside a fenced code block — was reported as a section that no
 * write could target. Two components disagreeing about what a section is was
 * how a section write came to be able to destroy one. */
static void adr_list_sections_from_content(yyjson_mut_doc *doc, yyjson_mut_val *root_obj,
                                           const char *content) {
    yyjson_mut_val *sections = yyjson_mut_arr(doc);
    adr_sections_ctx_t ctx = {doc, sections};
    if (content && cbm_adr_scan_headings(content, adr_sections_cb, &ctx) != CBM_STORE_OK) {
        /* The ambiguity that refuses a section write is reported here too,
         * rather than answering with a heading list that is quietly partial. */
        yyjson_mut_obj_add_str(doc, root_obj, "sections_status", "unterminated_code_fence");
    }
    yyjson_mut_obj_add_val(doc, root_obj, "sections", sections);
}

/* Read the legacy file-based ADR (<root>/.codebase-memory/adr.md), used by
 * older versions. Returns a heap buffer (caller frees) or NULL if missing/
 * empty. Kept only to migrate old ADRs into the store (#256). */
static char *adr_read_legacy_file(const char *root_path) {
    if (!root_path) {
        return NULL;
    }
    char adr_path[CBM_SZ_4K];
    snprintf(adr_path, sizeof(adr_path), "%s/.codebase-memory/adr.md", root_path);
    FILE *fp = cbm_fopen(adr_path, "r");
    if (!fp) {
        return NULL;
    }
    (void)fseek(fp, 0, SEEK_END);
    long sz = ftell(fp);
    if (sz <= 0) {
        (void)fclose(fp);
        return NULL;
    }
    (void)fseek(fp, 0, SEEK_SET);
    char *buf = malloc((size_t)sz + SKIP_ONE);
    if (!buf) {
        (void)fclose(fp);
        return NULL;
    }
    size_t n = fread(buf, SKIP_ONE, (size_t)sz, fp);
    buf[n] = '\0';
    (void)fclose(fp);
    if (buf[0] == '\0') {
        free(buf);
        return NULL;
    }
    return buf;
}

#define ADR_EMPTY_HINT                                                             \
    "No ADR yet. Create one with manage_adr(mode='update', "                       \
    "content='## PURPOSE\\n...\\n\\n## STACK\\n...\\n\\n## ARCHITECTURE\\n..."     \
    "\\n\\n## PATTERNS\\n...\\n\\n## TRADEOFFS\\n...\\n\\n## PHILOSOPHY\\n...'). " \
    "For guided creation: explore the codebase with get_architecture, "            \
    "then draft and store. Sections: PURPOSE, STACK, ARCHITECTURE, "               \
    "PATTERNS, TRADEOFFS, PHILOSOPHY."

/* resolve_store opens file-backed projects query-only. A mutation must release
 * that reader before opening a dedicated writer because atomic publication uses
 * a self-contained DELETE-mode database that switches back to WAL on write. */
static cbm_store_t *open_adr_store_for_write(cbm_mcp_server_t *srv, cbm_store_t *resolved,
                                             cbm_store_t **owned_rw) {
    if (!srv || !resolved || !owned_rw) {
        return NULL;
    }
    const char *resolved_db_path = cbm_store_db_path(resolved);
    if (!resolved_db_path) {
        return resolved;
    }
    char *rw_path = heap_strdup(resolved_db_path);
    if (!rw_path) {
        return NULL;
    }
    invalidate_cached_store(srv);
    *owned_rw = cbm_store_open_path(rw_path);
    free(rw_path);
    return *owned_rw;
}

/* Parsed `section_updates` for mode='set_sections'.
 *
 * mode='update' replaces the whole document, so adding one entry costs a full
 * re-send and the stored ADR is only ever as good as that round-trip. Writing
 * named sections instead leaves the rest of the document as the authority for
 * itself — and, unlike a whole-document append, applying the same request
 * twice yields the same document, so a client that retries after a lost
 * response cannot silently duplicate content. */
typedef struct {
    char *keys[PROPS_MAX];
    char *values[PROPS_MAX];
    int count;
    /* Rejection reason, or NULL when the request parsed cleanly. Set means no
     * store was opened and nothing was written. */
    const char *status;
    const char *error;
} adr_section_updates_t;

static void adr_section_updates_free(adr_section_updates_t *u) {
    for (int i = 0; i < u->count; i++) {
        free(u->keys[i]);
        free(u->values[i]);
    }
    u->count = 0;
}

static bool adr_collect_section_update(adr_section_updates_t *u, yyjson_val *key, yyjson_val *val) {
    const char *name = yyjson_get_str(key);
    if (!name || !name[0]) {
        u->status = "invalid_section_updates";
        u->error = "'section_updates' keys must be non-empty section names. "
                   "No ADR write was performed.";
        return false;
    }
    if (!yyjson_is_str(val)) {
        u->status = "invalid_section_updates";
        u->error = "'section_updates' values must be strings (the new body for that section). "
                   "No ADR write was performed.";
        return false;
    }
    const char *body = yyjson_get_str(val);
    /* An empty body would render a heading with nothing under it — a silent
     * content deletion wearing the response shape of an update. Clearing a
     * section is whole-document surgery; that is what mode='update' is for. */
    if (!body || !body[0]) {
        u->status = "empty_section_content";
        u->error = "'section_updates' values must be non-empty; use mode='update' to remove a "
                   "section. No ADR write was performed.";
        return false;
    }
    if (u->count >= PROPS_MAX) {
        u->status = "too_many_sections";
        u->error = "'section_updates' carries more entries than an ADR can hold. "
                   "No ADR write was performed.";
        return false;
    }
    u->keys[u->count] = heap_strdup(name);
    u->values[u->count] = heap_strdup(body);
    if (!u->keys[u->count] || !u->values[u->count]) {
        free(u->keys[u->count]);
        free(u->values[u->count]);
        u->status = "write_error";
        u->error = "out of memory parsing 'section_updates'. No ADR write was performed.";
        return false;
    }
    u->count++;
    return true;
}

static adr_section_updates_t adr_parse_section_updates(const char *args) {
    adr_section_updates_t u;
    memset(&u, 0, sizeof(u));

    yyjson_doc *doc = yyjson_read(args, strlen(args), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *updates =
        (root && yyjson_is_obj(root)) ? yyjson_obj_get(root, "section_updates") : NULL;

    if (!updates) {
        /* Never fall through to 'get': a caller that meant to write must not
         * receive a success-shaped read. */
        u.status = "missing_section_updates";
        u.error = "mode='set_sections' requires 'section_updates', an object mapping section "
                  "name to its new body. No ADR write was performed.";
    } else if (!yyjson_is_obj(updates) || yyjson_obj_size(updates) == 0) {
        u.status = "invalid_section_updates";
        u.error = "'section_updates' must be a non-empty object mapping section name to its new "
                  "body. No ADR write was performed.";
    } else {
        size_t idx = 0;
        size_t max = 0;
        yyjson_val *key = NULL;
        yyjson_val *val = NULL;
        yyjson_obj_foreach(updates, idx, max, key, val) {
            if (!adr_collect_section_update(&u, key, val)) {
                adr_section_updates_free(&u);
                break;
            }
        }
    }
    yyjson_doc_free(doc);
    return u;
}

/* Build the rejection payload for a set_sections request that never reached a
 * store. Caller frees. */
static char *adr_section_updates_error(const adr_section_updates_t *u) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);
    yyjson_mut_obj_add_str(doc, root_obj, "status", u->status);
    yyjson_mut_obj_add_strcpy(doc, root_obj, "error", u->error);
    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    return json;
}

static char *handle_manage_adr(cbm_mcp_server_t *srv, const char *args) {
    char *project = get_project_arg(args);
    char *mode_str = cbm_mcp_get_string_arg(args, "mode");
    char *content = cbm_mcp_get_string_arg(args, "content");

    if (!mode_str) {
        mode_str = heap_strdup("get");
    }

    /* `sections` used to be advertised as an input argument, but it was never
     * consumed: mode=update still replaced the whole document. Reject the old
     * shape explicitly before opening a store so a stale client cannot mistake
     * a whole-document replacement for a section-scoped update. */
    bool has_sections_arg = false;
    yyjson_doc *args_doc = yyjson_read(args, strlen(args), 0);
    if (args_doc) {
        yyjson_val *args_root = yyjson_doc_get_root(args_doc);
        has_sections_arg =
            args_root && yyjson_is_obj(args_root) && yyjson_obj_get(args_root, "sections") != NULL;
        yyjson_doc_free(args_doc);
    }
    if (has_sections_arg) {
        free(project);
        free(mode_str);
        free(content);
        return cbm_mcp_text_result(
            "{\"status\":\"invalid_arguments\",\"error\":\"The sections argument is not an "
            "update primitive and has been removed. No ADR write was performed.\"}",
            true);
    }

    bool set_sections_mode = (strcmp(mode_str, "set_sections") == 0);
    adr_section_updates_t updates;
    memset(&updates, 0, sizeof(updates));
    char section_key_err[CBM_SZ_256] = "";
    if (set_sections_mode) {
        updates = adr_parse_section_updates(args);
        /* Any heading name is writable — the canonical six are a convention,
         * not a privilege. What is still refused is a name that could not
         * round-trip through a "## NAME" line (empty, '#'-leading, newline- or
         * edge-whitespace-bearing, over-long): such a name would scan back as
         * a different heading or as none, so a second identical write would
         * append a duplicate instead of being a no-op. */
        if (!updates.status && cbm_adr_validate_section_keys(
                                   (const char **)updates.keys, updates.count, section_key_err,
                                   (int)sizeof(section_key_err)) != CBM_STORE_OK) {
            adr_section_updates_free(&updates);
            updates.status = "invalid_section_name";
            updates.error = section_key_err;
        }
        if (updates.status) {
            /* Reject before taking the project lease or opening a store: a
             * malformed write must not block an index, and must not read. */
            char *err = adr_section_updates_error(&updates);
            adr_section_updates_free(&updates);
            free(project);
            free(mode_str);
            free(content);
            char *res = cbm_mcp_text_result(err, true);
            free(err);
            return res;
        }
    }

    /* This classification is load-bearing. A mode missing from it takes no
     * per-project mutation lease, resolves the store query-only, and never
     * reaches open_adr_store_for_write — so its write would be attempted
     * through a read-only handle, concurrently with an active index. */
    bool write_request =
        (content && (strcmp(mode_str, "update") == 0 || strcmp(mode_str, "store") == 0)) ||
        set_sections_mode;
    bool mutation_held = false;
    if (write_request && project) {
        mutation_held = mcp_project_mutation_begin(srv, project);
        if (!mutation_held) {
            adr_section_updates_free(&updates);
            free(project);
            free(mode_str);
            free(content);
            return cbm_mcp_text_result("project operation cancelled or blocked by an active index",
                                       true);
        }
        if (mcp_request_cancelled(srv)) {
            mcp_project_mutation_end(srv, project);
            adr_section_updates_free(&updates);
            free(project);
            free(mode_str);
            free(content);
            return cbm_mcp_text_result("project operation cancelled for this request", true);
        }
    }

    /* ADRs are stored in the SQLite store (project_summaries), the SAME
     * backend the UI /api/adr endpoints use — so writes via the MCP tool and
     * the UI are visible to each other (#256). */
    store_recovery_status_t recovery_status = STORE_RECOVERY_NONE;
    cbm_store_t *resolved =
        resolve_store_internal(srv, project, mutation_held, !write_request, &recovery_status);
    if (!resolved) {
        char *res = NULL;
        if (recovery_status == STORE_RECOVERY_BUSY) {
            res = cbm_mcp_text_result("project is busy; retry after indexing", true);
        } else if (recovery_status == STORE_RECOVERY_TRY_GUARD_UNAVAILABLE) {
            res =
                cbm_mcp_text_result("project recovery requires a nonblocking mutation guard", true);
        } else {
            char *err = build_no_store_error(project);
            res = cbm_mcp_text_result(err, true);
            free(err);
        }
        if (mutation_held) {
            mcp_project_mutation_end(srv, project);
        }
        adr_section_updates_free(&updates);
        free(project);
        free(mode_str);
        free(content);
        return res;
    }

    cbm_store_t *store = resolved;
    cbm_store_t *owned_rw = NULL;
    if (write_request) {
        store = open_adr_store_for_write(srv, resolved, &owned_rw);
        if (!store) {
            if (mutation_held) {
                mcp_project_mutation_end(srv, project);
            }
            adr_section_updates_free(&updates);
            free(project);
            free(mode_str);
            free(content);
            return cbm_mcp_text_result("failed to open writable ADR store", true);
        }
    }

    /* One-time migration: older versions wrote ADRs to a file at
     * <root>/.codebase-memory/adr.md. A read never waits for the project lease:
     * it returns the legacy content immediately and attempts migration only if
     * a nonblocking acquire succeeds. */
    cbm_adr_t adr;
    memset(&adr, 0, sizeof(adr));
    bool have_adr = (cbm_store_adr_get(store, project, &adr) == CBM_STORE_OK);
    char *legacy = NULL;
    if (!have_adr && !write_request) {
        char *root_path = project_root_from_store(store, project);
        legacy = adr_read_legacy_file(root_path);
        free(root_path);
        if (legacy && mcp_project_mutation_try_begin(srv, project)) {
            if (!mcp_request_cancelled(srv)) {
                /* A publisher may have completed before the lease was granted.
                 * File-backed stores must reopen after acquisition and trust
                 * only that generation. Embedded stores have no publication
                 * boundary, so retain their live handle. */
                if (cbm_store_db_path(resolved)) {
                    invalidate_cached_store(srv);
                    resolved = NULL;
                    store = NULL;
                    resolved = resolve_store_internal(srv, project, true, false, NULL);
                }
                if (resolved) {
                    store = open_adr_store_for_write(srv, resolved, &owned_rw);
                    if (store) {
                        have_adr = (cbm_store_adr_get(store, project, &adr) == CBM_STORE_OK);
                        if (!have_adr &&
                            cbm_store_adr_store(store, project, legacy) == CBM_STORE_OK) {
                            have_adr = (cbm_store_adr_get(store, project, &adr) == CBM_STORE_OK);
                        }
                    }
                }
            }
            mcp_project_mutation_end(srv, project);
        }
    }

    /* A set_sections write must see a legacy file-backed ADR too. The
     * migration above deliberately runs on the read path only — it must never
     * block on the lease — so the write path reads the file here, where the
     * exclusive project lease and a writable store are already held. Merging
     * onto an empty document instead would silently discard an ADR the user
     * still has on disk. */
    char *legacy_seed = NULL;
    if (set_sections_mode && !have_adr) {
        char *root_path = project_root_from_store(store, project);
        legacy_seed = adr_read_legacy_file(root_path);
        free(root_path);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root_obj = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root_obj);

    bool is_error = false;
    const char *adr_content = have_adr ? adr.content : legacy;
    if (set_sections_mode) {
        /* cbm_store_adr_update_sections requires an existing row — its
         * contract, pinned by TEST(adr_update_no_existing). Seed one when the
         * project has none, so the mode degrades to a plain create: the legacy
         * document when there is one, an empty document otherwise. */
        bool base_present = have_adr;
        bool seeded_empty = false;
        if (!base_present) {
            const char *seed = legacy_seed ? legacy_seed : "";
            if (cbm_store_adr_store(store, project, seed) == CBM_STORE_OK) {
                base_present = true;
                seeded_empty = (legacy_seed == NULL);
            }
        }
        cbm_adr_t updated;
        memset(&updated, 0, sizeof(updated));
        int section_rc = base_present ? cbm_store_adr_update_sections(
                                            store, project, (const char **)updates.keys,
                                            (const char **)updates.values, updates.count, &updated)
                                      : CBM_STORE_ERR;
        if (section_rc == CBM_STORE_OK) {
            yyjson_mut_obj_add_str(doc, root_obj, "status", "sections_updated");
            yyjson_mut_obj_add_str(doc, root_obj, "semantics",
                                   "named_sections_replaced_rest_preserved");
            yyjson_mut_obj_add_uint(doc, root_obj, "sections_written", (uint64_t)updates.count);
            /* Callers confirm the write landed without re-fetching the ADR. */
            yyjson_mut_obj_add_uint(doc, root_obj, "content_length",
                                    (uint64_t)strlen(updated.content));
            cbm_store_adr_free(&updated);
        } else {
            /* Undo an empty seed. A rejected write must not leave the project
             * holding a blank ADR where `get` used to answer no_adr. A legacy
             * seed is a real migration and is kept. */
            if (seeded_empty) {
                (void)cbm_store_adr_delete(store, project);
            }
            yyjson_mut_obj_add_str(doc, root_obj, "status", "write_error");
            const char *store_err = cbm_store_error(store);
            if (store_err && store_err[0]) {
                yyjson_mut_obj_add_strcpy(doc, root_obj, "error", store_err);
            }
            is_error = true;
        }
    } else if (write_request) {
        if (cbm_store_adr_store(store, project, content) == CBM_STORE_OK) {
            yyjson_mut_obj_add_str(doc, root_obj, "status", "updated");
            yyjson_mut_obj_add_str(doc, root_obj, "semantics", "whole_document_replaced");
        } else {
            yyjson_mut_obj_add_str(doc, root_obj, "status", "write_error");
            is_error = true;
        }
    } else if (strcmp(mode_str, "sections") == 0) {
        adr_list_sections_from_content(doc, root_obj, adr_content);
    } else { /* get */
        if (adr_content) {
            yyjson_mut_obj_add_strcpy(doc, root_obj, "content", adr_content);
        } else {
            yyjson_mut_obj_add_str(doc, root_obj, "content", "");
            yyjson_mut_obj_add_str(doc, root_obj, "status", "no_adr");
            yyjson_mut_obj_add_str(doc, root_obj, "adr_hint", ADR_EMPTY_HINT);
        }
    }

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);
    if (have_adr) {
        cbm_store_adr_free(&adr);
    }
    if (owned_rw) {
        cbm_store_close(owned_rw);
    }
    if (mutation_held) {
        mcp_project_mutation_end(srv, project);
    }
    adr_section_updates_free(&updates);
    free(legacy_seed);
    free(legacy);
    free(project);
    free(mode_str);
    free(content);

    char *result = cbm_mcp_text_result(json, is_error);
    free(json);
    return result;
}

/* ── ingest_traces ────────────────────────────────────────────── */

static char *handle_ingest_traces(cbm_mcp_server_t *srv, const char *args) {
    (void)srv;
    /* Parse traces array from JSON args */
    yyjson_doc *adoc = yyjson_read(args, strlen(args), 0);
    int trace_count = 0;

    if (adoc) {
        yyjson_val *aroot = yyjson_doc_get_root(adoc);
        yyjson_val *traces = yyjson_obj_get(aroot, "traces");
        if (traces && yyjson_is_arr(traces)) {
            trace_count = (int)yyjson_arr_size(traces);
        }
        yyjson_doc_free(adoc);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_obj_add_str(doc, root, "status", "accepted");
    yyjson_mut_obj_add_int(doc, root, "traces_received", trace_count);
    yyjson_mut_obj_add_str(doc, root, "note",
                           "Runtime edge creation from traces not yet implemented");

    char *json = yy_doc_to_str(doc);
    yyjson_mut_doc_free(doc);

    char *result = cbm_mcp_text_result(json, false);
    free(json);
    return result;
}

/* ── Tool dispatch ────────────────────────────────────────────── */

static bool mcp_operation_runtime_cancelled(void *context) {
    return mcp_request_cancelled((const cbm_mcp_server_t *)context);
}

static bool mcp_operation_runtime_command_allowed(void *context, const char *command) {
    cbm_mcp_server_t *srv = (cbm_mcp_server_t *)context;
    return !srv || !srv->command_test_hook ||
           srv->command_test_hook(srv->command_test_context, command);
}

static char *mcp_operation_adapter(cbm_mcp_server_t *srv, cbm_operation_id_t operation,
                                   const char *args_json) {
    cbm_operation_runtime_t runtime = {
        .cancelled = mcp_operation_runtime_cancelled,
        .cancelled_context = srv,
        .command_allowed = mcp_operation_runtime_command_allowed,
        .command_allowed_context = srv,
        .command_output_limit_override = srv ? srv->search_output_limit_override : 0,
        .command_override = srv ? srv->search_scan_command_override : NULL,
        .command_timeout_override_ms = srv ? srv->search_scan_timeout_override_ms : 0,
        .command_timeout_override_set = srv ? srv->search_scan_timeout_override_set : false,
    };
    cbm_operation_context_t context = {.runtime = &runtime};
    cbm_operation_result_t result =
        cbm_operation_execute(&context, operation, args_json ? args_json : "{}");
    if (!result.payload) {
        cbm_operation_result_dispose(&result);
        return NULL;
    }
    char *envelope = cbm_mcp_text_result(result.payload, result.is_error);
    cbm_operation_result_dispose(&result);
    return envelope;
}

static char *dispatch_tool(cbm_mcp_server_t *srv, const char *tool_name, const char *args_json) {
    if (!tool_name) {
        return cbm_mcp_text_result("missing tool name", true);
    }
    if (srv && !mcp_tool_allowed(srv->tool_profile, tool_name)) {
        char message[CBM_SZ_256];
        snprintf(message, sizeof(message), "tool '%s' is not available in the %s tool profile",
                 tool_name, mcp_tool_profile_name(srv->tool_profile));
        return cbm_mcp_text_result(message, true);
    }

    if (strcmp(tool_name, "list_projects") == 0) {
        char *adapted = mcp_operation_adapter(srv, CBM_OPERATION_PROJECTS, args_json);
        return adapted;
    }
    if (strcmp(tool_name, "get_graph_schema") == 0) {
        return mcp_operation_adapter(srv, CBM_OPERATION_SCHEMA, args_json);
    }
    if (strcmp(tool_name, "compare_graphs") == 0) {
        return handle_compare_graphs(srv, args_json);
    }
    if (strcmp(tool_name, "search_graph") == 0) {
        char *adapted = mcp_operation_adapter(srv, CBM_OPERATION_SEARCH, args_json);
        return adapted;
    }
    if (strcmp(tool_name, "query_graph") == 0) {
        return mcp_operation_adapter(srv, CBM_OPERATION_QUERY, args_json);
    }
    if (strcmp(tool_name, "index_status") == 0) {
        char *adapted = mcp_operation_adapter(srv, CBM_OPERATION_STATUS, args_json);
        return adapted;
    }
    if (strcmp(tool_name, "check_index_coverage") == 0) {
        char *adapted = mcp_operation_adapter(srv, CBM_OPERATION_COVERAGE, args_json);
        return adapted;
    }
    if (strcmp(tool_name, "delete_project") == 0) {
        return handle_delete_project(srv, args_json);
    }
    if (strcmp(tool_name, "trace_path") == 0 || strcmp(tool_name, "trace_call_path") == 0) {
        char *adapted = mcp_operation_adapter(srv, CBM_OPERATION_TRACE, args_json);
        return adapted;
    }
    if (strcmp(tool_name, "get_architecture") == 0) {
        return mcp_operation_adapter(srv, CBM_OPERATION_ARCHITECTURE, args_json);
    }

    /* Pipeline-dependent tools */
    if (strcmp(tool_name, "index_repository") == 0) {
        return handle_index_repository(srv, args_json);
    }
    if (strcmp(tool_name, "get_code_snippet") == 0) {
        char *adapted = mcp_operation_adapter(srv, CBM_OPERATION_SNIPPET, args_json);
        return adapted;
    }
    if (strcmp(tool_name, "get_file_outline") == 0) {
        return mcp_operation_adapter(srv, CBM_OPERATION_FILE_OUTLINE, args_json);
    }
    if (strcmp(tool_name, "search_code") == 0) {
        return mcp_operation_adapter(srv, CBM_OPERATION_SOURCE_SEARCH, args_json);
    }
    if (strcmp(tool_name, "detect_changes") == 0) {
        return mcp_operation_adapter(srv, CBM_OPERATION_CHANGES, args_json);
    }
    if (strcmp(tool_name, "manage_adr") == 0) {
        return handle_manage_adr(srv, args_json);
    }
    if (strcmp(tool_name, "ingest_traces") == 0) {
        return handle_ingest_traces(srv, args_json);
    }
    char msg[CBM_SZ_256];
    snprintf(msg, sizeof(msg), "unknown tool: %s", tool_name);
    return cbm_mcp_text_result(msg, true);
}

/* File-backed query stores are request-scoped. Keeping one open between MCP
 * calls pins an old database generation after another process atomically
 * replaces the project DB. On Windows it can also prevent that replacement
 * entirely. Embedded/in-memory stores have no path and retain their existing
 * process lifetime. */
static void release_request_store(cbm_mcp_server_t *srv) {
    if (!srv || !srv->owns_store || !srv->store || !cbm_store_db_path(srv->store)) {
        return;
    }
    cbm_store_close(srv->store);
    srv->store = NULL;
    free(srv->current_project);
    srv->current_project = NULL;
    /* The close above frees a connection's worth of page cache. Ask the
     * allocator to hand those pages back now, which keeps a long-lived daemon
     * flat across thousands of request-scoped stores (#581). This only became
     * meaningful once the Windows interposer made the pages mimalloc's: an
     * earlier attempt aimed at the CRT heap instead and could not release
     * them. POSIX already purges on free, so this is a no-op there. */
    cbm_mem_collect();
}

char *cbm_mcp_handle_tool(cbm_mcp_server_t *srv, const char *tool_name, const char *args_json) {
    /* Phase marks bracket the WHOLE request with no unlabelled gap, so growth
     * cannot hide between them (CBM_MEM_PHASES=1; see foundation/mem.h). The
     * "idle" label owns everything outside a request, which is what makes a
     * request-path retainer distinguishable from background growth. */
    cbm_mem_phase_mark("request.scope_begin");
    bool request_scope = !srv || cbm_mcp_server_request_scope_begin(srv);
    if (!request_scope) {
        release_request_store(srv);
        cbm_mem_phase_mark("idle");
        return cbm_mcp_text_result("request cancellation scope unavailable", true);
    }
    cbm_mem_phase_mark("request.dispatch_tool");
    char *result = dispatch_tool(srv, tool_name, args_json);
    cbm_mem_phase_mark("request.scope_end");
    if (srv) {
        cbm_mcp_server_request_scope_end(srv);
    }
    cbm_mem_phase_mark("request.release_store");
    release_request_store(srv);
    cbm_mem_phase_mark("idle");
    /* One census per completed request, so growth can be attributed to a POOL
     * rather than inferred from a process total (#581). Emitted after the
     * request store is released, which is the point where a well-behaved
     * request has given everything back. */
    cbm_mem_census_log("mcp.request");
    return result;
}

/* ── Session detection + auto-index ────────────────────────────── */

/* Detect session root from CWD (fallback: single indexed project from DB). */
static void detect_session(cbm_mcp_server_t *srv) {
    if (srv->session_detected) {
        return;
    }
    srv->session_detected = true;

    /* 1. Try CWD */
    char cwd[CBM_SZ_1K];
    if (getcwd(cwd, sizeof(cwd)) != NULL) {
        const char *home = cbm_get_home_dir();
        /* Skip useless roots: / and $HOME */
        if (strcmp(cwd, "/") != 0 && (home == NULL || strcmp(cwd, home) != 0)) {
            snprintf(srv->session_root, sizeof(srv->session_root), "%s", cwd);
            cbm_log_info("session.root.cwd", "path", cwd);
        }
    }

    /* Derive project name from path — must match cbm_project_name_from_path
     * used by the pipeline, otherwise session queries look for a .db file
     * that doesn't match the indexed project name. */
    if (srv->session_root[0]) {
        char *pname = cbm_project_name_from_path(srv->session_root);
        if (pname) {
            snprintf(srv->session_project, sizeof(srv->session_project), "%s", pname);
            free(pname);
        }
    }
}

/* auto_watch config: gates background watcher registration (default on).
 * Multi-project users can contain a session to its own project with
 * `config set auto_watch false`. */
static bool auto_watch_enabled(cbm_mcp_server_t *srv) {
    if (!srv->config) {
        return true; /* default on */
    }
    return cbm_config_get_bool(srv->config, CBM_CONFIG_AUTO_WATCH, true);
}

/* Register the session project with the background watcher for ongoing
 * change detection — unless auto_watch is disabled. */
static void register_watcher_if_enabled(cbm_mcp_server_t *srv) {
    if (!srv->watcher || srv->session_project[0] == '\0' || srv->session_root[0] == '\0') {
        return;
    }
    if (!auto_watch_enabled(srv)) {
        cbm_log_info("watcher.register.skipped", "reason", "auto_watch_off", "project",
                     srv->session_project);
        return;
    }
    cbm_watcher_watch(srv->watcher, srv->session_project, srv->session_root);
}

/* Background auto-index thread function */
static void *autoindex_thread(void *arg) {
    cbm_mcp_server_t *srv = (cbm_mcp_server_t *)arg;

    cbm_log_info("autoindex.start", "project", srv->session_project, "path", srv->session_root);

    /* #832: use the supervised worker subprocess. Indexing the whole session in
     * this long-lived server thread ratchets RSS (mimalloc v3 does not reclaim the
     * pages worker threads abandon at exit); running it in a child that exits hands
     * 100% of that memory back to the OS every cycle. In a marked host this is a
     * safety boundary: preparation/start failure stops the operation. */
    if (cbm_index_supervisor_should_wrap()) {
        char *resp = index_run_supervised_path(srv, srv->session_root);
        if (resp) {
            free(resp);
            cbm_log_info("autoindex.done", "project", srv->session_project, "mode", "supervised");
            /* Register with watcher for ongoing change detection — gated on
             * auto_watch (#849), same as the in-process branch below. A bare
             * `if (srv->watcher)` would register even when the user set
             * `config set auto_watch false`, since srv->watcher is always set. */
            register_watcher_if_enabled(srv);
            return NULL;
        }
        cbm_log_error("autoindex.supervision_failed", "project", srv->session_project, "action",
                      "fail_closed");
        return NULL;
    }

    cbm_pipeline_t *p = cbm_pipeline_new(srv->session_root, NULL, CBM_MODE_FULL);
    if (!p) {
        cbm_log_warn("autoindex.err", "msg", "pipeline_create_failed");
        return NULL;
    }

    /* Block until any concurrent pipeline finishes */
    cbm_pipeline_lock();
    int rc = cbm_pipeline_run(p);
    cbm_pipeline_unlock();

    cbm_pipeline_free(p);
    cbm_mem_collect(); /* return mimalloc pages to OS after indexing (in-process only) */

    if (rc == 0) {
        cbm_log_info("autoindex.done", "project", srv->session_project);
        register_watcher_if_enabled(srv);
    } else {
        cbm_log_warn("autoindex.err", "msg", "pipeline_run_failed");
    }
    return NULL;
}

bool cbm_mcp_auto_index_within_file_limit(const char *root_path, int file_limit,
                                          int *file_count_out) {
    if (file_count_out) {
        *file_count_out = -1;
    }
    if (!root_path || !root_path[0] || file_limit < 0) {
        return false;
    }
    enum { AUTO_INDEX_COUNT_TIMEOUT_MS = 5000 };
    cbm_discover_opts_t options = {
        .mode = CBM_MODE_FULL,
        .ignore_file = NULL,
        .max_file_size = 0,
    };
    int count = -1;
    cbm_discover_status_t status = cbm_discover_count_bounded(
        root_path, &options, file_limit, cbm_now_ms() + AUTO_INDEX_COUNT_TIMEOUT_MS, &count);
    if (file_count_out) {
        *file_count_out = status == CBM_DISCOVER_LIMIT_EXCEEDED
                              ? (file_limit < INT_MAX ? file_limit + 1 : INT_MAX)
                              : count;
    }
    return status == CBM_DISCOVER_OK;
}

/* Start auto-indexing if configured and project not yet indexed. */
static void maybe_auto_index(cbm_mcp_server_t *srv) {
    if (srv->session_root[0] == '\0') {
        return; /* no session root detected */
    }

    /* Automatic work must honor the same shared workspace boundary as the
     * explicit index_repository worker. Do this before the existing-DB watcher
     * branch and before bounded discovery, so a refused session root begins no
     * automatic observation or indexing. An exact sensitive-root grant remains
     * the shared policy's authenticated override. */
    const char *allowed_root =
        srv->allowed_root_policy_set ? srv->allowed_root : getenv("CBM_ALLOWED_ROOT");
    char boundary_err[CBM_SZ_1K];
    if (!cbm_workspace_root_allowed(srv->session_root, cbm_workspace_home_dir(),
                                    cbm_workspace_cache_dir(), allowed_root, boundary_err,
                                    sizeof(boundary_err))) {
        cbm_log_warn("autoindex.skip", "reason", "workspace_boundary", "detail", boundary_err);
        return;
    }

    /* Check if project already has a DB */
    const char *home = cbm_get_home_dir();
    if (home) {
        char db_check[CBM_SZ_1K];
        snprintf(db_check, sizeof(db_check), "%s/%s.db", cbm_resolve_cache_dir(),
                 srv->session_project);
        if (cbm_file_size(db_check) >= 0) {
            /* Already indexed → register watcher for change detection */
            cbm_log_info("autoindex.skip", "reason", "already_indexed", "project",
                         srv->session_project);
            register_watcher_if_enabled(srv);
            return;
        }
    }

    /* Check auto_index config */
    bool auto_index = false;
    int file_limit = CBM_MCP_DEFAULT_AUTO_INDEX_LIMIT;
    if (srv->config) {
        auto_index = cbm_config_get_bool(srv->config, CBM_CONFIG_AUTO_INDEX, false);
        file_limit = cbm_config_get_int(srv->config, CBM_CONFIG_AUTO_INDEX_LIMIT,
                                        CBM_MCP_DEFAULT_AUTO_INDEX_LIMIT);
    }

    if (!auto_index) {
        cbm_log_info("autoindex.skip", "reason", "disabled", "hint",
                     "run: codebase-memory-cli config set auto_index true");
        return;
    }

    /* Quick tracked-file count check to avoid OOM on massive repos. */
    int file_count = -1;
#ifdef CBM_ENABLE_TEST_SEAMS
    if (srv->auto_index_count_test_hook) {
        srv->auto_index_count_test_hook(srv->auto_index_count_test_context);
    }
#endif
    if (!cbm_mcp_auto_index_within_file_limit(srv->session_root, file_limit, &file_count)) {
        char files[32];
        (void)snprintf(files, sizeof(files), "%d", file_count);
        cbm_log_warn("autoindex.skip", "reason",
                     file_count >= 0 ? "too_many_files" : "unsafe_or_unavailable_path", "files",
                     files, "limit", CBM_CONFIG_AUTO_INDEX_LIMIT);
        return;
    }

    /* Launch auto-index in background */
    if (cbm_thread_create(&srv->autoindex_tid, 0, autoindex_thread, srv) == 0) {
        srv->autoindex_active = true;
    }
}

/* ── Server request handler ───────────────────────────────────── */

bool cbm_mcp_jsonrpc_response_prepend_notice(char **response_io, const char *notice) {
    if (!response_io || !*response_io || !notice || !notice[0]) {
        return false;
    }
    yyjson_doc *document = yyjson_read(*response_io, strlen(*response_io), 0);
    if (!document) {
        return false;
    }
    yyjson_mut_doc *mutable_document = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root =
        mutable_document ? yyjson_val_mut_copy(mutable_document, yyjson_doc_get_root(document))
                         : NULL;
    yyjson_doc_free(document);
    if (!mutable_document || !root) {
        yyjson_mut_doc_free(mutable_document);
        return false;
    }
    yyjson_mut_doc_set_root(mutable_document, root);
    yyjson_mut_val *result = yyjson_mut_is_obj(root) ? yyjson_mut_obj_get(root, "result") : NULL;
    yyjson_mut_val *content =
        result && yyjson_mut_is_obj(result) ? yyjson_mut_obj_get(result, "content") : NULL;
    yyjson_mut_val *item =
        content && yyjson_mut_is_arr(content) ? yyjson_mut_obj(mutable_document) : NULL;
    bool added = item && yyjson_mut_obj_add_str(mutable_document, item, "type", "text") &&
                 yyjson_mut_obj_add_str(mutable_document, item, "text", notice) &&
                 yyjson_mut_arr_prepend(content, item);
    if (!added) {
        yyjson_mut_doc_free(mutable_document);
        return false;
    }
    char *replacement =
        yyjson_mut_write(mutable_document, YYJSON_WRITE_ALLOW_INVALID_UNICODE, NULL);
    yyjson_mut_doc_free(mutable_document);
    if (!replacement) {
        return false;
    }
    free(*response_io);
    *response_io = replacement;
    return true;
}

char *cbm_mcp_server_handle(cbm_mcp_server_t *srv, const char *line) {
    cbm_jsonrpc_request_t req = {0};
    if (cbm_jsonrpc_parse(line, &req) < 0) {
        return cbm_jsonrpc_format_error(0, JSONRPC_PARSE_ERROR, "Parse error");
    }

    /* Notifications (no id) → handle cancellation, then no response */
    if (!req.has_id) {
        if (req.method && strcmp(req.method, "notifications/cancelled") == 0) {
            if (cbm_mcp_cancel_request_matches(req.params_raw, srv->active_request_id,
                                               srv->active_request_id_str) &&
                cbm_mcp_server_cancel_active(srv)) {
                cbm_log_info("mcp.cancelled", "match", "true");
            }
        }
        cbm_jsonrpc_request_free(&req);
        return NULL;
    }

    if (!cbm_mcp_server_request_scope_begin(srv)) {
        int64_t request_id = req.id;
        cbm_jsonrpc_request_free(&req);
        return cbm_jsonrpc_format_error(request_id, JSONRPC_INTERNAL_ERROR,
                                        "Request cancellation scope unavailable");
    }

    struct timespec req_t0;
    cbm_clock_gettime(CLOCK_MONOTONIC, &req_t0);
    char *result_json = NULL;
    char *request_error_json = NULL;
    bool request_logged = false;

    if (strcmp(req.method, "initialize") == 0) {
        result_json = cbm_mcp_initialize_response_for_profile(req.params_raw, srv->tool_profile);
        detect_session(srv);
        if (srv->background_tasks && srv->tool_profile == CBM_MCP_TOOL_PROFILE_ALL) {
            maybe_auto_index(srv);
        }
    } else if (strcmp(req.method, "ping") == 0) {
        result_json = heap_strdup("{}");
    } else if (strcmp(req.method, "resources/list") == 0) {
        /* This server exposes no resources, but clients probe these on
         * connect regardless of declared capabilities and surface -32601 as
         * a failed connection (#958). Empty lists are interoperable. */
        result_json = heap_strdup("{\"resources\":[]}");
    } else if (strcmp(req.method, "resources/templates/list") == 0) {
        result_json = heap_strdup("{\"resourceTemplates\":[]}");
    } else if (strcmp(req.method, "prompts/list") == 0) {
        result_json = cbm_mcp_prompts_list();
    } else if (strcmp(req.method, "prompts/get") == 0) {
        result_json = cbm_mcp_prompt_get(req.params_raw, &request_error_json);
    } else if (strcmp(req.method, "tools/list") == 0) {
        result_json = cbm_mcp_tools_list_page(srv->tool_profile, req.params_raw);
    } else if (strcmp(req.method, "tools/call") == 0) {
        char *tool_name = req.params_raw ? cbm_mcp_get_tool_name(req.params_raw) : NULL;
        char *tool_args =
            req.params_raw ? cbm_mcp_get_arguments(req.params_raw) : heap_strdup("{}");
        srv->active_request_id = req.id;
        free(srv->active_request_id_str);
        srv->active_request_id_str = req.id_str ? heap_strdup(req.id_str) : NULL;

        struct timespec t0;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t0);
        result_json = cbm_mcp_handle_tool(srv, tool_name, tool_args);
        srv->active_request_id = CBM_NOT_FOUND;
        free(srv->active_request_id_str);
        srv->active_request_id_str = NULL;
        struct timespec t1;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t1);
        long long dur_us = ((long long)(t1.tv_sec - t0.tv_sec) * MCP_S_TO_US) +
                           ((long long)(t1.tv_nsec - t0.tv_nsec) / MCP_MS_TO_US);
        bool is_err = (result_json != NULL) && (strstr(result_json, "\"isError\":true") != NULL);
        cbm_diag_record_query(dur_us, is_err);
        long long request_dur_us = ((long long)(t1.tv_sec - req_t0.tv_sec) * MCP_S_TO_US) +
                                   ((long long)(t1.tv_nsec - req_t0.tv_nsec) / MCP_MS_TO_US);
        cbm_log_mcp_request(req.method, tool_name, is_err, request_dur_us);
        request_logged = true;

        free(tool_name);
        free(tool_args);
    } else {
        /* Echo the original id (string or numeric, issue #253) on the error. */
        char err_obj[160];
        snprintf(err_obj, sizeof(err_obj), "{\"code\":%d,\"message\":\"Method not found\"}",
                 JSONRPC_METHOD_NOT_FOUND);
        cbm_jsonrpc_response_t err_resp = {
            .id = req.id,
            .id_str = req.id_str,
            .error_json = err_obj,
        };
        char *err = cbm_jsonrpc_format_response(&err_resp);
        struct timespec t1;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t1);
        long long dur_us = ((long long)(t1.tv_sec - req_t0.tv_sec) * MCP_S_TO_US) +
                           ((long long)(t1.tv_nsec - req_t0.tv_nsec) / MCP_MS_TO_US);
        cbm_log_mcp_request(req.method, NULL, true, dur_us);
        cbm_mcp_server_request_scope_end(srv);
        cbm_jsonrpc_request_free(&req);
        return err;
    }

    if (request_error_json) {
        cbm_jsonrpc_response_t err_resp = {
            .id = req.id,
            .id_str = req.id_str,
            .error_json = request_error_json,
        };
        char *err = cbm_jsonrpc_format_response(&err_resp);
        struct timespec t1;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t1);
        long long dur_us = ((long long)(t1.tv_sec - req_t0.tv_sec) * MCP_S_TO_US) +
                           ((long long)(t1.tv_nsec - req_t0.tv_nsec) / MCP_MS_TO_US);
        cbm_log_mcp_request(req.method, NULL, true, dur_us);
        free(request_error_json);
        cbm_mcp_server_request_scope_end(srv);
        cbm_jsonrpc_request_free(&req);
        return err;
    }

    if (!request_logged) {
        struct timespec t1;
        cbm_clock_gettime(CLOCK_MONOTONIC, &t1);
        long long dur_us = ((long long)(t1.tv_sec - req_t0.tv_sec) * MCP_S_TO_US) +
                           ((long long)(t1.tv_nsec - req_t0.tv_nsec) / MCP_MS_TO_US);
        cbm_log_mcp_request(req.method, NULL, false, dur_us);
    }

    cbm_jsonrpc_response_t resp = {
        .id = req.id,
        .id_str = req.id_str,
        .result_json = result_json,
    };
    char *out = cbm_jsonrpc_format_response(&resp);
    free(result_json);
    cbm_mcp_server_request_scope_end(srv);
    cbm_jsonrpc_request_free(&req);
    return out;
}

/* Read through one newline without ever growing the buffer beyond max_bytes.
 * Returns 1 for a line (including a final unterminated line), 0 for clean EOF,
 * and -1 for I/O, allocation, or size failure. */
static int read_bounded_line(FILE *in, char **line, size_t *cap, size_t max_bytes,
                             size_t *out_len) {
    if (!in || !line || !cap || !out_len || max_bytes == 0) {
        return CBM_NOT_FOUND;
    }

    size_t len = 0;
    for (;;) {
        int ch = fgetc(in);
        if (ch == EOF) {
            if (ferror(in)) {
                return CBM_NOT_FOUND;
            }
            if (len == 0) {
                return 0;
            }
            break;
        }
        if (len >= max_bytes) {
            return CBM_NOT_FOUND;
        }

        size_t needed = len + 2; /* byte plus trailing NUL */
        if (*cap < needed) {
            size_t limit_cap = max_bytes + 1;
            size_t new_cap = *cap ? *cap : (limit_cap < 256 ? limit_cap : 256);
            while (new_cap < needed && new_cap < max_bytes + 1) {
                size_t doubled = new_cap * 2;
                new_cap = doubled > limit_cap ? limit_cap : doubled;
            }
            if (new_cap < needed) {
                return CBM_NOT_FOUND;
            }
            char *grown = realloc(*line, new_cap);
            if (!grown) {
                return CBM_NOT_FOUND;
            }
            /* Zero the tail: every byte of the buffer is then defined in any
             * caller's model (parse_content_length reads up to one byte past
             * the matched prefix), and a future over-read degrades to reading
             * NULs instead of undefined memory. One memset per growth step. */
            memset(grown + len, 0, new_cap - len);
            *line = grown;
            *cap = new_cap;
        }

        (*line)[len++] = (char)ch;
        if (ch == '\n') {
            break;
        }
    }

    (*line)[len] = '\0';
    *out_len = len;
    return SKIP_ONE;
}

static bool parse_content_length(const char *line, size_t *out) {
    if (!line || !out || strncmp(line, "Content-Length:", SLEN("Content-Length:")) != 0) {
        return false;
    }

    const char *cursor = line + MCP_CONTENT_PREFIX;
    while (*cursor == ' ' || *cursor == '\t') {
        cursor++;
    }
    if (!isdigit((unsigned char)*cursor)) {
        return false;
    }

    errno = 0;
    char *end = NULL;
    unsigned long long parsed = strtoull(cursor, &end, CBM_DECIMAL_BASE);
    if (errno == ERANGE || end == cursor) {
        return false;
    }
    while (*end == ' ' || *end == '\t') {
        end++;
    }
    if (*end != '\0' || parsed == 0 || parsed > MCP_MAX_MESSAGE_SIZE) {
        return false;
    }
    *out = (size_t)parsed;
    return true;
}

int cbm_mcp_read_message(FILE *in, char **message, bool *content_length_framed) {
    if (!in || !message || !content_length_framed) {
        return CBM_NOT_FOUND;
    }
    *message = NULL;
    *content_length_framed = false;

    char *line = NULL;
    size_t cap = 0;
    for (;;) {
        size_t line_read = 0;
        int line_status = read_bounded_line(in, &line, &cap, MCP_MAX_MESSAGE_SIZE, &line_read);
        if (line_status <= 0) {
            free(line);
            return line_status;
        }
        if (memchr(line, '\0', line_read) != NULL) {
            free(line);
            return CBM_NOT_FOUND;
        }

        size_t len = line_read;
        while (len > 0 && (line[len - SKIP_ONE] == '\n' || line[len - SKIP_ONE] == '\r')) {
            line[--len] = '\0';
        }
        if (len == 0) {
            continue;
        }

        if (strncmp(line, "Content-Length:", SLEN("Content-Length:")) != 0) {
            *message = line;
            return SKIP_ONE;
        }

        size_t content_len = 0;
        if (line_read > MCP_MAX_HEADER_SIZE || !parse_content_length(line, &content_len)) {
            free(line);
            return CBM_NOT_FOUND;
        }

        bool found_separator = false;
        size_t header_bytes = line_read;
        for (;;) {
            if (header_bytes >= MCP_MAX_HEADER_SIZE) {
                free(line);
                return CBM_NOT_FOUND;
            }
            size_t header_read = 0;
            int header_status = read_bounded_line(in, &line, &cap,
                                                  MCP_MAX_HEADER_SIZE - header_bytes, &header_read);
            if (header_status <= 0) {
                break;
            }
            if (memchr(line, '\0', header_read) != NULL) {
                free(line);
                return CBM_NOT_FOUND;
            }
            header_bytes += header_read;
            size_t header_len = header_read;
            while (header_len > 0 &&
                   (line[header_len - SKIP_ONE] == '\n' || line[header_len - SKIP_ONE] == '\r')) {
                line[--header_len] = '\0';
            }
            if (header_len == 0) {
                found_separator = true;
                break;
            }
        }
        if (!found_separator) {
            free(line);
            return CBM_NOT_FOUND;
        }
        free(line);

        char *body = malloc(content_len + SKIP_ONE);
        if (!body) {
            return CBM_NOT_FOUND;
        }
        size_t total = 0;
        while (total < content_len) {
            size_t nread = fread(body + total, SKIP_ONE, content_len - total, in);
            if (nread == 0) {
                free(body);
                return CBM_NOT_FOUND;
            }
            total += nread;
        }
        if (memchr(body, '\0', content_len) != NULL) {
            free(body);
            return CBM_NOT_FOUND;
        }
        body[content_len] = '\0';
        *message = body;
        *content_length_framed = true;
        return SKIP_ONE;
    }
}

#ifndef _WIN32
/* Unix 3-phase poll: non-blocking fd check, FILE* buffer peek, blocking poll.
 * Returns: 1 = data ready, 0 = timeout (evicted idle stores), -1 = error/EOF. */
static int poll_for_input_unix(cbm_mcp_server_t *srv, int fd, FILE *in) {
    struct pollfd pfd = {.fd = fd, .events = POLLIN};
    int pr = poll(&pfd, SKIP_ONE, 0); /* Phase 1: non-blocking */

    if (pr < 0) {
        return CBM_NOT_FOUND;
    }
    if (pr > 0) {
        return SKIP_ONE;
    }

    /* Phase 2: peek FILE* buffer */
    int saved_flags = fcntl(fd, F_GETFL);
    if (saved_flags < 0) {
        /* fcntl failed — fall through to a short blocking poll (see the Phase-3
         * note below on why the interval is bounded, not the full idle timeout) */
        pr = poll(&pfd, SKIP_ONE, MCP_TIMEOUT_MS);
        if (pr < 0) {
            return CBM_NOT_FOUND;
        }
        if (pr == 0) {
            cbm_mcp_server_evict_idle(srv, STORE_IDLE_TIMEOUT_S);
            return 0;
        }
        return SKIP_ONE;
    }

    (void)fcntl(fd, F_SETFL, saved_flags | O_NONBLOCK);
    int c = fgetc(in);
    (void)fcntl(fd, F_SETFL, saved_flags);

    if (c == EOF) {
        if (feof(in)) {
            return CBM_NOT_FOUND; /* true EOF */
        }
        clearerr(in);
        /* Phase 3: blocking poll, bounded to a SHORT interval (not the full idle
         * timeout). macOS poll()/select() do NOT report POLLIN/POLLHUP when a
         * FIFO's last writer closes — only read() returns 0 there (verified). A
         * 60s poll would therefore leave the server blocked up to a full idle
         * timeout after stdin EOF (a client that closes the pipe would appear to
         * hang). Waking every MCP_TIMEOUT_MS lets the Phase-2 read() above detect
         * the EOF within ~1s. Idle-store eviction (threshold STORE_IDLE_TIMEOUT_S)
         * is idempotent, so checking it on each short tick is harmless. */
        pr = poll(&pfd, SKIP_ONE, MCP_TIMEOUT_MS);
        if (pr < 0) {
            return CBM_NOT_FOUND;
        }
        if (pr == 0) {
            cbm_mcp_server_evict_idle(srv, STORE_IDLE_TIMEOUT_S);
            return 0;
        }
        return SKIP_ONE;
    }

    (void)ungetc(c, in);
    return SKIP_ONE;
}
#endif

/* ── Event loop ───────────────────────────────────────────────── */

int cbm_mcp_server_run(cbm_mcp_server_t *srv, FILE *in, FILE *out) {
    int fd = cbm_fileno(in);

#ifdef _WIN32
    /* Ensure stdio is in binary mode to prevent CRLF translation from corrupting
     * Content-Length byte counts and causing fread() to hang. */
    _setmode(cbm_fileno(in), _O_BINARY);
    _setmode(cbm_fileno(out), _O_BINARY);
#endif

    for (;;) {
        /* Poll with idle timeout so we can evict unused stores between requests.
         *
         * IMPORTANT: poll() operates on the raw fd, but getline() reads from a
         * buffered FILE*. When a client sends multiple messages in rapid
         * succession, the first getline() call may drain ALL kernel data into
         * libc's internal FILE* buffer. Subsequent poll() calls then see an
         * empty kernel fd and block for STORE_IDLE_TIMEOUT_S seconds even
         * though the next messages are already in the FILE* buffer.
         *
         * Fix (Unix): use a three-phase approach —
         *   Phase 1: non-blocking poll (timeout=0) to check the kernel fd.
         *   Phase 2: if Phase 1 returns 0, peek the FILE* buffer via fgetc/
         *            ungetc to detect data buffered by a prior getline() call.
         *            The fd is temporarily set O_NONBLOCK so fgetc() returns
         *            immediately (EAGAIN → EOF + ferror) instead of blocking
         *            when the FILE* buffer is empty, which would otherwise
         *            bypass the Phase 3 idle eviction timeout.
         *   Phase 3: only if both phases confirm no data, do blocking poll. */
#ifdef _WIN32
        /* Windows: WaitForSingleObject on stdin handle */
        HANDLE hStdin = (HANDLE)_get_osfhandle(fd);
        DWORD wr = WaitForSingleObject(hStdin, STORE_IDLE_TIMEOUT_S * MCP_TIMEOUT_MS);
        if (wr == WAIT_FAILED) {
            break;
        }
        if (wr == WAIT_TIMEOUT) {
            cbm_mcp_server_evict_idle(srv, STORE_IDLE_TIMEOUT_S);
            continue;
        }
#else
        int pr = poll_for_input_unix(srv, fd, in);
        if (pr < 0) {
            break;
        }
        if (pr == 0) {
            continue; /* timeout — idle stores evicted */
        }
#endif

        char *message = NULL;
        bool content_length_framed = false;
        if (cbm_mcp_read_message(in, &message, &content_length_framed) <= 0) {
            break;
        }

        char *resp = cbm_mcp_server_handle(srv, message);
        free(message);
        if (resp) {
            if (content_length_framed) {
                size_t response_len = strlen(resp);
                (void)fprintf(out, "Content-Length: %zu\r\n\r\n%s", response_len, resp);
            } else {
                (void)fprintf(out, "%s\n", resp);
            }
            (void)fflush(out);
            free(resp);
        }
    }

    return 0;
}

/* ── cbm_parse_file_uri ──────────────────────────────────────── */

bool cbm_parse_file_uri(const char *uri, char *out_path, int out_size) {
    if (!uri || !out_path || out_size <= 0) {
        if (out_path && out_size > 0) {
            out_path[0] = '\0';
        }
        return false;
    }

    /* Must start with file:// */
    if (strncmp(uri, "file://", SLEN("file://")) != 0) {
        out_path[0] = '\0';
        return false;
    }

    const char *path = uri + MCP_URI_PREFIX;

    /* On Windows, file:///C:/path → /C:/path. Strip leading / before drive letter. */
    if (path[0] == '/' && path[SKIP_ONE] &&
        ((path[SKIP_ONE] >= 'A' && path[SKIP_ONE] <= 'Z') ||
         (path[SKIP_ONE] >= 'a' && path[SKIP_ONE] <= 'z')) &&
        path[PAIR_LEN] == ':') {
        path++; /* skip the leading / */
    }

    snprintf(out_path, out_size, "%s", path);
    return true;
}
