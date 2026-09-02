#include "operations/architecture.h"

#include "foundation/constants.h"
#include "operations/compact_out.h"
#include "store/store.h"
#include "yyjson/yyjson.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static char *architecture_copy_string(const char *text) {
    if (!text) return NULL;
    size_t len = strlen(text);
    char *copy = malloc(len + 1U);
    if (copy) memcpy(copy, text, len + 1U);
    return copy;
}

static char *architecture_string_arg(const char *args_json, const char *name) {
    const char *json = args_json ? args_json : "{}";
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    char *result = value && yyjson_is_str(value) ? architecture_copy_string(yyjson_get_str(value)) : NULL;
    if (doc) yyjson_doc_free(doc);
    return result;
}

static char *architecture_project_arg(const char *args_json) {
    static const char *const names[] = {"project", "project_name", "project_id", "projectName"};
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); ++i) {
        char *value = architecture_string_arg(args_json, names[i]);
        if (value) return value;
    }
    return NULL;
}

static cbm_operation_result_t architecture_error(const char *message) {
    return cbm_operation_result_copy(message ? message : "architecture operation failed", true);
}

/* Canonical list of valid aspect tokens for get_architecture. Single source
 * of truth for the server-side validation (authoritative); the JSON-Schema
 * enum in the TOOLS entry above is the advisory client-side mirror — update
 * both together when the aspect set changes. */
static const char *VALID_ASPECTS[] = {"all",      "overview",   "structure", "dependencies",
                                      "routes",   "languages",  "packages",  "entry_points",
                                      "hotspots", "boundaries", "layers",    "file_tree",
                                      "clusters", "cycles",     NULL};

/* ── SCC / cycle condensation (get_architecture "cycles") ─────────
 * Iterative Tarjan over the CALLS call graph. Recursion would overflow on a
 * large graph, so the DFS state lives on explicit heap stacks. Reports the
 * strongly-connected components of size > 1 — the circular call dependencies,
 * the non-trivial content of the condensation quotient. */
typedef struct {
    int64_t *ids;  /* sorted unique node ids; index = position */
    int nverts;    /* |V| */
    int *adj_head; /* CSR row starts, length nverts+1 */
    int *adj;      /* CSR column (target vertex indices), length nedges */
    int nedges;    /* |E| within the vertex set */
} scc_graph_t;

static int scc_id_index(const int64_t *ids, int n, int64_t id) {
    int lo = 0;
    int hi = n - 1;
    while (lo <= hi) {
        int mid = (lo + hi) / 2;
        if (ids[mid] < id) {
            lo = mid + 1;
        } else if (ids[mid] > id) {
            hi = mid - 1;
        } else {
            return mid;
        }
    }
    return -1;
}

static int cmp_int64(const void *a, const void *b) {
    int64_t x = *(const int64_t *)a;
    int64_t y = *(const int64_t *)b;
    return (x > y) - (x < y);
}

/* Build the CSR call graph from parallel (src,tgt) edge arrays. Returns false
 * on OOM/empty. */
static bool scc_build(const int64_t *src, const int64_t *tgt, int ecount, scc_graph_t *g) {
    memset(g, 0, sizeof(*g));
    if (ecount <= 0) {
        return false;
    }
    int64_t *all = malloc((size_t)ecount * 2 * sizeof(int64_t));
    if (!all) {
        return false;
    }
    for (int i = 0; i < ecount; i++) {
        all[2 * i] = src[i];
        all[2 * i + 1] = tgt[i];
    }
    qsort(all, (size_t)ecount * 2, sizeof(int64_t), cmp_int64);
    int nv = 0;
    for (int i = 0; i < ecount * 2; i++) {
        if (i == 0 || all[i] != all[i - 1]) {
            all[nv++] = all[i];
        }
    }
    g->ids = all;
    g->nverts = nv;
    g->adj_head = calloc((size_t)nv + 1, sizeof(int));
    g->adj = malloc((size_t)ecount * sizeof(int));
    if (!g->adj_head || !g->adj) {
        free(g->ids);
        free(g->adj_head);
        free(g->adj);
        memset(g, 0, sizeof(*g));
        return false;
    }
    /* two-pass CSR fill. Every endpoint is in ids[] by construction (ids is
     * built from these same edge arrays), so a negative lookup is unreachable
     * -- but that invariant is invisible to path-sensitive analysis, and a
     * guard keeps a future collection bug from becoming an OOB write. Both
     * passes must skip identically or the cursors desync. */
    for (int i = 0; i < ecount; i++) {
        int u = scc_id_index(g->ids, nv, src[i]);
        int v = scc_id_index(g->ids, nv, tgt[i]);
        if (u < 0 || v < 0) {
            continue;
        }
        g->adj_head[u + 1]++;
    }
    for (int i = 0; i < nv; i++) {
        g->adj_head[i + 1] += g->adj_head[i];
    }
    int *cursor = malloc((size_t)nv * sizeof(int));
    if (!cursor) {
        free(g->ids);
        free(g->adj_head);
        free(g->adj);
        memset(g, 0, sizeof(*g));
        return false;
    }
    for (int i = 0; i < nv; i++) {
        cursor[i] = g->adj_head[i];
    }
    int filled = 0;
    for (int i = 0; i < ecount; i++) {
        int u = scc_id_index(g->ids, nv, src[i]);
        int v = scc_id_index(g->ids, nv, tgt[i]);
        if (u < 0 || v < 0) {
            continue;
        }
        g->adj[cursor[u]++] = v;
        filled++;
    }
    free(cursor);
    g->nedges = filled;
    return true;
}

static void scc_free(scc_graph_t *g) {
    free(g->ids);
    free(g->adj_head);
    free(g->adj);
    memset(g, 0, sizeof(*g));
}

/* Iterative Tarjan. Fills comp[v] with a component id; components are numbered
 * in discovery order. Returns the component count, or -1 on OOM. */
static int scc_tarjan(const scc_graph_t *g, int *comp) {
    enum { SCC_UNVISITED = -1 };
    int nv = g->nverts;
    int *index = malloc((size_t)nv * sizeof(int));
    int *low = malloc((size_t)nv * sizeof(int));
    bool *on_stack = calloc((size_t)nv, sizeof(bool));
    int *tstack = malloc((size_t)nv * sizeof(int)); /* Tarjan's node stack */
    int *dfs_v = malloc((size_t)nv * sizeof(int));  /* explicit DFS: vertex */
    int *dfs_i = malloc((size_t)nv * sizeof(int));  /* explicit DFS: adj cursor */
    if (!index || !low || !on_stack || !tstack || !dfs_v || !dfs_i) {
        free(index);
        free(low);
        free(on_stack);
        free(tstack);
        free(dfs_v);
        free(dfs_i);
        return -1;
    }
    for (int i = 0; i < nv; i++) {
        index[i] = SCC_UNVISITED;
        comp[i] = SCC_UNVISITED;
    }
    int counter = 0;
    int tsp = 0;   /* Tarjan stack pointer */
    int ncomp = 0; /* component id allocator */
    for (int s = 0; s < nv; s++) {
        if (index[s] != SCC_UNVISITED) {
            continue;
        }
        int dsp = 0; /* DFS stack pointer */
        dfs_v[dsp] = s;
        dfs_i[dsp] = g->adj_head[s];
        index[s] = low[s] = counter++;
        tstack[tsp++] = s;
        on_stack[s] = true;
        while (dsp >= 0) {
            int v = dfs_v[dsp];
            if (dfs_i[dsp] < g->adj_head[v + 1]) {
                int w = g->adj[dfs_i[dsp]++];
                if (index[w] == SCC_UNVISITED) {
                    index[w] = low[w] = counter++;
                    tstack[tsp++] = w;
                    on_stack[w] = true;
                    dsp++;
                    dfs_v[dsp] = w;
                    dfs_i[dsp] = g->adj_head[w];
                } else if (on_stack[w] && index[w] < low[v]) {
                    low[v] = index[w];
                }
            } else {
                /* v fully explored: it is a root iff low==index -> pop an SCC */
                if (low[v] == index[v]) {
                    int w;
                    do {
                        w = tstack[--tsp];
                        on_stack[w] = false;
                        comp[w] = ncomp;
                    } while (w != v);
                    ncomp++;
                }
                dsp--;
                if (dsp >= 0 && low[v] < low[dfs_v[dsp]]) {
                    low[dfs_v[dsp]] = low[v];
                }
            }
        }
    }
    free(index);
    free(low);
    free(on_stack);
    free(tstack);
    free(dfs_v);
    free(dfs_i);
    return ncomp;
}

static bool aspect_is_valid(const char *name) {
    if (!name) {
        return false;
    }
    for (int i = 0; VALID_ASPECTS[i]; i++) {
        if (strcmp(name, VALID_ASPECTS[i]) == 0) {
            return true;
        }
    }
    return false;
}

/* Check if an aspect is requested. NULL aspects = all. The array can contain
 * "all" (everything), "overview" (everything except file_tree — see
 * cbm_store_arch_aspect_in_overview in store.c), or the aspect name itself. */
/* True ONLY when `name` is explicitly present in the aspects array — never via
 * the no-filter default, "all", or "overview". For expensive opt-in aspects
 * (cycles scans the whole call graph) that must not run on a bare call. */
static bool aspect_explicitly_named(yyjson_val *aspects_arr, const char *name) {
    if (!aspects_arr) {
        return false;
    }
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(aspects_arr, &iter);
    yyjson_val *val;
    while ((val = yyjson_arr_iter_next(&iter)) != NULL) {
        const char *s = yyjson_get_str(val);
        if (s && strcmp(s, name) == 0) {
            return true;
        }
    }
    return false;
}

static bool aspect_wanted(yyjson_doc *aspects_doc, yyjson_val *aspects_arr, const char *name) {
    if (!aspects_arr) {
        return true; /* no filter = all */
    }
    yyjson_arr_iter iter;
    yyjson_arr_iter_init(aspects_arr, &iter);
    yyjson_val *val;
    while ((val = yyjson_arr_iter_next(&iter)) != NULL) {
        const char *s = yyjson_get_str(val);
        if (!s) {
            continue;
        }
        if (strcmp(s, "all") == 0) {
            return true;
        }
        if (strcmp(s, "overview") == 0 && cbm_store_arch_aspect_in_overview(name)) {
            return true;
        }
        if (strcmp(s, name) == 0) {
            return true;
        }
    }
    (void)aspects_doc;
    return false;
}

/* Append cross_repo_links summary to architecture JSON if CROSS_* edges exist. */
static void append_cross_repo_summary(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                      const cbm_schema_info_t *schema) {
    /* Scan edge types for any CROSS_* edges and sum them */
    int cross_total = 0;
    yyjson_mut_val *cr = yyjson_mut_obj(doc);
    static const char *cross_types[] = {"CROSS_HTTP_CALLS",    "CROSS_ASYNC_CALLS",
                                        "CROSS_CHANNEL",       "CROSS_GRPC_CALLS",
                                        "CROSS_GRAPHQL_CALLS", "CROSS_TRPC_CALLS"};
    for (int t = 0; t < (int)(sizeof(cross_types) / sizeof(cross_types[0])); t++) {
        for (int i = 0; i < schema->edge_type_count; i++) {
            if (strcmp(schema->edge_types[i].type, cross_types[t]) == 0) {
                yyjson_mut_obj_add_int(doc, cr, cross_types[t], schema->edge_types[i].count);
                cross_total += schema->edge_types[i].count;
                break;
            }
        }
    }
    if (cross_total > 0) {
        yyjson_mut_obj_add_int(doc, cr, "total", cross_total);
        yyjson_mut_obj_add_val(doc, root, "cross_repo_links", cr);
    }
}

/* Join a string list into buf with ';' separators (keeps TOON cells
 * comma-free so they need no quoting). Truncates silently at sz. */
static void arch_join_list(char *buf, size_t sz, const char **items, int n) {
    size_t pos = 0;
    buf[0] = '\0';
    for (int i = 0; i < n; i++) {
        const char *s = items[i] ? items[i] : "";
        int w = snprintf(buf + pos, sz - pos, "%s%s", i > 0 ? ";" : "", s);
        if (w < 0 || (size_t)w >= sz - pos) {
            break;
        }
        pos += (size_t)w;
    }
}

/* Compute the circular-dependency SCCs (size > 1) of the CALLS graph. Returns
 * a malloc'd array of components, each a malloc'd int64 array of member node
 * ids, with sizes in *out_sizes and count in *out_ncycles; sets *scanned_edges
 * and *edges_truncated. Caller frees each component + the arrays. Returns
 * CBM_STORE_OK, or CBM_STORE_ERR on failure (all outs zeroed). */
enum { ARCH_SCC_MAX_EDGES = 400000, ARCH_SCC_MAX_CYCLES = 100, ARCH_SCC_MEMBERS_SHOWN = 20 };

static int arch_compute_cycles(cbm_store_t *store, const char *project, int64_t ***out_members,
                               int **out_sizes, int *out_ncycles, int *out_total_cycles,
                               int *scanned_edges, bool *edges_truncated) {
    *out_members = NULL;
    *out_sizes = NULL;
    *out_ncycles = 0;
    *out_total_cycles = 0;
    *scanned_edges = 0;
    *edges_truncated = false;

    int64_t *src = NULL;
    int64_t *tgt = NULL;
    int ecount = 0;
    if (cbm_store_fetch_call_edges(store, project, ARCH_SCC_MAX_EDGES, &src, &tgt, &ecount,
                                   edges_truncated) != CBM_STORE_OK) {
        return CBM_STORE_ERR;
    }
    *scanned_edges = ecount;
    scc_graph_t g;
    if (!scc_build(src, tgt, ecount, &g)) {
        free(src);
        free(tgt);
        return CBM_STORE_OK; /* no edges = no cycles, not an error */
    }
    free(src);
    free(tgt);

    int *comp = malloc((size_t)g.nverts * sizeof(int));
    if (!comp) {
        scc_free(&g);
        return CBM_STORE_ERR;
    }
    int ncomp = scc_tarjan(&g, comp);
    if (ncomp < 0) {
        free(comp);
        scc_free(&g);
        return CBM_STORE_ERR;
    }
    /* Tarjan assigns every vertex a component, so ncomp >= 1 whenever
     * nverts >= 1 -- state it, so the zero-size-allocation path below is
     * provably confined to the empty graph. */
    if (g.nverts > 0 && ncomp <= 0) {
        free(comp);
        scc_free(&g);
        return CBM_STORE_ERR;
    }
    /* size per component */
    int *csize = calloc((size_t)ncomp, sizeof(int));
    if (!csize) {
        free(comp);
        scc_free(&g);
        return CBM_STORE_ERR;
    }
    for (int v = 0; v < g.nverts; v++) {
        csize[comp[v]]++;
    }
    /* collect components with size > 1 (the cycles) */
    int ncyc = 0;
    for (int c = 0; c < ncomp; c++) {
        if (csize[c] > 1) {
            ncyc++;
        }
    }
    *out_total_cycles = ncyc; /* the TRUE count, before the display clamp */
    if (ncyc > ARCH_SCC_MAX_CYCLES) {
        ncyc = ARCH_SCC_MAX_CYCLES;
    }
    int64_t **members = ncyc > 0 ? calloc((size_t)ncyc, sizeof(int64_t *)) : NULL;
    int *sizes = ncyc > 0 ? calloc((size_t)ncyc, sizeof(int)) : NULL;
    /* map component id -> output slot (only for the first ARCH_SCC_MAX_CYCLES
     * size>1 comps, in component-id order) */
    int *slot = malloc((size_t)ncomp * sizeof(int));
    if ((ncyc > 0 && (!members || !sizes)) || !slot) {
        free(members);
        free(sizes);
        free(slot);
        free(csize);
        free(comp);
        scc_free(&g);
        return CBM_STORE_ERR;
    }
    int next_slot = 0;
    for (int c = 0; c < ncomp; c++) {
        if (csize[c] > 1 && next_slot < ncyc) {
            slot[c] = next_slot;
            sizes[next_slot] = csize[c];
            members[next_slot] = malloc((size_t)csize[c] * sizeof(int64_t));
            next_slot++;
        } else {
            slot[c] = -1;
        }
    }
    int *fill = calloc((size_t)ncyc, sizeof(int));
    if (ncyc > 0 && !fill) {
        for (int i = 0; i < ncyc; i++) {
            free(members[i]);
        }
        free(members);
        free(sizes);
        free(slot);
        free(csize);
        free(comp);
        scc_free(&g);
        return CBM_STORE_ERR;
    }
    /* With ncyc == 0 no slot is ever >= 0 (the slot loop can't assign one),
     * so members -- NULL in that case -- is never indexed; make that
     * invariant local instead of cross-loop so it is checkable. */
    if (ncyc > 0) {
        for (int v = 0; v < g.nverts; v++) {
            int sl = slot[comp[v]];
            if (sl >= 0) {
                members[sl][fill[sl]++] = g.ids[v];
            }
        }
    }
    free(fill);
    free(slot);
    free(csize);
    free(comp);
    scc_free(&g);
    *out_members = members;
    *out_sizes = sizes;
    *out_ncycles = ncyc;
    return CBM_STORE_OK;
}

/* Begin a json-tree {cols, rows} section under `key` on root; returns the
 * rows array for the caller to fill with column-ordered row arrays. */
static yyjson_mut_val *arch_json_section(yyjson_mut_doc *doc, yyjson_mut_val *root, const char *key,
                                         const char *const *cols, int ncols) {
    yyjson_mut_val *sec = yyjson_mut_obj(doc);
    yyjson_mut_val *c = yyjson_mut_arr(doc);
    for (int i = 0; i < ncols; i++) {
        yyjson_mut_arr_add_str(doc, c, cols[i]);
    }
    yyjson_mut_obj_add_val(doc, sec, "cols", c);
    yyjson_mut_val *rows = yyjson_mut_arr(doc);
    yyjson_mut_obj_add_val(doc, sec, "rows", rows);
    yyjson_mut_obj_add_val(doc, root, key, sec);
    return rows;
}

/* Fetch the qualified_name for a node id, or a "#<id>" fallback. */
static void arch_node_qn(cbm_store_t *store, int64_t id, char *out, size_t outsz) {
    cbm_node_t n = {0};
    if (cbm_store_find_node_by_id(store, id, &n) == CBM_STORE_OK && n.qualified_name) {
        snprintf(out, outsz, "%s", n.qualified_name);
    } else {
        snprintf(out, outsz, "#%lld", (long long)id);
    }
    cbm_node_free_fields(&n);
}

cbm_operation_result_t cbm_architecture_operation_execute(const char *args) {
    char *project = architecture_project_arg(args);
    if (!project || !project[0]) {
        free(project);
        return architecture_error("project is required");
    }
    cbm_store_t *store = cbm_store_open(project);
    if (!store) {
        free(project);
        return architecture_error("project not found or not indexed");
    }
    char *scope_path = architecture_string_arg(args, "path");

    if (cbm_store_count_nodes(store, project) <= 0) {
        cbm_store_close(store);
        free(project);
        free(scope_path);
        return architecture_error("project not indexed or index is empty");
    }

    /* Parse aspects array from args */
    yyjson_doc *aspects_doc = NULL;
    yyjson_val *aspects_arr = NULL;
    {
        yyjson_doc *args_doc = yyjson_read(args, strlen(args), 0);
        if (args_doc) {
            yyjson_val *aval = yyjson_obj_get(yyjson_doc_get_root(args_doc), "aspects");
            if (yyjson_is_arr(aval)) {
                aspects_doc = args_doc; /* keep alive */
                aspects_arr = aval;
            } else {
                yyjson_doc_free(args_doc);
            }
        }
    }

    /* Build a C string array from aspects for cbm_store_get_architecture.
     * Strings point into aspects_doc memory so aspects_doc must outlive this array. */
    const char *aspects_strs[16];
    int aspects_strs_count = 0;
    if (aspects_arr) {
        size_t aspect_idx;
        size_t aspect_max;
        yyjson_val *aspect_val;
        yyjson_arr_foreach(aspects_arr, aspect_idx, aspect_max, aspect_val) {
            const char *s = yyjson_get_str(aspect_val);
            if (s && aspects_strs_count < 16) {
                aspects_strs[aspects_strs_count++] = s;
            }
        }
    }

    /* Server-side validation: reject unknown aspect tokens with an isError
     * result listing the valid values. The JSON-Schema enum is advisory —
     * many MCP clients do not validate arguments against tool schemas — so
     * without this check a typo degraded to a silent near-empty payload. */
    for (int i = 0; i < aspects_strs_count; i++) {
        if (!aspect_is_valid(aspects_strs[i])) {
            char valid_list[CBM_SZ_256];
            size_t off = 0;
            for (int j = 0; VALID_ASPECTS[j] && off < sizeof(valid_list); j++) {
                int n = snprintf(valid_list + off, sizeof(valid_list) - off, "%s%s",
                                 j > 0 ? ", " : "", VALID_ASPECTS[j]);
                if (n < 0) {
                    break;
                }
                off += (size_t)n;
            }
            char msg[CBM_SZ_512];
            snprintf(msg, sizeof(msg), "Unknown aspect '%s'. Valid: %s.", aspects_strs[i],
                     valid_list);
            cbm_operation_result_t err = architecture_error(msg);
            free(project);
            free(scope_path);
            if (aspects_doc) {
                yyjson_doc_free(aspects_doc);
            }
            return err;
        }
    }

    /* Default (no aspects) = compact summary. The old default rendered ALL
     * aspects including the full file_tree — ~94KB (~23K tokens) on a
     * mid-size repo, a context bomb for the LLM consumers. Explicit
     * aspects (or ["all"]) keep full access to every section. */
    bool default_summary = false;
    if (aspects_strs_count == 0) {
        /* NOT "overview" — that means everything-except-file_tree. Totals and
         * node_labels/edge_types counts are always emitted alongside. */
        aspects_strs[aspects_strs_count++] = "languages";
        aspects_strs[aspects_strs_count++] = "packages";
        aspects_strs[aspects_strs_count++] = "entry_points";
        default_summary = true;
    }

    cbm_schema_info_t schema = {0};
    /* Counts-only: this handler renders label/type counts but never property
     * keys, and full key discovery json_each-scans every row (seconds-to-
     * minutes on multi-million-node graphs). */
    cbm_store_get_schema_counts_scoped(store, project, scope_path, &schema);

    cbm_architecture_info_t arch = {0};
    cbm_store_get_architecture(store, project, scope_path, aspects_strs, aspects_strs_count, &arch);

    int node_count = cbm_store_count_nodes_scoped(store, project, scope_path);
    int edge_count = cbm_store_count_edges_scoped(store, project, scope_path);
    char norm_path[CBM_SZ_512];
    bool path_scoped = cbm_store_normalize_arch_path(scope_path, norm_path, sizeof(norm_path));

    /* Response encoding: tree tables by default; format:"json" emits the
     * same model as structured JSON ({cols, rows} per section). */
    char *arch_format = architecture_string_arg(args, "format");
    bool arch_legacy_json = arch_format && strcmp(arch_format, "json") == 0;
    free(arch_format);

    if (!arch_legacy_json) {
        cbm_sb_t sb;
        cbm_sb_init(&sb);
        if (project) {
            cbm_tree_scalar_str(&sb, "project", project);
        }
        if (default_summary) {
            cbm_tree_scalar_str(&sb, "aspects_hint",
                                "Summary view (default). More on request via aspects:[...] — "
                                "structure, dependencies, routes, hotspots, boundaries, layers, "
                                "clusters, file_tree — or [\"all\"] for everything.");
        }
        if (path_scoped) {
            cbm_tree_scalar_str(&sb, "path", norm_path);
            cbm_tree_scalar_int(&sb, "root_total_nodes", cbm_store_count_nodes(store, project));
            cbm_tree_scalar_int(&sb, "root_total_edges", cbm_store_count_edges(store, project));
            cbm_tree_scalar_int(&sb, "scoped_total_nodes", node_count);
            cbm_tree_scalar_int(&sb, "scoped_total_edges", edge_count);
        }
        cbm_tree_scalar_int(&sb, "total_nodes", node_count);
        cbm_tree_scalar_int(&sb, "total_edges", edge_count);

        if (aspect_wanted(aspects_doc, aspects_arr, "structure") && schema.node_label_count > 0) {
            static const char *const lcols[] = {"label", "count"};
            cbm_tree_table_header(&sb, "node_labels", schema.node_label_count, lcols, 2);
            for (int i = 0; i < schema.node_label_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, schema.node_labels[i].label, true);
                cbm_tree_cell_int(&sb, schema.node_labels[i].count, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (aspect_wanted(aspects_doc, aspects_arr, "dependencies") && schema.edge_type_count > 0) {
            static const char *const tcols[] = {"type", "count"};
            cbm_tree_table_header(&sb, "edge_types", schema.edge_type_count, tcols, 2);
            for (int i = 0; i < schema.edge_type_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, schema.edge_types[i].type, true);
                cbm_tree_cell_int(&sb, schema.edge_types[i].count, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (aspect_wanted(aspects_doc, aspects_arr, "routes") && schema.rel_pattern_count > 0) {
            static const char *const pcols[] = {"pattern"};
            cbm_tree_table_header(&sb, "relationship_patterns", schema.rel_pattern_count, pcols, 1);
            for (int i = 0; i < schema.rel_pattern_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, schema.rel_patterns[i], true);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.language_count > 0) {
            static const char *const gcols[] = {"language", "files"};
            cbm_tree_table_header(&sb, "languages", arch.language_count, gcols, 2);
            for (int i = 0; i < arch.language_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, arch.languages[i].language, true);
                cbm_tree_cell_int(&sb, arch.languages[i].file_count, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.package_count > 0) {
            static const char *const kcols[] = {"name", "nodes", "fan_in", "fan_out"};
            cbm_tree_table_header(&sb, "packages", arch.package_count, kcols, 4);
            for (int i = 0; i < arch.package_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, arch.packages[i].name, true);
                cbm_tree_cell_int(&sb, arch.packages[i].node_count, false);
                cbm_tree_cell_int(&sb, arch.packages[i].fan_in, false);
                cbm_tree_cell_int(&sb, arch.packages[i].fan_out, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.entry_point_count > 0) {
            /* qn only — `name` is its last segment. */
            static const char *const ecols[] = {"qn", "file"};
            cbm_tree_table_header(&sb, "entry_points", arch.entry_point_count, ecols, 2);
            for (int i = 0; i < arch.entry_point_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, arch.entry_points[i].qualified_name, true);
                cbm_tree_cell_str(&sb, arch.entry_points[i].file, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.route_count > 0) {
            static const char *const rcols[] = {"method", "path", "handler"};
            cbm_tree_table_header(&sb, "routes", arch.route_count, rcols, 3);
            for (int i = 0; i < arch.route_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, arch.routes[i].method, true);
                cbm_tree_cell_str(&sb, arch.routes[i].path, false);
                cbm_tree_cell_str(&sb, arch.routes[i].handler, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.hotspot_count > 0) {
            static const char *const hcols[] = {"qn", "fan_in"};
            cbm_tree_table_header(&sb, "hotspots", arch.hotspot_count, hcols, 2);
            for (int i = 0; i < arch.hotspot_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, arch.hotspots[i].qualified_name, true);
                cbm_tree_cell_int(&sb, arch.hotspots[i].fan_in, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.boundary_count > 0) {
            static const char *const bcols[] = {"from", "to", "calls"};
            cbm_tree_table_header(&sb, "boundaries", arch.boundary_count, bcols, 3);
            for (int i = 0; i < arch.boundary_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, arch.boundaries[i].from, true);
                cbm_tree_cell_str(&sb, arch.boundaries[i].to, false);
                cbm_tree_cell_int(&sb, arch.boundaries[i].call_count, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.service_count > 0) {
            static const char *const scols[] = {"from", "to", "type", "count"};
            cbm_tree_table_header(&sb, "services", arch.service_count, scols, 4);
            for (int i = 0; i < arch.service_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, arch.services[i].from, true);
                cbm_tree_cell_str(&sb, arch.services[i].to, false);
                cbm_tree_cell_str(&sb, arch.services[i].type, false);
                cbm_tree_cell_int(&sb, arch.services[i].count, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.layer_count > 0) {
            static const char *const ycols[] = {"name", "layer", "reason"};
            cbm_tree_table_header(&sb, "layers", arch.layer_count, ycols, 3);
            for (int i = 0; i < arch.layer_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, arch.layers[i].name, true);
                cbm_tree_cell_str(&sb, arch.layers[i].layer, false);
                cbm_tree_cell_str(&sb, arch.layers[i].reason, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.cluster_count > 0) {
            /* Nested lists become ';'-joined cells. */
            static const char *const ccols[] = {"id",        "label",    "members",   "cohesion",
                                                "top_nodes", "packages", "edge_types"};
            cbm_tree_table_header(&sb, "clusters", arch.cluster_count, ccols, 7);
            for (int i = 0; i < arch.cluster_count; i++) {
                const cbm_cluster_info_t *c = &arch.clusters[i];
                char joined[CBM_SZ_1K];
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_int(&sb, c->id, true);
                cbm_tree_cell_str(&sb, c->label, false);
                cbm_tree_cell_int(&sb, c->members, false);
                cbm_tree_cell_real(&sb, c->cohesion, false);
                arch_join_list(joined, sizeof(joined), c->top_nodes, c->top_node_count);
                cbm_tree_cell_str(&sb, joined, false);
                arch_join_list(joined, sizeof(joined), c->packages, c->package_count);
                cbm_tree_cell_str(&sb, joined, false);
                arch_join_list(joined, sizeof(joined), c->edge_types, c->edge_type_count);
                cbm_tree_cell_str(&sb, joined, false);
                cbm_tree_row_end(&sb);
            }
        }
        if (arch.file_tree_count > 0) {
            static const char *const fcols[] = {"path", "type", "children"};
            cbm_tree_table_header(&sb, "file_tree", arch.file_tree_count, fcols, 3);
            for (int i = 0; i < arch.file_tree_count; i++) {
                cbm_tree_row_begin(&sb);
                cbm_tree_cell_str(&sb, arch.file_tree[i].path, true);
                cbm_tree_cell_str(&sb, arch.file_tree[i].type, false);
                cbm_tree_cell_int(&sb, arch.file_tree[i].children, false);
                cbm_tree_row_end(&sb);
            }
        }
        /* Cross-repo edge summary (mirrors append_cross_repo_summary). */
        {
            static const char *const cross_types[] = {"CROSS_HTTP_CALLS",    "CROSS_ASYNC_CALLS",
                                                      "CROSS_CHANNEL",       "CROSS_GRPC_CALLS",
                                                      "CROSS_GRAPHQL_CALLS", "CROSS_TRPC_CALLS"};
            int cross_total = 0;
            for (int t = 0; t < (int)(sizeof(cross_types) / sizeof(cross_types[0])); t++) {
                for (int i = 0; i < schema.edge_type_count; i++) {
                    if (strcmp(schema.edge_types[i].type, cross_types[t]) == 0) {
                        cross_total += schema.edge_types[i].count;
                        break;
                    }
                }
            }
            if (cross_total > 0) {
                cbm_tree_scalar_int(&sb, "cross_repo_links_total", cross_total);
            }
        }

        /* cycles: circular CALLS dependencies (SCCs of size > 1) — opt-in, it
         * scans the whole call graph. A quotient/condensation view. */
        if (aspect_explicitly_named(aspects_arr, "cycles")) {
            int64_t **members = NULL;
            int *sizes = NULL;
            int ncyc = 0;
            int total_cyc = 0;
            int scanned = 0;
            bool etrunc = false;
            if (arch_compute_cycles(store, project, &members, &sizes, &ncyc, &total_cyc, &scanned,
                                    &etrunc) == CBM_STORE_OK) {
                cbm_tree_scalar_int(&sb, "call_edges_scanned", scanned);
                cbm_tree_scalar_int(&sb, "cycles_total", total_cyc);
                if (etrunc) {
                    cbm_tree_scalar_bool(&sb, "cycles_partial", true);
                    cbm_tree_scalar_str(&sb, "cycles_hint",
                                        "call graph exceeded the scan budget; cycle list may be "
                                        "incomplete");
                }
                if (total_cyc > ncyc) {
                    char omit[CBM_SZ_128];
                    snprintf(omit, sizeof(omit), "cycles_omitted: %d  (showing the first %d)\n",
                             total_cyc - ncyc, ncyc);
                    cbm_sb_append(&sb, omit);
                }
                char hdr[CBM_SZ_128];
                snprintf(hdr, sizeof(hdr),
                         "cycles: %d  (rows: size members; circular CALLS dependencies)\n", ncyc);
                cbm_sb_append(&sb, hdr);
                for (int c = 0; c < ncyc; c++) {
                    char row[CBM_SZ_2K];
                    int off = snprintf(row, sizeof(row), "  %d ", sizes[c]);
                    bool clipped = sizes[c] > ARCH_SCC_MEMBERS_SHOWN;
                    int show = clipped ? ARCH_SCC_MEMBERS_SHOWN : sizes[c];
                    for (int m = 0; m < show && off < (int)sizeof(row) - 2; m++) {
                        char qn[CBM_SZ_512];
                        arch_node_qn(store, members[c][m], qn, sizeof(qn));
                        off += snprintf(row + off, sizeof(row) - off, "%s%s", m ? ";" : "", qn);
                    }
                    if (clipped && off < (int)sizeof(row) - 8) {
                        snprintf(row + off, sizeof(row) - off, ";+%d", sizes[c] - show);
                    }
                    cbm_sb_append(&sb, row);
                    cbm_sb_append(&sb, "\n");
                    free(members[c]);
                }
                free(members);
                free(sizes);
            }
        }

        cbm_store_architecture_free(&arch);
        cbm_store_schema_free(&schema);
        if (aspects_doc) {
            yyjson_doc_free(aspects_doc);
        }
        free(project);
        free(scope_path);
        char *text = cbm_sb_finish(&sb);
        if (!text) {
            cbm_store_close(store);
            return architecture_error("out of memory");
        }
        cbm_store_close(store);
        return cbm_operation_result_take(text, false);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = yyjson_mut_obj(doc);
    yyjson_mut_doc_set_root(doc, root);

    if (project) {
        yyjson_mut_obj_add_str(doc, root, "project", project);
    }
    if (default_summary) {
        yyjson_mut_obj_add_str(doc, root, "aspects_hint",
                               "Summary view (default). More on request via aspects:[...] — "
                               "structure, dependencies, routes, hotspots, boundaries, layers, "
                               "clusters, file_tree — or [\"all\"] for everything.");
    }
    if (path_scoped) {
        yyjson_mut_obj_add_str(doc, root, "path", norm_path);
        int root_nodes = cbm_store_count_nodes(store, project);
        int root_edges = cbm_store_count_edges(store, project);
        yyjson_mut_obj_add_int(doc, root, "root_total_nodes", root_nodes);
        yyjson_mut_obj_add_int(doc, root, "root_total_edges", root_edges);
        yyjson_mut_obj_add_int(doc, root, "scoped_total_nodes", node_count);
        yyjson_mut_obj_add_int(doc, root, "scoped_total_edges", edge_count);
    }
    yyjson_mut_obj_add_int(doc, root, "total_nodes", node_count);
    yyjson_mut_obj_add_int(doc, root, "total_edges", edge_count);

    /* Every section below is the json-tree model: {cols, rows[[...]]} —
     * mirrors the text tree tables one-for-one. */
    if (aspect_wanted(aspects_doc, aspects_arr, "structure")) {
        static const char *const lcols[] = {"label", "count"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "node_labels", lcols, 2);
        for (int i = 0; i < schema.node_label_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row, schema.node_labels[i].label);
            yyjson_mut_arr_add_int(doc, row, schema.node_labels[i].count);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (aspect_wanted(aspects_doc, aspects_arr, "dependencies")) {
        static const char *const tcols[] = {"type", "count"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "edge_types", tcols, 2);
        for (int i = 0; i < schema.edge_type_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row, schema.edge_types[i].type);
            yyjson_mut_arr_add_int(doc, row, schema.edge_types[i].count);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    /* Relationship patterns (a plain list — no columns to factor) */
    if (aspect_wanted(aspects_doc, aspects_arr, "routes") && schema.rel_pattern_count > 0) {
        yyjson_mut_val *pats = yyjson_mut_arr(doc);
        for (int i = 0; i < schema.rel_pattern_count; i++) {
            yyjson_mut_arr_add_str(doc, pats, schema.rel_patterns[i]);
        }
        yyjson_mut_obj_add_val(doc, root, "relationship_patterns", pats);
    }

    if (arch.language_count > 0) {
        static const char *const gcols[] = {"language", "files"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "languages", gcols, 2);
        for (int i = 0; i < arch.language_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row,
                                      arch.languages[i].language ? arch.languages[i].language : "");
            yyjson_mut_arr_add_int(doc, row, arch.languages[i].file_count);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.package_count > 0) {
        static const char *const kcols[] = {"name", "nodes", "fan_in", "fan_out"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "packages", kcols, 4);
        for (int i = 0; i < arch.package_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row, arch.packages[i].name ? arch.packages[i].name : "");
            yyjson_mut_arr_add_int(doc, row, arch.packages[i].node_count);
            yyjson_mut_arr_add_int(doc, row, arch.packages[i].fan_in);
            yyjson_mut_arr_add_int(doc, row, arch.packages[i].fan_out);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.entry_point_count > 0) {
        static const char *const ecols[] = {"qn", "file"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "entry_points", ecols, 2);
        for (int i = 0; i < arch.entry_point_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(
                doc, row,
                arch.entry_points[i].qualified_name ? arch.entry_points[i].qualified_name : "");
            yyjson_mut_arr_add_strcpy(doc, row,
                                      arch.entry_points[i].file ? arch.entry_points[i].file : "");
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.route_count > 0) {
        static const char *const rcols[] = {"method", "path", "handler"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "routes", rcols, 3);
        for (int i = 0; i < arch.route_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row, arch.routes[i].method ? arch.routes[i].method : "");
            yyjson_mut_arr_add_strcpy(doc, row, arch.routes[i].path ? arch.routes[i].path : "");
            yyjson_mut_arr_add_strcpy(doc, row,
                                      arch.routes[i].handler ? arch.routes[i].handler : "");
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.hotspot_count > 0) {
        static const char *const hcols[] = {"qn", "fan_in"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "hotspots", hcols, 2);
        for (int i = 0; i < arch.hotspot_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(
                doc, row, arch.hotspots[i].qualified_name ? arch.hotspots[i].qualified_name : "");
            yyjson_mut_arr_add_int(doc, row, arch.hotspots[i].fan_in);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.boundary_count > 0) {
        static const char *const bcols[] = {"from", "to", "calls"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "boundaries", bcols, 3);
        for (int i = 0; i < arch.boundary_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row,
                                      arch.boundaries[i].from ? arch.boundaries[i].from : "");
            yyjson_mut_arr_add_strcpy(doc, row, arch.boundaries[i].to ? arch.boundaries[i].to : "");
            yyjson_mut_arr_add_int(doc, row, arch.boundaries[i].call_count);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.service_count > 0) {
        static const char *const scols[] = {"from", "to", "type", "count"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "services", scols, 4);
        for (int i = 0; i < arch.service_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row, arch.services[i].from ? arch.services[i].from : "");
            yyjson_mut_arr_add_strcpy(doc, row, arch.services[i].to ? arch.services[i].to : "");
            yyjson_mut_arr_add_strcpy(doc, row, arch.services[i].type ? arch.services[i].type : "");
            yyjson_mut_arr_add_int(doc, row, arch.services[i].count);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.layer_count > 0) {
        static const char *const ycols[] = {"name", "layer", "reason"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "layers", ycols, 3);
        for (int i = 0; i < arch.layer_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row, arch.layers[i].name ? arch.layers[i].name : "");
            yyjson_mut_arr_add_strcpy(doc, row, arch.layers[i].layer ? arch.layers[i].layer : "");
            yyjson_mut_arr_add_strcpy(doc, row, arch.layers[i].reason ? arch.layers[i].reason : "");
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.cluster_count > 0) {
        /* Nested lists stay nested arrays within the row (tree-json). */
        static const char *const ccols[] = {"id",        "label",    "members",   "cohesion",
                                            "top_nodes", "packages", "edge_types"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "clusters", ccols, 7);
        for (int i = 0; i < arch.cluster_count; i++) {
            const cbm_cluster_info_t *c = &arch.clusters[i];
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_int(doc, row, c->id);
            yyjson_mut_arr_add_strcpy(doc, row, c->label ? c->label : "");
            yyjson_mut_arr_add_int(doc, row, c->members);
            yyjson_mut_arr_add_real(doc, row, c->cohesion);
            yyjson_mut_val *top = yyjson_mut_arr(doc);
            for (int j = 0; j < c->top_node_count; j++) {
                yyjson_mut_arr_add_str(doc, top, c->top_nodes[j] ? c->top_nodes[j] : "");
            }
            yyjson_mut_arr_add_val(row, top);
            yyjson_mut_val *pkgs = yyjson_mut_arr(doc);
            for (int j = 0; j < c->package_count; j++) {
                yyjson_mut_arr_add_str(doc, pkgs, c->packages[j] ? c->packages[j] : "");
            }
            yyjson_mut_arr_add_val(row, pkgs);
            yyjson_mut_val *etypes = yyjson_mut_arr(doc);
            for (int j = 0; j < c->edge_type_count; j++) {
                yyjson_mut_arr_add_str(doc, etypes, c->edge_types[j] ? c->edge_types[j] : "");
            }
            yyjson_mut_arr_add_val(row, etypes);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    if (arch.file_tree_count > 0) {
        static const char *const fcols[] = {"path", "type", "children"};
        yyjson_mut_val *rows = arch_json_section(doc, root, "file_tree", fcols, 3);
        for (int i = 0; i < arch.file_tree_count; i++) {
            yyjson_mut_val *row = yyjson_mut_arr(doc);
            yyjson_mut_arr_add_strcpy(doc, row,
                                      arch.file_tree[i].path ? arch.file_tree[i].path : "");
            yyjson_mut_arr_add_strcpy(doc, row,
                                      arch.file_tree[i].type ? arch.file_tree[i].type : "");
            yyjson_mut_arr_add_int(doc, row, arch.file_tree[i].children);
            yyjson_mut_arr_add_val(rows, row);
        }
    }

    append_cross_repo_summary(doc, root, &schema);

    /* cycles: SCCs of size > 1 in the CALLS graph (same model as tree). */
    if (aspect_explicitly_named(aspects_arr, "cycles")) {
        int64_t **members = NULL;
        int *sizes = NULL;
        int ncyc = 0;
        int total_cyc = 0;
        int scanned = 0;
        bool etrunc = false;
        if (arch_compute_cycles(store, project, &members, &sizes, &ncyc, &total_cyc, &scanned,
                                &etrunc) == CBM_STORE_OK) {
            yyjson_mut_obj_add_int(doc, root, "call_edges_scanned", scanned);
            yyjson_mut_obj_add_int(doc, root, "cycles_total", total_cyc);
            yyjson_mut_obj_add_bool(doc, root, "cycles_partial", etrunc);
            yyjson_mut_val *cyc = yyjson_mut_arr(doc);
            for (int c = 0; c < ncyc; c++) {
                yyjson_mut_val *o = yyjson_mut_obj(doc);
                yyjson_mut_obj_add_int(doc, o, "size", sizes[c]);
                yyjson_mut_val *mem = yyjson_mut_arr(doc);
                int show = sizes[c] < ARCH_SCC_MEMBERS_SHOWN ? sizes[c] : ARCH_SCC_MEMBERS_SHOWN;
                for (int m = 0; m < show; m++) {
                    char qn[CBM_SZ_512];
                    arch_node_qn(store, members[c][m], qn, sizeof(qn));
                    yyjson_mut_arr_add_strcpy(doc, mem, qn);
                }
                yyjson_mut_obj_add_val(doc, o, "members", mem);
                yyjson_mut_arr_add_val(cyc, o);
                free(members[c]);
            }
            yyjson_mut_obj_add_val(doc, root, "cycles", cyc);
            free(members);
            free(sizes);
        }
    }

    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    cbm_store_architecture_free(&arch);
    cbm_store_schema_free(&schema);
    if (aspects_doc) {
        yyjson_doc_free(aspects_doc);
    }
    free(project);
    free(scope_path);

    cbm_store_close(store);
    return json ? cbm_operation_result_take(json, false)
                : architecture_error("result encoding failed");
}

