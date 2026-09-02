#include "operations/trace.h"

#include "foundation/limits.h"
#include "store/store.h"
#include "yyjson/yyjson.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
    TRACE_DEFAULT_DEPTH = 3,
    TRACE_DEFAULT_LIMIT = 100,
    TRACE_MAX_LIMIT = 5000,
    TRACE_MAX_EDGE_TYPES = 16,
    TRACE_RES_CALLABLE = 2,
    TRACE_RES_OTHER = 1,
    TRACE_RES_MODULE = 0,
    TRACE_RES_WEIGHT = 1000000,
};

typedef struct trace_cursor {
    char leg;
    char generation[96];
    uint64_t qhash;
    int hop;
    int64_t node_id;
} trace_cursor_t;

static char *copy_text(const char *text) {
    if (!text) return NULL;
    size_t len = strlen(text);
    char *copy = malloc(len + 1U);
    if (copy) memcpy(copy, text, len + 1U);
    return copy;
}

static yyjson_doc *args_doc(const char *args) {
    return args ? yyjson_read(args, strlen(args), 0) : NULL;
}

static char *string_arg(const char *args, const char *name) {
    yyjson_doc *doc = args_doc(args);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    char *result = value && yyjson_is_str(value) ? copy_text(yyjson_get_str(value)) : NULL;
    if (doc) yyjson_doc_free(doc);
    return result;
}

static int int_arg(const char *args, const char *name, int fallback) {
    yyjson_doc *doc = args_doc(args);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, name) : NULL;
    int result = value && yyjson_is_int(value) ? (int)yyjson_get_sint(value) : fallback;
    if (doc) yyjson_doc_free(doc);
    return result;
}

static bool bool_arg(const char *args, const char *name) {
    yyjson_doc *doc = args_doc(args);
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
        return cbm_operation_result_copy(message ? message : "trace failed", true);
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "error", message ? message : "trace failed");
    if (hint) yyjson_mut_obj_add_strcpy(doc, root, "hint", hint);
    return json_result(doc, true);
}

static bool is_test_file(const char *path) {
    if (!path) return false;
    return strstr(path, "/test") != NULL || strstr(path, "test_") != NULL ||
           strstr(path, "_test.") != NULL || strstr(path, "/tests/") != NULL ||
           strstr(path, "/spec/") != NULL || strstr(path, ".test.") != NULL ||
           strncmp(path, "tests/", 6U) == 0 || strncmp(path, "test/", 5U) == 0 ||
           strncmp(path, "spec/", 5U) == 0 || strncmp(path, "__tests__/", 10U) == 0;
}

static long resolution_score(const cbm_node_t *node) {
    long rank = TRACE_RES_MODULE;
    if (node->label) {
        if (strcmp(node->label, "Function") == 0 || strcmp(node->label, "Method") == 0) {
            rank = TRACE_RES_CALLABLE;
        } else if (strcmp(node->label, "Module") != 0 && strcmp(node->label, "File") != 0) {
            rank = TRACE_RES_OTHER;
        }
    }
    long span = (long)node->end_line - (long)node->start_line;
    if (span < 0) span = 0;
    return rank * TRACE_RES_WEIGHT + span;
}

static bool real_callable(const cbm_node_t *node) {
    return node->label &&
           (strcmp(node->label, "Function") == 0 || strcmp(node->label, "Method") == 0) &&
           node->end_line > node->start_line;
}

static bool nodes_ambiguous(const cbm_node_t *nodes, int count) {
    if (count <= 1) return false;
    long best = resolution_score(&nodes[0]);
    for (int i = 1; i < count; ++i) {
        long score = resolution_score(&nodes[i]);
        if (score > best) best = score;
    }
    int top = 0;
    int real = 0;
    for (int i = 0; i < count; ++i) {
        if (resolution_score(&nodes[i]) == best) ++top;
        if (real_callable(&nodes[i])) ++real;
    }
    return top > 1 || real > 1;
}

static cbm_operation_result_t ambiguous_result(const char *input, const cbm_node_t *nodes, int count) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    yyjson_mut_val *items = doc ? yyjson_mut_arr(doc) : NULL;
    if (!doc || !root || !items) {
        if (doc) yyjson_mut_doc_free(doc);
        return error_result("result allocation failed", NULL);
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", "ambiguous");
    yyjson_mut_obj_add_strcpy(doc, root, "input", input ? input : "");
    for (int i = 0; i < count; ++i) {
        yyjson_mut_val *item = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_strcpy(doc, item, "qualified_name", nodes[i].qualified_name ? nodes[i].qualified_name : "");
        yyjson_mut_obj_add_strcpy(doc, item, "label", nodes[i].label ? nodes[i].label : "");
        yyjson_mut_obj_add_strcpy(doc, item, "file_path", nodes[i].file_path ? nodes[i].file_path : "");
        yyjson_mut_arr_add_val(items, item);
    }
    yyjson_mut_obj_add_val(doc, root, "suggestions", items);
    yyjson_mut_obj_add_str(doc, root, "hint", "Choose an exact qualified_name from suggestions, or narrow with search.");
    return json_result(doc, false);
}

static int hop_id_cmp(const void *left, const void *right) {
    const cbm_node_hop_t *a = left;
    const cbm_node_hop_t *b = right;
    if (a->hop != b->hop) return a->hop < b->hop ? -1 : 1;
    if (a->node.id != b->node.id) return a->node.id < b->node.id ? -1 : 1;
    return 0;
}

static bool grow_visited(cbm_traverse_result_t *out, int *capacity) {
    if (out->visited_count < *capacity) return true;
    int next = *capacity ? *capacity * 2 : 8;
    cbm_node_hop_t *grown = realloc(out->visited, (size_t)next * sizeof(*grown));
    if (!grown) return false;
    out->visited = grown;
    *capacity = next;
    return true;
}

static bool grow_edges(cbm_traverse_result_t *out, int *capacity) {
    if (out->edge_count < *capacity) return true;
    int next = *capacity ? *capacity * 2 : 8;
    cbm_edge_info_t *grown = realloc(out->edges, (size_t)next * sizeof(*grown));
    if (!grown) return false;
    out->edges = grown;
    *capacity = next;
    return true;
}

/* Preserve edge properties for data-flow/evidence output. cbm_store_bfs_multi()
 * intentionally returns only nodes today, so trace still unions the individual
 * traversals until the store primitive grows an edge-preserving multi-seed form. */
static bool bfs_union(cbm_store_t *store, const cbm_node_t *seeds, int seed_count,
                      const char *direction, const char **edge_types, int edge_type_count,
                      int depth, int limit, cbm_traverse_result_t *out) {
    memset(out, 0, sizeof(*out));
    int vcap = 0;
    int ecap = 0;
    for (int seed = 0; seed < seed_count; ++seed) {
        cbm_traverse_result_t current = {0};
        if (cbm_store_bfs(store, seeds[seed].id, direction, edge_types, edge_type_count, depth,
                          limit, &current) != CBM_STORE_OK) {
            cbm_store_traverse_free(&current);
            cbm_store_traverse_free(out);
            return false;
        }
        for (int i = 0; i < current.visited_count; ++i) {
            int existing = -1;
            for (int j = 0; j < out->visited_count; ++j) {
                if (out->visited[j].node.id == current.visited[i].node.id) {
                    existing = j;
                    break;
                }
            }
            if (existing >= 0) {
                if (current.visited[i].hop < out->visited[existing].hop)
                    out->visited[existing].hop = current.visited[i].hop;
                continue;
            }
            if (!grow_visited(out, &vcap)) {
                cbm_store_traverse_free(&current);
                cbm_store_traverse_free(out);
                return false;
            }
            out->visited[out->visited_count++] = current.visited[i];
            memset(&current.visited[i], 0, sizeof(current.visited[i]));
        }
        for (int i = 0; i < current.edge_count; ++i) {
            bool duplicate = false;
            for (int j = 0; j < out->edge_count; ++j) {
                const char *a = out->edges[j].type;
                const char *b = current.edges[i].type;
                if (out->edges[j].source_id == current.edges[i].source_id &&
                    out->edges[j].target_id == current.edges[i].target_id &&
                    ((!a && !b) || (a && b && strcmp(a, b) == 0))) {
                    duplicate = true;
                    break;
                }
            }
            if (duplicate) continue;
            if (!grow_edges(out, &ecap)) {
                cbm_store_traverse_free(&current);
                cbm_store_traverse_free(out);
                return false;
            }
            out->edges[out->edge_count++] = current.edges[i];
            memset(&current.edges[i], 0, sizeof(current.edges[i]));
        }
        cbm_store_traverse_free(&current);
    }
    if (out->visited_count > 1)
        qsort(out->visited, (size_t)out->visited_count, sizeof(*out->visited), hop_id_cmp);
    return true;
}

static yyjson_doc *resolve_edge_types(const char *args, const char *mode,
                                      const char **types, int *count) {
    static const char *calls[] = {"CALLS"};
    static const char *data_flow[] = {"CALLS", "DATA_FLOWS"};
    static const char *cross_service[] = {"HTTP_CALLS", "ASYNC_CALLS", "DATA_FLOWS", "CALLS",
                                          "CROSS_HTTP_CALLS", "CROSS_ASYNC_CALLS", "CROSS_CHANNEL",
                                          "CROSS_GRPC_CALLS", "CROSS_GRAPHQL_CALLS", "CROSS_TRPC_CALLS"};
    *count = 0;
    yyjson_doc *doc = args_doc(args);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *array = yyjson_is_obj(root) ? yyjson_obj_get(root, "edge_types") : NULL;
    if (yyjson_is_arr(array)) {
        size_t index, max;
        yyjson_val *value;
        yyjson_arr_foreach(array, index, max, value) {
            if (yyjson_is_str(value) && *count < TRACE_MAX_EDGE_TYPES)
                types[(*count)++] = yyjson_get_str(value);
        }
    }
    if (*count > 0) return doc;
    if (doc) yyjson_doc_free(doc);
    const char **defaults = calls;
    int n = 1;
    if (mode && strcmp(mode, "data_flow") == 0) {
        defaults = data_flow;
        n = 2;
    } else if (mode && strcmp(mode, "cross_service") == 0) {
        defaults = cross_service;
        n = (int)(sizeof(cross_service) / sizeof(cross_service[0]));
    }
    for (int i = 0; i < n; ++i) types[i] = defaults[i];
    *count = n;
    return NULL;
}

static uint64_t fnv1a(const char *text, uint64_t hash) {
    while (text && *text) {
        hash ^= (uint64_t)(unsigned char)*text++;
        hash *= UINT64_C(0x100000001b3);
    }
    return hash;
}

static uint64_t params_hash(const char *project, const char *function, const char *direction,
                            const char *mode, int depth, bool include_tests, int limit) {
    uint64_t hash = UINT64_C(0xcbf29ce484222325);
    hash = fnv1a(project ? project : "", hash); hash = fnv1a("|", hash);
    hash = fnv1a(function ? function : "", hash); hash = fnv1a("|", hash);
    hash = fnv1a(direction ? direction : "", hash); hash = fnv1a("|", hash);
    hash = fnv1a(mode ? mode : "", hash);
    char numbers[64];
    (void)snprintf(numbers, sizeof(numbers), "|%d|%d|%d", depth, include_tests ? 1 : 0, limit);
    return fnv1a(numbers, hash);
}

static void cursor_encode(const trace_cursor_t *cursor, char *buffer, size_t size) {
    (void)snprintf(buffer, size, "c1.%c.%s.%016llx.%d.%lld", cursor->leg, cursor->generation,
                   (unsigned long long)cursor->qhash, cursor->hop, (long long)cursor->node_id);
}

static const char *cursor_decode(const char *token, const char *generation, uint64_t expected,
                                 trace_cursor_t *out) {
    memset(out, 0, sizeof(*out));
    if (!token || strncmp(token, "c1.", 3U) != 0) return "invalid_cursor";
    const char *p = token + 3;
    if (*p != 'o' && *p != 'i') return "invalid_cursor";
    out->leg = *p;
    p += 2;
    const char *end = strchr(p, '.');
    if (!end || (size_t)(end - p) >= sizeof(out->generation)) return "invalid_cursor";
    memcpy(out->generation, p, (size_t)(end - p));
    out->generation[end - p] = '\0';
    unsigned long long hash = 0;
    long long node = 0;
    if (sscanf(end + 1, "%16llx.%d.%lld", &hash, &out->hop, &node) != 3) return "invalid_cursor";
    out->qhash = (uint64_t)hash;
    out->node_id = (int64_t)node;
    if (out->qhash != expected) return "cursor_params_mismatch";
    if (strcmp(out->generation, generation) != 0) return "stale_cursor";
    return NULL;
}

static int watermark_index(const cbm_traverse_result_t *result, int hop, int64_t node_id) {
    for (int i = 0; i < result->visited_count; ++i) {
        if (result->visited[i].hop > hop ||
            (result->visited[i].hop == hop && result->visited[i].node.id > node_id)) return i;
    }
    return result->visited_count;
}

static const char *edge_args(const cbm_traverse_result_t *result, int64_t node_id, size_t *length) {
    for (int i = 0; i < result->edge_count; ++i) {
        if (result->edges[i].source_id != node_id && result->edges[i].target_id != node_id) continue;
        const char *properties = result->edges[i].properties_json;
        const char *key = properties ? strstr(properties, "\"args\"") : NULL;
        const char *open = key ? strchr(key, '[') : NULL;
        if (!open) continue;
        int depth = 0;
        const char *p = open;
        for (; *p; ++p) {
            if (*p == '[') ++depth;
            else if (*p == ']' && --depth == 0) { ++p; break; }
        }
        if (depth != 0) continue;
        *length = (size_t)(p - open);
        return open;
    }
    return NULL;
}

static const char *strategy_class(const char *strategy) {
    if (!strategy || !strategy[0]) return NULL;
    if (strcmp(strategy, "lsp_unresolved") == 0 || strcmp(strategy, "unknown") == 0) return "unresolved";
    if (strncmp(strategy, "lsp_", 4U) == 0) return "lsp";
    if (strncmp(strategy, "php_", 4U) == 0 || strncmp(strategy, "perl_", 5U) == 0) return "language_rule";
    return "heuristic";
}

static bool edge_evidence(const cbm_traverse_result_t *result, int64_t node_id,
                          char class_buffer[32], double *confidence) {
    for (int i = 0; i < result->edge_count; ++i) {
        if (result->edges[i].source_id != node_id && result->edges[i].target_id != node_id) continue;
        const char *properties = result->edges[i].properties_json;
        const char *key = properties ? strstr(properties, "\"strategy\"") : NULL;
        const char *open = key ? strchr(key + 10, '"') : NULL;
        if (!open) continue;
        ++open;
        const char *close = strchr(open, '"');
        if (!close || close == open) continue;
        char raw[64];
        size_t length = (size_t)(close - open);
        if (length >= sizeof(raw)) length = sizeof(raw) - 1U;
        memcpy(raw, open, length); raw[length] = '\0';
        const char *classification = strategy_class(raw);
        if (!classification) continue;
        (void)snprintf(class_buffer, 32U, "%s", classification);
        *confidence = -1.0;
        const char *conf = strstr(properties, "\"confidence\"");
        const char *colon = conf ? strchr(conf, ':') : NULL;
        if (colon) *confidence = strtod(colon + 1, NULL);
        return true;
    }
    return false;
}

static size_t qn_prefix_length(const char *qualified_name) {
    const char *last = qualified_name ? strrchr(qualified_name, '.') : NULL;
    return last ? (size_t)(last - qualified_name) : 0U;
}

static yyjson_mut_val *leg_json(yyjson_mut_doc *doc, const cbm_traverse_result_t *result,
                                bool risk_labels, bool include_tests, bool data_flow,
                                bool include_evidence) {
    yyjson_mut_val *leg = yyjson_mut_obj(doc);
    yyjson_mut_val *columns = yyjson_mut_arr(doc);
    yyjson_mut_arr_add_str(doc, columns, "name");
    yyjson_mut_arr_add_str(doc, columns, "hop");
    if (risk_labels) yyjson_mut_arr_add_str(doc, columns, "risk");
    if (include_evidence) {
        yyjson_mut_arr_add_str(doc, columns, "strategy");
        yyjson_mut_arr_add_str(doc, columns, "confidence");
    }
    if (data_flow) yyjson_mut_arr_add_str(doc, columns, "args");
    yyjson_mut_obj_add_val(doc, leg, "cols", columns);
    yyjson_mut_val *groups = yyjson_mut_arr(doc);
    yyjson_mut_val *rows = NULL;
    char group_name[1024] = "";
    bool have_group = false;
    for (int i = 0; i < result->visited_count; ++i) {
        const cbm_node_hop_t *hop = &result->visited[i];
        if (!include_tests && is_test_file(hop->node.file_path)) continue;
        const char *qn = hop->node.qualified_name ? hop->node.qualified_name : "";
        size_t prefix = qn_prefix_length(qn);
        if (prefix >= sizeof(group_name)) prefix = 0U;
        if (!have_group || strlen(group_name) != prefix || strncmp(group_name, qn, prefix) != 0) {
            (void)snprintf(group_name, sizeof(group_name), "%.*s", (int)prefix, qn);
            have_group = true;
            yyjson_mut_val *group = yyjson_mut_obj(doc);
            yyjson_mut_obj_add_strcpy(doc, group, "qn_prefix", group_name);
            rows = yyjson_mut_arr(doc);
            yyjson_mut_obj_add_val(doc, group, "rows", rows);
            yyjson_mut_arr_add_val(groups, group);
        }
        yyjson_mut_val *row = yyjson_mut_arr(doc);
        yyjson_mut_arr_add_strcpy(doc, row, prefix ? qn + prefix + 1U : qn);
        yyjson_mut_arr_add_int(doc, row, hop->hop);
        if (risk_labels) yyjson_mut_arr_add_str(doc, row, cbm_risk_label(cbm_hop_to_risk(hop->hop)));
        if (include_evidence) {
            char classification[32];
            double confidence = -1.0;
            if (edge_evidence(result, hop->node.id, classification, &confidence)) {
                yyjson_mut_arr_add_strcpy(doc, row, classification);
                if (confidence >= 0.0) yyjson_mut_arr_add_real(doc, row, confidence);
                else yyjson_mut_arr_add_null(doc, row);
            } else {
                yyjson_mut_arr_add_null(doc, row);
                yyjson_mut_arr_add_null(doc, row);
            }
        }
        if (data_flow) {
            size_t length = 0U;
            const char *raw = edge_args(result, hop->node.id, &length);
            yyjson_mut_val *value = raw && length ? yyjson_mut_rawn(doc, raw, length) : NULL;
            if (value) yyjson_mut_arr_add_val(row, value);
            else yyjson_mut_arr_add_str(doc, row, "");
        }
        yyjson_mut_arr_add_val(rows, row);
    }
    yyjson_mut_obj_add_val(doc, leg, "groups", groups);
    return leg;
}

static int visible_count(const cbm_traverse_result_t *result, bool include_tests) {
    int count = 0;
    for (int i = 0; i < result->visited_count; ++i)
        if (include_tests || !is_test_file(result->visited[i].node.file_path)) ++count;
    return count;
}

cbm_operation_result_t cbm_trace_operation_execute(const char *args) {
    char *function = string_arg(args, "function_name");
    char *project = string_arg(args, "project");
    char *direction = string_arg(args, "direction");
    char *mode = string_arg(args, "mode");
    char *cursor_text = string_arg(args, "cursor");
    bool risk_labels = bool_arg(args, "risk_labels");
    bool include_tests = bool_arg(args, "include_tests");
    bool include_evidence = bool_arg(args, "include_evidence");
    int requested_depth = int_arg(args, "depth", TRACE_DEFAULT_DEPTH);
    int requested_limit = int_arg(args, "limit", TRACE_DEFAULT_LIMIT);
    int depth = requested_depth < 1 ? 1 : requested_depth;
    int max_depth = cbm_operation_max_depth();
    if (depth > max_depth) depth = max_depth;
    int limit = requested_limit < 1 ? 1 : requested_limit;
    if (limit > TRACE_MAX_LIMIT) limit = TRACE_MAX_LIMIT;
    if (!direction) direction = copy_text("both");

    cbm_operation_result_t result = {0};
    cbm_store_t *store = NULL;
    cbm_node_t *nodes = NULL;
    int node_count = 0;
    yyjson_doc *edge_doc = NULL;
    cbm_traverse_result_t outbound = {0};
    cbm_traverse_result_t inbound = {0};

    if (!function || !function[0]) { result = error_result("function_name is required", "Use search first to discover a symbol."); goto done; }
    if (!project || !project[0]) { result = error_result("project is required", "Run the command from an indexed repository."); goto done; }
    if (strcmp(direction, "inbound") != 0 && strcmp(direction, "outbound") != 0 && strcmp(direction, "both") != 0) {
        result = error_result("invalid direction", "Use inbound, outbound, or both."); goto done;
    }
    store = cbm_store_open(project);
    if (!store) { result = error_result("project not indexed", "Run 'codebase-memory-cli index .' first."); goto done; }

    (void)cbm_store_find_nodes_by_name(store, project, function, &nodes, &node_count);
    if (node_count == 0) {
        cbm_node_t exact = {0};
        if (cbm_store_find_node_by_qn(store, project, function, &exact) == CBM_STORE_OK) {
            nodes = malloc(sizeof(*nodes));
            if (!nodes) { cbm_node_free_fields(&exact); result = error_result("out of memory", NULL); goto done; }
            nodes[0] = exact;
            node_count = 1;
        }
    }
    if (node_count == 0) { result = error_result("function not found", "Use 'codebase-memory-cli search <term>' to discover the exact qualified name."); goto done; }
    if (nodes_ambiguous(nodes, node_count)) { result = ambiguous_result(function, nodes, node_count); goto done; }

    char generation[96] = "legacy";
    (void)cbm_store_generation(store, generation, sizeof(generation));
    bool legacy_generation = strcmp(generation, "legacy") == 0;
    trace_cursor_t cursor = {0};
    bool have_cursor = cursor_text && cursor_text[0];
    uint64_t hash = params_hash(project, function, direction, mode, requested_depth, include_tests,
                                requested_limit);
    if (have_cursor) {
        if (legacy_generation) { result = error_result("cursor_unsupported", "Re-run with a higher limit or re-index to enable generation-aware cursors."); goto done; }
        const char *cursor_error = cursor_decode(cursor_text, generation, hash, &cursor);
        if (cursor_error) { result = error_result(cursor_error, "Re-run without cursor, or pass it back with all other arguments unchanged."); goto done; }
    }

    const char *edge_types[TRACE_MAX_EDGE_TYPES];
    int edge_type_count = 0;
    edge_doc = resolve_edge_types(args, mode, edge_types, &edge_type_count);
    bool do_outbound = strcmp(direction, "outbound") == 0 || strcmp(direction, "both") == 0;
    bool do_inbound = strcmp(direction, "inbound") == 0 || strcmp(direction, "both") == 0;
    if (do_outbound && !bfs_union(store, nodes, node_count, "outbound", edge_types, edge_type_count,
                                  depth, TRACE_MAX_LIMIT, &outbound)) {
        result = error_result("trace traversal failed", NULL); goto done;
    }
    if (do_inbound && !bfs_union(store, nodes, node_count, "inbound", edge_types, edge_type_count,
                                 depth, TRACE_MAX_LIMIT, &inbound)) {
        result = error_result("trace traversal failed", NULL); goto done;
    }

    int out_start = 0;
    int in_start = 0;
    if (have_cursor) {
        if (cursor.leg == 'o') out_start = watermark_index(&outbound, cursor.hop, cursor.node_id);
        else { out_start = outbound.visited_count; in_start = watermark_index(&inbound, cursor.hop, cursor.node_id); }
    }
    int budget = limit;
    int out_len = do_outbound ? outbound.visited_count - out_start : 0;
    if (out_len > budget) out_len = budget;
    budget -= out_len;
    int in_len = do_inbound ? inbound.visited_count - in_start : 0;
    if (in_len > budget) in_len = budget;
    bool out_more = do_outbound && out_start + out_len < outbound.visited_count;
    bool in_more = do_inbound && in_start + in_len < inbound.visited_count;
    bool more = out_more || in_more;
    char next[192] = "";
    if (more && !legacy_generation) {
        trace_cursor_t next_cursor = {0};
        (void)snprintf(next_cursor.generation, sizeof(next_cursor.generation), "%s", generation);
        next_cursor.qhash = hash;
        if (out_more && out_len > 0) {
            next_cursor.leg = 'o'; next_cursor.hop = outbound.visited[out_start + out_len - 1].hop;
            next_cursor.node_id = outbound.visited[out_start + out_len - 1].node.id;
        } else if (in_len > 0) {
            next_cursor.leg = 'i'; next_cursor.hop = inbound.visited[in_start + in_len - 1].hop;
            next_cursor.node_id = inbound.visited[in_start + in_len - 1].node.id;
        }
        if (next_cursor.leg) cursor_encode(&next_cursor, next, sizeof(next));
    }

    cbm_traverse_result_t out_view = outbound;
    out_view.visited = outbound.visited ? outbound.visited + out_start : NULL;
    out_view.visited_count = out_len;
    cbm_traverse_result_t in_view = inbound;
    in_view.visited = inbound.visited ? inbound.visited + in_start : NULL;
    in_view.visited_count = in_len;

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) { if (doc) yyjson_mut_doc_free(doc); result = error_result("result allocation failed", NULL); goto done; }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_strcpy(doc, root, "project", project);
    yyjson_mut_obj_add_strcpy(doc, root, "function", function);
    yyjson_mut_obj_add_strcpy(doc, root, "direction", direction);
    if (mode) yyjson_mut_obj_add_strcpy(doc, root, "mode", mode);
    if (requested_depth != depth) {
        yyjson_mut_obj_add_int(doc, root, "requested_depth", requested_depth);
        yyjson_mut_obj_add_int(doc, root, "depth", depth);
        yyjson_mut_obj_add_str(doc, root, "depth_note", "requested depth was capped by the configured traversal safety limit");
    }
    bool data_flow = mode && strcmp(mode, "data_flow") == 0;
    if (do_outbound) {
        yyjson_mut_obj_add_int(doc, root, "callees_total", visible_count(&outbound, include_tests));
        yyjson_mut_obj_add_val(doc, root, "callees", leg_json(doc, &out_view, risk_labels, include_tests, data_flow, include_evidence));
    }
    if (do_inbound) {
        yyjson_mut_obj_add_int(doc, root, "callers_total", visible_count(&inbound, include_tests));
        yyjson_mut_obj_add_val(doc, root, "callers", leg_json(doc, &in_view, risk_labels, include_tests, data_flow, include_evidence));
    }
    if (more) {
        yyjson_mut_obj_add_bool(doc, root, "truncated", true);
        if (next[0]) yyjson_mut_obj_add_strcpy(doc, root, "next_cursor", next);
        else yyjson_mut_obj_add_str(doc, root, "hint", "More rows exist; raise limit because this legacy index cannot mint a safe cursor.");
    }
    result = json_result(doc, false);

done:
    if (edge_doc) yyjson_doc_free(edge_doc);
    cbm_store_traverse_free(&outbound);
    cbm_store_traverse_free(&inbound);
    if (nodes) cbm_store_free_nodes(nodes, node_count);
    if (store) cbm_store_close(store);
    free(function); free(project); free(direction); free(mode); free(cursor_text);
    return result;
}
