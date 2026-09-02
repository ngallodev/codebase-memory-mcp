#include "operations/cross_repo.h"

#include "foundation/constants.h"
#include "foundation/str_util.h"
#include "pipeline/pipeline.h"
#include "pipeline/pass_cross_repo.h"
#include "yyjson/yyjson.h"

#include <stdatomic.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>

#define CROSS_REPO_MAX_TARGETS 4096

static char *cross_repo_strdup(const char *text) {
    if (!text) return NULL;
    size_t len = strlen(text);
    char *copy = malloc(len + 1U);
    if (copy) memcpy(copy, text, len + 1U);
    return copy;
}

static char *cross_repo_string_arg(const char *args, const char *key) {
    yyjson_doc *doc = yyjson_read(args ? args : "{}", args ? strlen(args) : 2U, 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, key) : NULL;
    const char *text = yyjson_is_str(value) ? yyjson_get_str(value) : NULL;
    char *copy = text ? cross_repo_strdup(text) : NULL;
    if (doc) yyjson_doc_free(doc);
    return copy;
}

static cbm_operation_result_t cross_repo_error(const char *message) {
    return cbm_operation_result_copy(message ? message : "cross-repository operation failed", true);
}

static int project_key_compare(const void *left, const void *right) {
    const char *const *a = left;
    const char *const *b = right;
    return strcmp(*a, *b);
}

static unsigned char lock_fold(unsigned char ch) {
    return ch >= 'A' && ch <= 'Z' ? (unsigned char)(ch + ('a' - 'A')) : ch;
}

static int lock_key_compare_values(const char *left, const char *right) {
    const unsigned char *a = (const unsigned char *)left;
    const unsigned char *b = (const unsigned char *)right;
    while (*a && *b) {
        unsigned char af = lock_fold(*a);
        unsigned char bf = lock_fold(*b);
        if (af != bf) return af < bf ? -1 : 1;
        ++a;
        ++b;
    }
    if (*a != *b) return *a ? 1 : -1;
    return strcmp(left, right);
}

static int lock_key_compare(const void *left, const void *right) {
    const char *const *a = left;
    const char *const *b = right;
    return lock_key_compare_values(*a, *b);
}

static bool lock_keys_equivalent(const char *left, const char *right) {
    const unsigned char *a = (const unsigned char *)left;
    const unsigned char *b = (const unsigned char *)right;
    while (*a && *b) {
        if (lock_fold(*a) != lock_fold(*b)) return false;
        ++a;
        ++b;
    }
    return *a == *b;
}

cbm_operation_result_t cbm_cross_repo_operation_execute(const char *repo_path,
                                                         const char *args_json,
                                                         const cbm_operation_runtime_t *runtime) {
    if (!repo_path || !args_json) return cross_repo_error("cross-repository request is incomplete");
    if (!runtime || !runtime->mutation_begin || !runtime->mutation_end) {
        return cross_repo_error("cross-repository mutation coordination is unavailable");
    }

    char *name_override = cross_repo_string_arg(args_json, "name");
    if (name_override && name_override[0] && !cbm_validate_project_name(name_override)) {
        free(name_override);
        return cross_repo_error("invalid project name");
    }
    char *project = name_override && name_override[0] ? cross_repo_strdup(name_override)
                                                       : cbm_project_name_from_path(repo_path);
    free(name_override);
    if (!project) return cross_repo_error("cannot derive project name");

    yyjson_doc *jdoc = yyjson_read(args_json, strlen(args_json), 0);
    yyjson_val *jroot = jdoc ? yyjson_doc_get_root(jdoc) : NULL;
    yyjson_val *array = yyjson_is_obj(jroot) ? yyjson_obj_get(jroot, "target_projects") : NULL;
    if (!array || !yyjson_is_arr(array) || yyjson_arr_size(array) == 0) {
        if (jdoc) yyjson_doc_free(jdoc);
        free(project);
        return cross_repo_error("{\"error\":\"target_projects is required for cross-repo-intelligence mode. Use [\\\"*\\\"] for all projects. Run list_projects to see available.\"}");
    }

    size_t target_count = yyjson_arr_size(array);
    if (target_count > CROSS_REPO_MAX_TARGETS) {
        yyjson_doc_free(jdoc);
        free(project);
        return cross_repo_error("too many cross-repo target projects");
    }
    int tp_count = (int)target_count;
    const char **targets = malloc((size_t)tp_count * sizeof(*targets));
    const char **lease_keys = malloc(((size_t)tp_count + 1U) * sizeof(*lease_keys));
    if (!targets || !lease_keys) {
        free(targets); free(lease_keys); yyjson_doc_free(jdoc); free(project);
        return cross_repo_error("failed to allocate cross-repo project leases");
    }

    size_t index = 0, max = 0;
    yyjson_val *value = NULL;
    int target_index = 0;
    bool all_projects = false;
    bool invalid_target = false;
    yyjson_arr_foreach(array, index, max, value) {
        const char *target = yyjson_is_str(value) ? yyjson_get_str(value) : NULL;
        if (!target || !target[0] || strlen(target) >= CBM_SZ_256 ||
            (strcmp(target, "*") != 0 && !cbm_validate_project_name(target))) {
            invalid_target = true;
            break;
        }
        targets[target_index++] = target;
        all_projects = all_projects || strcmp(target, "*") == 0;
    }
    if (invalid_target || target_index != tp_count) {
        free(targets); free(lease_keys); yyjson_doc_free(jdoc); free(project);
        return cross_repo_error("target_projects must contain valid project names or '*'");
    }
    if (all_projects && tp_count != 1) {
        free(targets); free(lease_keys); yyjson_doc_free(jdoc); free(project);
        return cross_repo_error("target_projects wildcard '*' must be the only entry");
    }
    if (!all_projects) {
        qsort(targets, (size_t)tp_count, sizeof(*targets), project_key_compare);
        int unique_count = 0;
        for (int i = 0; i < tp_count; ++i) {
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
        for (int i = 0; i < tp_count; ++i) lease_keys[lease_count++] = targets[i];
        qsort(lease_keys, (size_t)lease_count, sizeof(*lease_keys), lock_key_compare);
        int unique_count = 0;
        for (int i = 0; i < lease_count; ++i) {
            if (unique_count == 0 || !lock_keys_equivalent(lease_keys[i], lease_keys[unique_count - 1])) {
                lease_keys[unique_count++] = lease_keys[i];
            }
        }
        lease_count = unique_count;
    }

    int held_count = 0;
    while (held_count < lease_count &&
           runtime->mutation_begin(runtime->mutation_context, lease_keys[held_count])) {
        ++held_count;
    }
    bool cancelled = runtime->cancel_flag &&
                     atomic_load_explicit(runtime->cancel_flag, memory_order_acquire) != 0;
    if (held_count != lease_count || cancelled) {
        while (held_count > 0) {
            --held_count;
            runtime->mutation_end(runtime->mutation_context, lease_keys[held_count]);
        }
        free(targets); free(lease_keys); yyjson_doc_free(jdoc); free(project);
        return cross_repo_error("cross-repo operation cancelled or blocked by active indexing");
    }

    atomic_int local_cancel = ATOMIC_VAR_INIT(0);
    atomic_int *cancel_flag = runtime->cancel_flag ? runtime->cancel_flag : &local_cancel;
    cbm_cross_repo_result_t result = cbm_cross_repo_match_cancellable(project, targets, tp_count, cancel_flag);
    while (held_count > 0) {
        --held_count;
        runtime->mutation_end(runtime->mutation_context, lease_keys[held_count]);
    }
    free(targets); free(lease_keys); yyjson_doc_free(jdoc);

    if (result.failed) {
        free(project);
        return cross_repo_error("cross-repo source or target project is missing, invalid, or not indexed");
    }

    int total = result.http_edges + result.async_edges + result.channel_edges + result.grpc_edges +
                result.graphql_edges + result.trpc_edges;
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) {
        if (doc) yyjson_mut_doc_free(doc);
        free(project);
        return cross_repo_error("result allocation failed");
    }
    yyjson_mut_doc_set_root(doc, root);
    yyjson_mut_obj_add_str(doc, root, "status", result.cancelled ? "cancelled" : "success");
    yyjson_mut_obj_add_str(doc, root, "mode", "cross-repo-intelligence");
    yyjson_mut_obj_add_strcpy(doc, root, "project", project);
    if (result.cancelled) {
        yyjson_mut_obj_add_bool(doc, root, "partial_results", result.partial_results);
        yyjson_mut_obj_add_str(doc, root, "message",
            result.partial_results
                ? "cross-repo operation cancelled with partial results; completed database writes were retained"
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
    char *payload = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    free(project);
    if (!payload) return cross_repo_error("result encoding failed");
    return cbm_operation_result_take(payload, result.cancelled);
}
