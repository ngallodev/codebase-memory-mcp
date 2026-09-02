#include "operations/mutation.h"
#include "operations/project_arg.h"
#include "operations/index.h"

#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/mem.h"
#include "foundation/platform.h"
#include "foundation/str_util.h"
#include "pipeline/pipeline.h"
#include "yyjson/yyjson.h"

#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static cbm_operation_result_t mutation_json_result(const char *project, const char *status,
                                                    const char *error_detail, bool is_error) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) {
        if (doc) yyjson_mut_doc_free(doc);
        return cbm_operation_result_copy("{\"error\":\"result allocation failed\"}", true);
    }
    yyjson_mut_doc_set_root(doc, root);
    if (project) yyjson_mut_obj_add_strcpy(doc, root, "project", project);
    yyjson_mut_obj_add_strcpy(doc, root, "status", status ? status : "failed");
    if (error_detail) yyjson_mut_obj_add_strcpy(doc, root, "error", error_detail);
    char *json = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    if (!json) return cbm_operation_result_copy("{\"error\":\"result encoding failed\"}", true);
    return cbm_operation_result_take(json, is_error);
}


static cbm_operation_result_t execute_delete_project(const char *args_json,
                                                      const cbm_operation_runtime_t *runtime) {
    char *project = cbm_operation_project_arg(args_json);
    if (!project || !project[0]) {
        free(project);
        return cbm_operation_result_copy("{\"error\":\"project is required\"}", true);
    }
    if (!cbm_validate_project_name(project)) {
        cbm_operation_result_t result = mutation_json_result(project, "invalid_project",
                                                              "invalid project name", true);
        free(project);
        return result;
    }
    if (!runtime || !runtime->mutation_begin || !runtime->mutation_end) {
        cbm_operation_result_t result = mutation_json_result(
            project, "coordination_unavailable",
            "project mutation coordination is unavailable for this execution path", true);
        free(project);
        return result;
    }
    if (!runtime->mutation_begin(runtime->mutation_context, project)) {
        cbm_operation_result_t result = mutation_json_result(
            project, "busy",
            "project operation cancelled or blocked by an active index or mutation", true);
        free(project);
        return result;
    }

    if (runtime->project_detach) {
        runtime->project_detach(runtime->project_detach_context, project);
    }

    const char *cache_dir = cbm_resolve_cache_dir();
    char path[CBM_SZ_1K];
    char wal[CBM_SZ_1K];
    char shm[CBM_SZ_1K];
    bool path_ok = cache_dir && cache_dir[0] &&
                   snprintf(path, sizeof(path), "%s/%s.db", cache_dir, project) < (int)sizeof(path) &&
                   snprintf(wal, sizeof(wal), "%s-wal", path) < (int)sizeof(wal) &&
                   snprintf(shm, sizeof(shm), "%s-shm", path) < (int)sizeof(shm);

    const char *status = "not_found";
    char error_copy[CBM_SZ_256] = {0};
    const char *error_detail = NULL;
    bool is_error = false;

    if (!path_ok) {
        status = "delete_failed";
        error_detail = "project database path is unavailable or too long";
        is_error = true;
    } else {
        cbm_pipeline_lock();
        cbm_path_info_t info = {0};
        bool exists = cbm_path_info_utf8(path, &info) == 0;
        if (exists) {
            int rc = cbm_unlink(path);
            (void)cbm_unlink(wal);
            (void)cbm_unlink(shm);
            if (rc == 0) {
                status = "deleted";
            } else {
                status = "delete_failed";
                (void)snprintf(error_copy, sizeof(error_copy), "%s", strerror(errno));
                error_detail = error_copy;
                is_error = true;
            }
        } else {
            is_error = true;
        }
        cbm_pipeline_unlock();
    }

    cbm_mem_collect();
    runtime->mutation_end(runtime->mutation_context, project);
    cbm_operation_result_t result = mutation_json_result(project, status, error_detail, is_error);
    free(project);
    return result;
}

bool cbm_mutation_operation_supported(cbm_operation_id_t operation) {
    return operation == CBM_OPERATION_DELETE_PROJECT || operation == CBM_OPERATION_INDEX;
}

cbm_operation_result_t cbm_mutation_operation_execute(cbm_operation_id_t operation,
                                                       const char *args_json,
                                                       const cbm_operation_runtime_t *runtime) {
    switch (operation) {
    case CBM_OPERATION_DELETE_PROJECT:
        return execute_delete_project(args_json, runtime);
    case CBM_OPERATION_INDEX:
        return cbm_index_operation_execute(args_json, runtime);
    default:
        return cbm_operation_result_copy("unsupported mutation operation", true);
    }
}
