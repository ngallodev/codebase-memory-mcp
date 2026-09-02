#include "operations/adr.h"
#include "operations/project_arg.h"

#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "store/store.h"
#include "yyjson/yyjson.h"

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define ADR_EMPTY_HINT                                                             \
    "No ADR yet. Create one with manage_adr(mode='update', "                       \
    "content='## PURPOSE\\n...\\n\\n## STACK\\n...\\n\\n## ARCHITECTURE\\n..."     \
    "\\n\\n## PATTERNS\\n...\\n\\n## TRADEOFFS\\n...\\n\\n## PHILOSOPHY\\n...'). " \
    "For guided creation: explore the codebase with get_architecture, "            \
    "then draft and store. Sections: PURPOSE, STACK, ARCHITECTURE, "               \
    "PATTERNS, TRADEOFFS, PHILOSOPHY."

static char *adr_strdup(const char *text) {
    if (!text) return NULL;
    size_t n = strlen(text);
    char *copy = malloc(n + 1U);
    if (copy) memcpy(copy, text, n + 1U);
    return copy;
}

static char *adr_string_arg(const char *args, const char *key) {
    yyjson_doc *doc = yyjson_read(args ? args : "{}", args ? strlen(args) : 2U, 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, key) : NULL;
    const char *text = yyjson_is_str(value) ? yyjson_get_str(value) : NULL;
    char *copy = text ? adr_strdup(text) : NULL;
    if (doc) yyjson_doc_free(doc);
    return copy;
}

static cbm_operation_result_t adr_take_doc(yyjson_mut_doc *doc, bool is_error) {
    char *json = doc ? yyjson_mut_write(doc, 0, NULL) : NULL;
    if (doc) yyjson_mut_doc_free(doc);
    if (!json) return cbm_operation_result_copy("{\"error\":\"result encoding failed\"}", true);
    return cbm_operation_result_take(json, is_error);
}

static cbm_operation_result_t adr_error(const char *status, const char *error) {
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) {
        if (doc) yyjson_mut_doc_free(doc);
        return cbm_operation_result_copy("{\"error\":\"result allocation failed\"}", true);
    }
    yyjson_mut_doc_set_root(doc, root);
    if (status) yyjson_mut_obj_add_strcpy(doc, root, "status", status);
    yyjson_mut_obj_add_strcpy(doc, root, "error", error ? error : "ADR operation failed");
    return adr_take_doc(doc, true);
}

typedef struct {
    yyjson_mut_doc *doc;
    yyjson_mut_val *sections;
} adr_sections_ctx_t;

static void adr_sections_cb(void *context, const cbm_adr_heading_t *heading) {
    adr_sections_ctx_t *ctx = context;
    yyjson_mut_arr_add_strncpy(ctx->doc, ctx->sections, heading->name, heading->name_len);
}

static void adr_list_sections_from_content(yyjson_mut_doc *doc, yyjson_mut_val *root,
                                           const char *content) {
    yyjson_mut_val *sections = yyjson_mut_arr(doc);
    adr_sections_ctx_t ctx = {doc, sections};
    if (content && cbm_adr_scan_headings(content, adr_sections_cb, &ctx) != CBM_STORE_OK) {
        yyjson_mut_obj_add_str(doc, root, "sections_status", "unterminated_code_fence");
    }
    yyjson_mut_obj_add_val(doc, root, "sections", sections);
}

static char *adr_read_legacy_file(const char *root_path) {
    if (!root_path) return NULL;
    char path[CBM_SZ_4K];
    if (snprintf(path, sizeof(path), "%s/.codebase-memory/adr.md", root_path) >= (int)sizeof(path)) {
        return NULL;
    }
    FILE *fp = cbm_fopen(path, "r");
    if (!fp) return NULL;
    (void)fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    if (size <= 0) { (void)fclose(fp); return NULL; }
    (void)fseek(fp, 0, SEEK_SET);
    char *buffer = malloc((size_t)size + 1U);
    if (!buffer) { (void)fclose(fp); return NULL; }
    size_t read_size = fread(buffer, 1U, (size_t)size, fp);
    buffer[read_size] = '\0';
    (void)fclose(fp);
    if (!buffer[0]) { free(buffer); return NULL; }
    return buffer;
}

static char *adr_project_root(cbm_store_t *store, const char *project) {
    if (!store || !project) return NULL;
    cbm_project_t value = {0};
    if (cbm_store_get_project(store, project, &value) != CBM_STORE_OK) return NULL;
    char *root = adr_strdup(value.root_path);
    cbm_project_free_fields(&value);
    return root;
}

static cbm_store_t *adr_open_store_for_write(const cbm_operation_runtime_t *runtime,
                                             cbm_store_t *resolved, cbm_store_t **owned_rw) {
    if (!resolved || !owned_rw) return NULL;
    const char *path = cbm_store_db_path(resolved);
    if (!path) return resolved;
    char *copy = adr_strdup(path);
    if (!copy) return NULL;
    if (runtime && runtime->store_invalidate) runtime->store_invalidate(runtime->store_context);
    *owned_rw = cbm_store_open_path(copy);
    free(copy);
    return *owned_rw;
}

typedef struct {
    char *keys[PROPS_MAX];
    char *values[PROPS_MAX];
    int count;
    const char *status;
    const char *error;
} adr_section_updates_t;

static void adr_updates_free(adr_section_updates_t *updates) {
    if (!updates) return;
    for (int i = 0; i < updates->count; ++i) {
        free(updates->keys[i]);
        free(updates->values[i]);
    }
    updates->count = 0;
}

static bool adr_collect_update(adr_section_updates_t *updates, yyjson_val *key, yyjson_val *value) {
    const char *name = yyjson_get_str(key);
    if (!name || !name[0]) {
        updates->status = "invalid_section_updates";
        updates->error = "'section_updates' keys must be non-empty section names. No ADR write was performed.";
        return false;
    }
    if (!yyjson_is_str(value)) {
        updates->status = "invalid_section_updates";
        updates->error = "'section_updates' values must be strings (the new body for that section). No ADR write was performed.";
        return false;
    }
    const char *body = yyjson_get_str(value);
    if (!body || !body[0]) {
        updates->status = "empty_section_content";
        updates->error = "'section_updates' values must be non-empty; use mode='update' to remove a section. No ADR write was performed.";
        return false;
    }
    if (updates->count >= PROPS_MAX) {
        updates->status = "too_many_sections";
        updates->error = "'section_updates' carries more entries than an ADR can hold. No ADR write was performed.";
        return false;
    }
    updates->keys[updates->count] = adr_strdup(name);
    updates->values[updates->count] = adr_strdup(body);
    if (!updates->keys[updates->count] || !updates->values[updates->count]) {
        free(updates->keys[updates->count]);
        free(updates->values[updates->count]);
        updates->keys[updates->count] = NULL;
        updates->values[updates->count] = NULL;
        updates->status = "write_error";
        updates->error = "out of memory parsing 'section_updates'. No ADR write was performed.";
        return false;
    }
    updates->count++;
    return true;
}

static adr_section_updates_t adr_parse_updates(const char *args) {
    adr_section_updates_t updates = {0};
    yyjson_doc *doc = yyjson_read(args, strlen(args), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *object = yyjson_is_obj(root) ? yyjson_obj_get(root, "section_updates") : NULL;
    if (!object) {
        updates.status = "missing_section_updates";
        updates.error = "mode='set_sections' requires 'section_updates', an object mapping section name to its new body. No ADR write was performed.";
    } else if (!yyjson_is_obj(object) || yyjson_obj_size(object) == 0) {
        updates.status = "invalid_section_updates";
        updates.error = "'section_updates' must be a non-empty object mapping section name to its new body. No ADR write was performed.";
    } else {
        size_t index = 0, max = 0;
        yyjson_val *key = NULL, *value = NULL;
        yyjson_obj_foreach(object, index, max, key, value) {
            if (!adr_collect_update(&updates, key, value)) {
                adr_updates_free(&updates);
                break;
            }
        }
    }
    if (doc) yyjson_doc_free(doc);
    return updates;
}

static bool adr_has_removed_sections_arg(const char *args) {
    yyjson_doc *doc = yyjson_read(args, strlen(args), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    bool found = yyjson_is_obj(root) && yyjson_obj_get(root, "sections") != NULL;
    if (doc) yyjson_doc_free(doc);
    return found;
}

cbm_operation_result_t cbm_adr_operation_execute(const char *args_json,
                                                  const cbm_operation_runtime_t *runtime) {
    const char *args = args_json ? args_json : "{}";
    char *project = cbm_operation_project_arg(args);
    char *mode = adr_string_arg(args, "mode");
    char *content = adr_string_arg(args, "content");
    if (!mode) mode = adr_strdup("get");

    if (adr_has_removed_sections_arg(args)) {
        free(project); free(mode); free(content);
        return adr_error("invalid_arguments", "The sections argument is not an update primitive and has been removed. No ADR write was performed.");
    }

    bool set_sections = mode && strcmp(mode, "set_sections") == 0;
    adr_section_updates_t updates = {0};
    char section_error[CBM_SZ_256] = "";
    if (set_sections) {
        updates = adr_parse_updates(args);
        if (!updates.status && cbm_adr_validate_section_keys((const char **)updates.keys, updates.count,
                                                             section_error, (int)sizeof(section_error)) != CBM_STORE_OK) {
            adr_updates_free(&updates);
            updates.status = "invalid_section_name";
            updates.error = section_error;
        }
        if (updates.status) {
            cbm_operation_result_t result = adr_error(updates.status, updates.error);
            adr_updates_free(&updates); free(project); free(mode); free(content);
            return result;
        }
    }

    bool write_request = (content && mode && (strcmp(mode, "update") == 0 || strcmp(mode, "store") == 0)) || set_sections;
    if (!runtime || !runtime->store_resolve) {
        adr_updates_free(&updates); free(project); free(mode); free(content);
        return adr_error("store_unavailable", "project store resolution is unavailable for this execution path");
    }

    bool mutation_held = false;
    if (write_request && project) {
        if (!runtime->mutation_begin || !runtime->mutation_end ||
            !runtime->mutation_begin(runtime->mutation_context, project)) {
            adr_updates_free(&updates); free(project); free(mode); free(content);
            return adr_error("busy", "project operation cancelled or blocked by an active index");
        }
        mutation_held = true;
        if (runtime->cancelled && runtime->cancelled(runtime->cancelled_context)) {
            runtime->mutation_end(runtime->mutation_context, project);
            adr_updates_free(&updates); free(project); free(mode); free(content);
            return adr_error("cancelled", "project operation cancelled for this request");
        }
    }

    cbm_operation_store_recovery_status_t recovery = CBM_OPERATION_STORE_RECOVERY_NONE;
    cbm_store_t *resolved = runtime->store_resolve(runtime->store_context, project, mutation_held,
                                                    !write_request, &recovery);
    if (!resolved) {
        cbm_operation_result_t result;
        if (recovery == CBM_OPERATION_STORE_RECOVERY_BUSY) {
            result = adr_error("busy", "project is busy; retry after indexing");
        } else if (recovery == CBM_OPERATION_STORE_RECOVERY_TRY_GUARD_UNAVAILABLE) {
            result = adr_error("coordination_unavailable", "project recovery requires a nonblocking mutation guard");
        } else if (runtime->store_error) {
            char *error_json = runtime->store_error(runtime->store_context, project);
            result = cbm_operation_result_take(error_json, true);
            if (!error_json) result = adr_error("not_found", "project not found or not indexed");
        } else {
            result = adr_error("not_found", project ? "project not found or not indexed" : "missing required argument: project");
        }
        if (mutation_held) runtime->mutation_end(runtime->mutation_context, project);
        adr_updates_free(&updates); free(project); free(mode); free(content);
        return result;
    }

    cbm_store_t *store = resolved;
    cbm_store_t *owned_rw = NULL;
    if (write_request) {
        store = adr_open_store_for_write(runtime, resolved, &owned_rw);
        if (!store) {
            if (mutation_held) runtime->mutation_end(runtime->mutation_context, project);
            adr_updates_free(&updates); free(project); free(mode); free(content);
            return adr_error("open_failed", "failed to open writable ADR store");
        }
    }

    cbm_adr_t adr = {0};
    bool have_adr = cbm_store_adr_get(store, project, &adr) == CBM_STORE_OK;
    char *legacy = NULL;
    if (!have_adr && !write_request) {
        char *root_path = adr_project_root(store, project);
        legacy = adr_read_legacy_file(root_path);
        free(root_path);
        if (legacy && runtime->mutation_try_begin && runtime->mutation_end &&
            runtime->mutation_try_begin(runtime->mutation_context, project)) {
            if (!runtime->cancelled || !runtime->cancelled(runtime->cancelled_context)) {
                if (cbm_store_db_path(resolved)) {
                    if (runtime->store_invalidate) runtime->store_invalidate(runtime->store_context);
                    resolved = runtime->store_resolve(runtime->store_context, project, true, false, NULL);
                    store = resolved;
                }
                if (resolved) {
                    store = adr_open_store_for_write(runtime, resolved, &owned_rw);
                    if (store) {
                        have_adr = cbm_store_adr_get(store, project, &adr) == CBM_STORE_OK;
                        if (!have_adr && cbm_store_adr_store(store, project, legacy) == CBM_STORE_OK) {
                            have_adr = cbm_store_adr_get(store, project, &adr) == CBM_STORE_OK;
                        }
                    }
                }
            }
            runtime->mutation_end(runtime->mutation_context, project);
        }
    }

    char *legacy_seed = NULL;
    if (set_sections && !have_adr) {
        char *root_path = adr_project_root(store, project);
        legacy_seed = adr_read_legacy_file(root_path);
        free(root_path);
    }

    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) {
        if (doc) yyjson_mut_doc_free(doc);
        if (have_adr) cbm_store_adr_free(&adr);
        if (owned_rw) cbm_store_close(owned_rw);
        if (mutation_held) runtime->mutation_end(runtime->mutation_context, project);
        adr_updates_free(&updates); free(legacy_seed); free(legacy); free(project); free(mode); free(content);
        return cbm_operation_result_copy("{\"error\":\"result allocation failed\"}", true);
    }
    yyjson_mut_doc_set_root(doc, root);
    bool is_error = false;
    const char *adr_content = have_adr ? adr.content : legacy;
    if (set_sections) {
        bool base_present = have_adr;
        bool seeded_empty = false;
        if (!base_present) {
            const char *seed = legacy_seed ? legacy_seed : "";
            if (cbm_store_adr_store(store, project, seed) == CBM_STORE_OK) {
                base_present = true;
                seeded_empty = legacy_seed == NULL;
            }
        }
        cbm_adr_t updated = {0};
        int rc = base_present ? cbm_store_adr_update_sections(store, project,
                         (const char **)updates.keys, (const char **)updates.values,
                         updates.count, &updated) : CBM_STORE_ERR;
        if (rc == CBM_STORE_OK) {
            yyjson_mut_obj_add_str(doc, root, "status", "sections_updated");
            yyjson_mut_obj_add_str(doc, root, "semantics", "named_sections_replaced_rest_preserved");
            yyjson_mut_obj_add_uint(doc, root, "sections_written", (uint64_t)updates.count);
            yyjson_mut_obj_add_uint(doc, root, "content_length", (uint64_t)strlen(updated.content));
            cbm_store_adr_free(&updated);
        } else {
            if (seeded_empty) (void)cbm_store_adr_delete(store, project);
            yyjson_mut_obj_add_str(doc, root, "status", "write_error");
            const char *store_error = cbm_store_error(store);
            if (store_error && store_error[0]) yyjson_mut_obj_add_strcpy(doc, root, "error", store_error);
            is_error = true;
        }
    } else if (write_request) {
        if (cbm_store_adr_store(store, project, content) == CBM_STORE_OK) {
            yyjson_mut_obj_add_str(doc, root, "status", "updated");
            yyjson_mut_obj_add_str(doc, root, "semantics", "whole_document_replaced");
        } else {
            yyjson_mut_obj_add_str(doc, root, "status", "write_error");
            is_error = true;
        }
    } else if (mode && strcmp(mode, "sections") == 0) {
        adr_list_sections_from_content(doc, root, adr_content);
    } else {
        if (adr_content) {
            yyjson_mut_obj_add_strcpy(doc, root, "content", adr_content);
        } else {
            yyjson_mut_obj_add_str(doc, root, "content", "");
            yyjson_mut_obj_add_str(doc, root, "status", "no_adr");
            yyjson_mut_obj_add_str(doc, root, "adr_hint", ADR_EMPTY_HINT);
        }
    }

    cbm_operation_result_t result = adr_take_doc(doc, is_error);
    if (have_adr) cbm_store_adr_free(&adr);
    if (owned_rw) cbm_store_close(owned_rw);
    if (mutation_held) runtime->mutation_end(runtime->mutation_context, project);
    adr_updates_free(&updates);
    free(legacy_seed); free(legacy); free(project); free(mode); free(content);
    return result;
}
