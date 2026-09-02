#include "operations/schema.h"

#include "store/store.h"
#include "yyjson/yyjson.h"

#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

static char *copy_string(const char *text) {
    if (!text) return NULL;
    size_t len = strlen(text);
    char *copy = malloc(len + 1U);
    if (copy) memcpy(copy, text, len + 1U);
    return copy;
}

static char *project_arg(const char *args_json) {
    yyjson_doc *doc = yyjson_read(args_json ? args_json : "{}", strlen(args_json ? args_json : "{}"), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    static const char *const names[] = {"project", "project_name", "project_id", "projectName"};
    char *result = NULL;
    for (size_t i = 0; yyjson_is_obj(root) && i < sizeof(names) / sizeof(names[0]); ++i) {
        yyjson_val *value = yyjson_obj_get(root, names[i]);
        if (yyjson_is_str(value)) { result = copy_string(yyjson_get_str(value)); break; }
    }
    if (doc) yyjson_doc_free(doc);
    return result;
}

static bool blocked_property(const char *name) {
    return name && (strcmp(name, "fp") == 0 || strcmp(name, "sp") == 0 || strcmp(name, "bt") == 0);
}

static bool project_has_adr(cbm_store_t *store, const char *project, const char *root_path) {
    cbm_adr_t adr = {0};
    if (store && project && cbm_store_adr_get(store, project, &adr) == CBM_STORE_OK) {
        cbm_store_adr_free(&adr);
        return true;
    }
    if (!root_path) return false;
    char path[4096];
    if (snprintf(path, sizeof(path), "%s/.codebase-memory/adr.md", root_path) >= (int)sizeof(path)) return false;
    struct stat st;
    return stat(path, &st) == 0;
}

cbm_operation_result_t cbm_schema_operation_execute(const char *args_json) {
    char *project = project_arg(args_json);
    if (!project || !project[0]) {
        free(project);
        return cbm_operation_result_copy("project is required", true);
    }
    cbm_store_t *store = cbm_store_open(project);
    if (!store) {
        free(project);
        return cbm_operation_result_copy("project not found or not indexed", true);
    }
    if (cbm_store_count_nodes(store, project) <= 0) {
        cbm_store_close(store);
        free(project);
        return cbm_operation_result_copy("project not indexed or index is empty", true);
    }

    cbm_schema_info_t schema = {0};
    cbm_store_get_schema(store, project, &schema);
    yyjson_mut_doc *doc = yyjson_mut_doc_new(NULL);
    yyjson_mut_val *root = doc ? yyjson_mut_obj(doc) : NULL;
    if (!doc || !root) {
        if (doc) yyjson_mut_doc_free(doc);
        cbm_store_schema_free(&schema);
        cbm_store_close(store);
        free(project);
        return cbm_operation_result_copy("result allocation failed", true);
    }
    yyjson_mut_doc_set_root(doc, root);

    yyjson_mut_val *labels = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.node_label_count; ++i) {
        yyjson_mut_val *label = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, label, "label", schema.node_labels[i].label);
        yyjson_mut_obj_add_int(doc, label, "count", schema.node_labels[i].count);
        yyjson_mut_val *properties = yyjson_mut_arr(doc);
        for (int j = 0; j < schema.node_labels[i].property_count; ++j) {
            if (!blocked_property(schema.node_labels[i].properties[j]))
                yyjson_mut_arr_add_str(doc, properties, schema.node_labels[i].properties[j]);
        }
        yyjson_mut_obj_add_val(doc, label, "properties", properties);
        yyjson_mut_arr_add_val(labels, label);
    }
    yyjson_mut_obj_add_val(doc, root, "node_labels", labels);

    yyjson_mut_val *types = yyjson_mut_arr(doc);
    for (int i = 0; i < schema.edge_type_count; ++i) {
        yyjson_mut_val *type = yyjson_mut_obj(doc);
        yyjson_mut_obj_add_str(doc, type, "type", schema.edge_types[i].type);
        yyjson_mut_obj_add_int(doc, type, "count", schema.edge_types[i].count);
        yyjson_mut_val *properties = yyjson_mut_arr(doc);
        for (int j = 0; j < schema.edge_types[i].property_count; ++j)
            yyjson_mut_arr_add_str(doc, properties, schema.edge_types[i].properties[j]);
        yyjson_mut_obj_add_val(doc, type, "properties", properties);
        yyjson_mut_arr_add_val(types, type);
    }
    yyjson_mut_obj_add_val(doc, root, "edge_types", types);

    cbm_project_t info = {0};
    if (cbm_store_get_project(store, project, &info) == CBM_STORE_OK && info.root_path) {
        bool adr_present = project_has_adr(store, project, info.root_path);
        yyjson_mut_obj_add_bool(doc, root, "adr_present", adr_present);
        if (!adr_present) {
            yyjson_mut_obj_add_str(doc, root, "adr_hint",
                "No ADR found. Use manage_adr(mode='update') to persist architectural decisions across sessions. Run get_architecture(aspects=['all']) first.");
        }
        cbm_project_free_fields(&info);
    }

    char *payload = yyjson_mut_write(doc, 0, NULL);
    yyjson_mut_doc_free(doc);
    cbm_store_schema_free(&schema);
    cbm_store_close(store);
    free(project);
    return payload ? cbm_operation_result_take(payload, false)
                   : cbm_operation_result_copy("result encoding failed", true);
}
