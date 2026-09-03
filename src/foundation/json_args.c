#include "foundation/json_args.h"

#include "yyjson/yyjson.h"

#include <stdlib.h>
#include <string.h>

static char *json_arg_strdup(const char *value) {
    if (!value) return NULL;
    size_t size = strlen(value) + 1;
    char *copy = malloc(size);
    if (copy) memcpy(copy, value, size);
    return copy;
}

char *cbm_json_arg_string(const char *args_json, const char *key) {
    if (!args_json || !key) return NULL;
    yyjson_doc *doc = yyjson_read(args_json, strlen(args_json), 0);
    if (!doc) return NULL;
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, key) : NULL;
    char *result = yyjson_is_str(value) ? json_arg_strdup(yyjson_get_str(value)) : NULL;
    yyjson_doc_free(doc);
    return result;
}

int cbm_json_arg_int(const char *args_json, const char *key, int default_value) {
    if (!args_json || !key) return default_value;
    yyjson_doc *doc = yyjson_read(args_json, strlen(args_json), 0);
    if (!doc) return default_value;
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, key) : NULL;
    int result = yyjson_is_int(value) ? yyjson_get_int(value) : default_value;
    yyjson_doc_free(doc);
    return result;
}

bool cbm_json_arg_bool(const char *args_json, const char *key) {
    if (!args_json || !key) return false;
    yyjson_doc *doc = yyjson_read(args_json, strlen(args_json), 0);
    if (!doc) return false;
    yyjson_val *root = yyjson_doc_get_root(doc);
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, key) : NULL;
    bool result = yyjson_is_bool(value) && yyjson_get_bool(value);
    yyjson_doc_free(doc);
    return result;
}
