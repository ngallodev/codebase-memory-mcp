#include "operations/json_args.h"
#include "yyjson/yyjson.h"
#include <stdlib.h>
#include <string.h>

static char *json_strdup(const char *s) {
    if (!s) return NULL;
    size_t n = strlen(s) + 1U;
    char *out = malloc(n);
    if (out) memcpy(out, s, n);
    return out;
}

char *cbm_json_string_arg(const char *args_json, const char *key) {
    if (!key) return NULL;
    const char *json = args_json ? args_json : "{}";
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, key) : NULL;
    char *out = yyjson_is_str(value) ? json_strdup(yyjson_get_str(value)) : NULL;
    if (doc) yyjson_doc_free(doc);
    return out;
}

int cbm_json_int_arg(const char *args_json, const char *key, int fallback) {
    if (!key) return fallback;
    const char *json = args_json ? args_json : "{}";
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, key) : NULL;
    int out = yyjson_is_int(value) ? (int)yyjson_get_sint(value) : fallback;
    if (doc) yyjson_doc_free(doc);
    return out;
}

bool cbm_json_bool_arg(const char *args_json, const char *key) {
    if (!key) return false;
    const char *json = args_json ? args_json : "{}";
    yyjson_doc *doc = yyjson_read(json, strlen(json), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *value = yyjson_is_obj(root) ? yyjson_obj_get(root, key) : NULL;
    bool out = yyjson_is_bool(value) && yyjson_get_bool(value);
    if (doc) yyjson_doc_free(doc);
    return out;
}
