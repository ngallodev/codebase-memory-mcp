#ifndef CBM_FOUNDATION_JSON_ARGS_H
#define CBM_FOUNDATION_JSON_ARGS_H

#include <stdbool.h>

char *cbm_json_arg_string(const char *args_json, const char *key);
int cbm_json_arg_int(const char *args_json, const char *key, int default_value);
bool cbm_json_arg_bool(const char *args_json, const char *key);

#endif
