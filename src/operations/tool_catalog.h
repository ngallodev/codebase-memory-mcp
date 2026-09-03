#ifndef CBM_OPERATIONS_TOOL_CATALOG_H
#define CBM_OPERATIONS_TOOL_CATALOG_H

#include <stddef.h>

size_t cbm_tool_catalog_count(void);
const char *cbm_tool_catalog_name(size_t index);
const char *cbm_tool_catalog_title(const char *name);
const char *cbm_tool_catalog_description(const char *name);
const char *cbm_tool_catalog_input_schema(const char *name);

#endif
