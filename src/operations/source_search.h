#ifndef CBM_OPERATIONS_SOURCE_SEARCH_H
#define CBM_OPERATIONS_SOURCE_SEARCH_H

#include "operations/operation.h"

#include <stdbool.h>
#include <stddef.h>

cbm_operation_result_t cbm_source_search_operation_execute(
    const char *args_json, const cbm_operation_runtime_t *runtime);

/* Existing characterization seams retained while implementation ownership
 * moves out of MCP. */
bool cbm_search_code_file_pattern_can_prefilter(const char *file_pattern);
void cbm_search_code_build_grep_cmd(char *cmd, size_t cmd_sz, bool use_regex, bool scoped,
                                    const char *file_pattern, const char *tmpfile,
                                    const char *filelist, const char *root_path);

#endif
