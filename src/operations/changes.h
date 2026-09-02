#ifndef CBM_OPERATIONS_CHANGES_H
#define CBM_OPERATIONS_CHANGES_H

#include "operations/operation.h"
#include "pipeline/pipeline.h"
#include "store/store.h"

cbm_operation_result_t cbm_changes_operation_execute(const char *args_json,
                                                      const cbm_operation_runtime_t *runtime);

/* Kept public for the existing white-box characterization tests while the
 * implementation moves out of MCP. */
bool cbm_detect_node_in_hunks(const cbm_node_t *node, const cbm_changed_hunk_t *hunks,
                              int hunk_count, const char *file);

#endif
