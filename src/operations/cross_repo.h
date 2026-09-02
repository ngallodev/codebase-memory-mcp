#ifndef CBM_OPERATIONS_CROSS_REPO_H
#define CBM_OPERATIONS_CROSS_REPO_H

#include "operations/operation.h"

cbm_operation_result_t cbm_cross_repo_operation_execute(const char *repo_path,
                                                         const char *args_json,
                                                         const cbm_operation_runtime_t *runtime);

#endif
