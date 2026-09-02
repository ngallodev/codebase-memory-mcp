#ifndef CBM_OPERATIONS_COMPARE_H
#define CBM_OPERATIONS_COMPARE_H

#include "operations/operation.h"

cbm_operation_result_t cbm_compare_operation_execute(const char *args_json,
                                                     const cbm_operation_runtime_t *runtime);

#endif
