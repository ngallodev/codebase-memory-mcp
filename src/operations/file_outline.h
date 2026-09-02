#ifndef CBM_OPERATIONS_FILE_OUTLINE_H
#define CBM_OPERATIONS_FILE_OUTLINE_H

#include "operations/operation.h"

#ifdef __cplusplus
extern "C" {
#endif

cbm_operation_result_t cbm_file_outline_operation_execute(const char *args_json,
                                                           const cbm_operation_runtime_t *runtime);

#ifdef __cplusplus
}
#endif

#endif
