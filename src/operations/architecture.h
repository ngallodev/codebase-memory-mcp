#ifndef CBM_OPERATIONS_ARCHITECTURE_H
#define CBM_OPERATIONS_ARCHITECTURE_H

#include "operations/operation.h"

#ifdef __cplusplus
extern "C" {
#endif

cbm_operation_result_t cbm_architecture_operation_execute(const char *args_json);

#ifdef __cplusplus
}
#endif

#endif
