#ifndef CBM_OPERATIONS_ADR_H
#define CBM_OPERATIONS_ADR_H

#include "operations/operation.h"

cbm_operation_result_t cbm_adr_operation_execute(const char *args_json,
                                                  const cbm_operation_runtime_t *runtime);

#endif
