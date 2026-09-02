#ifndef CBM_OPERATIONS_MUTATION_H
#define CBM_OPERATIONS_MUTATION_H

#include "operations/operation.h"

bool cbm_mutation_operation_supported(cbm_operation_id_t operation);
cbm_operation_result_t cbm_mutation_operation_execute(cbm_operation_id_t operation,
                                                       const char *args_json,
                                                       const cbm_operation_runtime_t *runtime);

#endif
