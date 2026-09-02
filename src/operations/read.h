#ifndef CBM_OPERATIONS_READ_H
#define CBM_OPERATIONS_READ_H

#include "operations/operation.h"

bool cbm_read_operation_supported(cbm_operation_id_t operation);
cbm_operation_result_t cbm_read_operation_execute(cbm_operation_id_t operation,
                                                  const char *args_json);

#endif
