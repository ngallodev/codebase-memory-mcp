#ifndef CBM_OPERATIONS_RESULT_WIRE_H
#define CBM_OPERATIONS_RESULT_WIRE_H

#include <stdbool.h>
#include "operations/operation.h"

/* Internal worker/coordinator transport for an operation result. This is not a
 * user-facing protocol and deliberately carries only the neutral payload plus
 * its error bit. */
char *cbm_operation_result_wire_encode(const cbm_operation_result_t *result);
bool cbm_operation_result_wire_decode(const char *wire, cbm_operation_result_t *result_out);

#endif
