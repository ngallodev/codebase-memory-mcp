#ifndef CBM_OPERATIONS_COMMAND_RUNNER_H
#define CBM_OPERATIONS_COMMAND_RUNNER_H

#include "operations/operation.h"
#include "foundation/constants.h"
#include "foundation/subprocess.h"

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef enum cbm_operation_command_cause {
    CBM_OPERATION_COMMAND_SUCCESS = 0,
    CBM_OPERATION_COMMAND_FAILURE,
    CBM_OPERATION_COMMAND_CONTAINED_FAILURE,
    CBM_OPERATION_COMMAND_OUTPUT_LIMIT,
    CBM_OPERATION_COMMAND_DEADLINE,
    CBM_OPERATION_COMMAND_CANCELLED,
    CBM_OPERATION_COMMAND_SUPERVISION_FAILURE,
} cbm_operation_command_cause_t;

bool cbm_operation_runtime_cancelled(const cbm_operation_runtime_t *runtime);

cbm_operation_command_cause_t cbm_operation_run_shell_command_bounded(
    const cbm_operation_runtime_t *runtime, const char *command,
    char output_path[CBM_SZ_2K], size_t output_limit, uint64_t deadline_ms,
    bool deadline_enabled, bool deadline_latched, bool exit_one_is_no_match,
    cbm_proc_result_t *result_out);

/* Compatibility shape used by detect-changes style callers: any contained
 * terminal process tree is considered wrapper success and the caller inspects
 * result_out for exit/cancellation classification. */
int cbm_operation_run_shell_command(const cbm_operation_runtime_t *runtime,
                                    const char *command,
                                    char output_path[CBM_SZ_2K],
                                    cbm_proc_result_t *result_out);

#endif
