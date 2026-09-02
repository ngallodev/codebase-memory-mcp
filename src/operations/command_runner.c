#include "operations/command_runner.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/platform.h"

#ifdef _WIN32
#include "foundation/win_utf8.h"
#include <direct.h>
#include <io.h>
#include <windows.h>
#else
#include <unistd.h>
#endif

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define OPERATION_COMMAND_POLL_US 1000LL

bool cbm_operation_runtime_cancelled(const cbm_operation_runtime_t *runtime) {
    return runtime && runtime->cancelled && runtime->cancelled(runtime->cancelled_context);
}

static bool operation_command_output_path(char out[CBM_SZ_2K]) {
    char directory[CBM_SZ_1K];
    const char *cache = cbm_resolve_cache_dir();
    int written;
    if (cache && cache[0]) {
        written = snprintf(directory, sizeof(directory), "%s/logs", cache);
        if (written <= 0 || written >= (int)sizeof(directory) ||
            !cbm_mkdir_p(directory, 0700)) {
            return false;
        }
    } else {
        written = snprintf(directory, sizeof(directory), "%s", cbm_tmpdir());
        if (written <= 0 || written >= (int)sizeof(directory)) {
            return false;
        }
    }
    written = snprintf(out, CBM_SZ_2K, "%s/.operation-command-XXXXXX", directory);
    if (written <= 0 || written >= CBM_SZ_2K) {
        out[0] = '\0';
        return false;
    }
    int descriptor = cbm_mkstemp(out);
    if (descriptor < 0) {
        out[0] = '\0';
        return false;
    }
#ifdef _WIN32
    (void)_close(descriptor);
#else
    (void)close(descriptor);
#endif
    return true;
}

#ifdef _WIN32
static bool operation_resolve_windows_cmd(char out[CBM_SZ_4K]) {
    if (!out) {
        return false;
    }
    out[0] = '\0';
    wchar_t system_directory[MAX_PATH + 1];
    UINT directory_length = GetSystemDirectoryW(system_directory, MAX_PATH + 1);
    static const wchar_t suffix[] = L"\\cmd.exe";
    if (directory_length == 0 || directory_length > MAX_PATH ||
        (size_t)directory_length + (sizeof(suffix) / sizeof(suffix[0])) >
            sizeof(system_directory) / sizeof(system_directory[0])) {
        return false;
    }
    memcpy(system_directory + directory_length, suffix, sizeof(suffix));
    char *candidate = cbm_wide_to_utf8(system_directory);
    if (!candidate) {
        return false;
    }
    bool resolved = cbm_canonical_path(candidate, out, CBM_SZ_4K) != 0;
    free(candidate);
    return resolved;
}
#endif

static cbm_operation_command_cause_t operation_pre_spawn_cause(
    const cbm_operation_runtime_t *runtime, const char *output_path, size_t output_limit,
    uint64_t deadline_ms, bool deadline_enabled, bool *cancellation_latched,
    bool *deadline_latched, bool *output_limit_latched) {
    *cancellation_latched =
        *cancellation_latched || cbm_operation_runtime_cancelled(runtime);
    *deadline_latched =
        *deadline_latched || (deadline_enabled && cbm_now_ms() >= deadline_ms);
    if (!*output_limit_latched && output_limit > 0 && output_path && output_path[0]) {
        int64_t output_size = cbm_file_size(output_path);
        *output_limit_latched = output_size > 0 && (uint64_t)output_size > output_limit;
    }
    if (*cancellation_latched) {
        return CBM_OPERATION_COMMAND_CANCELLED;
    }
    if (*deadline_latched) {
        return CBM_OPERATION_COMMAND_DEADLINE;
    }
    if (*output_limit_latched) {
        return CBM_OPERATION_COMMAND_OUTPUT_LIMIT;
    }
    return CBM_OPERATION_COMMAND_SUCCESS;
}

cbm_operation_command_cause_t cbm_operation_run_shell_command_bounded(
    const cbm_operation_runtime_t *runtime, const char *command,
    char output_path[CBM_SZ_2K], size_t output_limit, uint64_t deadline_ms,
    bool deadline_enabled, bool deadline_latched, bool exit_one_is_no_match,
    cbm_proc_result_t *result_out) {
    if (!command || !output_path || !result_out) {
        return CBM_OPERATION_COMMAND_FAILURE;
    }
    memset(result_out, 0, sizeof(*result_out));
    bool cancellation_latched = false;
    bool output_limit_latched = false;
    cbm_operation_command_cause_t cause = operation_pre_spawn_cause(
        runtime, NULL, output_limit, deadline_ms, deadline_enabled, &cancellation_latched,
        &deadline_latched, &output_limit_latched);
    if (cause != CBM_OPERATION_COMMAND_SUCCESS) {
        result_out->tree_quiesced = true;
        return cause;
    }
    if (!operation_command_output_path(output_path)) {
        cause = operation_pre_spawn_cause(runtime, NULL, output_limit, deadline_ms,
                                          deadline_enabled, &cancellation_latched,
                                          &deadline_latched, &output_limit_latched);
        result_out->tree_quiesced = true;
        return cause != CBM_OPERATION_COMMAND_SUCCESS ? cause : CBM_OPERATION_COMMAND_FAILURE;
    }

    bool command_rejected = runtime && runtime->command_allowed &&
                            !runtime->command_allowed(runtime->command_allowed_context, command);
    cause = operation_pre_spawn_cause(runtime, output_path, output_limit, deadline_ms,
                                      deadline_enabled, &cancellation_latched, &deadline_latched,
                                      &output_limit_latched);
    if (cause != CBM_OPERATION_COMMAND_SUCCESS || command_rejected) {
        result_out->tree_quiesced = true;
        return cause != CBM_OPERATION_COMMAND_SUCCESS ? cause : CBM_OPERATION_COMMAND_FAILURE;
    }

#ifdef _WIN32
    char shell[CBM_SZ_4K];
    if (!operation_resolve_windows_cmd(shell)) {
        cause = operation_pre_spawn_cause(runtime, output_path, output_limit, deadline_ms,
                                          deadline_enabled, &cancellation_latched,
                                          &deadline_latched, &output_limit_latched);
        (void)cbm_unlink(output_path);
        output_path[0] = '\0';
        result_out->tree_quiesced = true;
        return cause != CBM_OPERATION_COMMAND_SUCCESS ? cause : CBM_OPERATION_COMMAND_FAILURE;
    }
#else
    const char *shell = "/bin/sh";
    const char *argv[] = {"sh", "-c", command, NULL};
#endif

    cbm_proc_opts_t options = {
        .bin = shell,
#ifdef _WIN32
        .windows_cmd_payload = command,
#else
        .argv = argv,
#endif
        .log_file = output_path,
        .quiet_timeout_ms = 0,
        .cancel_grace_ms = CBM_SUBPROCESS_DEFAULT_CANCEL_GRACE_MS,
        .delete_log_on_exit = false,
    };

    cause = operation_pre_spawn_cause(runtime, output_path, output_limit, deadline_ms,
                                      deadline_enabled, &cancellation_latched, &deadline_latched,
                                      &output_limit_latched);
    if (cause != CBM_OPERATION_COMMAND_SUCCESS) {
        result_out->tree_quiesced = true;
        return cause;
    }

    cbm_subprocess_t *process = NULL;
    if (cbm_subprocess_spawn(&options, &process) != 0) {
        cause = operation_pre_spawn_cause(runtime, output_path, output_limit, deadline_ms,
                                          deadline_enabled, &cancellation_latched,
                                          &deadline_latched, &output_limit_latched);
        (void)cbm_unlink(output_path);
        output_path[0] = '\0';
        result_out->tree_quiesced = true;
        return cause != CBM_OPERATION_COMMAND_SUCCESS ? cause : CBM_OPERATION_COMMAND_FAILURE;
    }

    cbm_proc_poll_t state;
    for (;;) {
        if (cbm_operation_runtime_cancelled(runtime)) {
            cancellation_latched = true;
            (void)cbm_subprocess_request_cancel(process);
        }
        if (deadline_enabled && cbm_now_ms() >= deadline_ms) {
            deadline_latched = true;
            (void)cbm_subprocess_request_cancel(process);
        }
        if (!output_limit_latched && output_limit > 0) {
            int64_t output_size = cbm_file_size(output_path);
            if (output_size > 0 && (uint64_t)output_size > output_limit) {
                output_limit_latched = true;
                (void)cbm_subprocess_request_cancel(process);
            }
        }
        state = cbm_subprocess_poll(process, result_out);
        cancellation_latched =
            cancellation_latched || cbm_operation_runtime_cancelled(runtime);
        deadline_latched =
            deadline_latched || (deadline_enabled && cbm_now_ms() >= deadline_ms);
        if (!output_limit_latched && output_limit > 0) {
            int64_t output_size = cbm_file_size(output_path);
            output_limit_latched = output_size > 0 && (uint64_t)output_size > output_limit;
        }
        if (state != CBM_PROC_POLL_RUNNING) {
            break;
        }
        cbm_usleep(OPERATION_COMMAND_POLL_US);
    }

    cbm_subprocess_destroy(process);
    if (state == CBM_PROC_POLL_ERROR || result_out->supervision_failed ||
        !result_out->tree_quiesced) {
        return CBM_OPERATION_COMMAND_SUPERVISION_FAILURE;
    }
    if (cancellation_latched || result_out->cancellation_requested) {
        return CBM_OPERATION_COMMAND_CANCELLED;
    }
    if (deadline_latched) {
        return CBM_OPERATION_COMMAND_DEADLINE;
    }
    if (output_limit_latched) {
        return CBM_OPERATION_COMMAND_OUTPUT_LIMIT;
    }
    if (exit_one_is_no_match && result_out->outcome == CBM_PROC_EXIT_NONZERO &&
        result_out->exit_code == 1) {
        return CBM_OPERATION_COMMAND_SUCCESS;
    }
    return result_out->outcome == CBM_PROC_CLEAN ? CBM_OPERATION_COMMAND_SUCCESS
                                                  : CBM_OPERATION_COMMAND_CONTAINED_FAILURE;
}

int cbm_operation_run_shell_command(const cbm_operation_runtime_t *runtime,
                                    const char *command,
                                    char output_path[CBM_SZ_2K],
                                    cbm_proc_result_t *result_out) {
    cbm_operation_command_cause_t cause = cbm_operation_run_shell_command_bounded(
        runtime, command, output_path, 0, 0, false, false, false, result_out);
    return cause == CBM_OPERATION_COMMAND_FAILURE ||
                   cause == CBM_OPERATION_COMMAND_SUPERVISION_FAILURE
               ? -1
               : 0;
}
