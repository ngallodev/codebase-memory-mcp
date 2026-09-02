#ifndef CBM_OPERATIONS_OPERATION_H
#define CBM_OPERATIONS_OPERATION_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum cbm_operation_id {
    CBM_OPERATION_INVALID = 0,
    CBM_OPERATION_PROJECTS,
    CBM_OPERATION_STATUS,
    CBM_OPERATION_COVERAGE,
    CBM_OPERATION_SEARCH,
    CBM_OPERATION_SNIPPET,
    CBM_OPERATION_TRACE,
    CBM_OPERATION_SCHEMA,
    CBM_OPERATION_QUERY,
    CBM_OPERATION_ARCHITECTURE,
    CBM_OPERATION_CHANGES,
    CBM_OPERATION_SOURCE_SEARCH,
    CBM_OPERATION_FILE_OUTLINE,
} cbm_operation_id_t;

typedef struct cbm_operation_descriptor {
    cbm_operation_id_t id;
    const char *name;
    const char *legacy_name;
    const char *description;
    bool requires_project;
    bool read_only;
} cbm_operation_descriptor_t;

typedef struct cbm_operation_result {
    char *payload;
    bool is_error;
} cbm_operation_result_t;

typedef cbm_operation_result_t (*cbm_operation_backend_fn)(void *context,
                                                            cbm_operation_id_t operation,
                                                            const char *args_json);

typedef bool (*cbm_operation_cancelled_fn)(void *context);
typedef bool (*cbm_operation_command_allowed_fn)(void *context, const char *command);

typedef struct cbm_operation_runtime {
    cbm_operation_cancelled_fn cancelled;
    void *cancelled_context;
    cbm_operation_command_allowed_fn command_allowed;
    void *command_allowed_context;

    /* Optional bounded-execution overrides. These are primarily useful for
     * deterministic integration/evaluation seams, but live at the neutral
     * operation boundary so read operations never depend on MCP server state. */
    size_t command_output_limit_override;
    const char *command_override;
    uint64_t command_timeout_override_ms;
    bool command_timeout_override_set;
} cbm_operation_runtime_t;

typedef struct cbm_operation_context {
    void *backend_context;
    cbm_operation_backend_fn backend;
    const cbm_operation_runtime_t *runtime;
} cbm_operation_context_t;

const cbm_operation_descriptor_t *cbm_operation_descriptor(cbm_operation_id_t id);
const cbm_operation_descriptor_t *cbm_operation_find(const char *name);
size_t cbm_operation_count(void);
const cbm_operation_descriptor_t *cbm_operation_at(size_t index);

cbm_operation_result_t cbm_operation_result_take(char *payload, bool is_error);
cbm_operation_result_t cbm_operation_result_copy(const char *payload, bool is_error);
void cbm_operation_result_dispose(cbm_operation_result_t *result);

/* Execute one application operation through the configured backend. The
 * operation layer deliberately knows nothing about MCP, daemon framing, CLI
 * rendering, hook transports, or the legacy tool result envelope. */
cbm_operation_result_t cbm_operation_execute(cbm_operation_context_t *context,
                                              cbm_operation_id_t operation,
                                              const char *args_json);

#ifdef __cplusplus
}
#endif

#endif
