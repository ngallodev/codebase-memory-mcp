#ifndef CBM_OPERATIONS_OPERATION_H
#define CBM_OPERATIONS_OPERATION_H

#include <stdbool.h>
#include <stddef.h>

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

typedef struct cbm_operation_context {
    void *backend_context;
    cbm_operation_backend_fn backend;
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
