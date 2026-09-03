#ifndef CBM_OPERATIONS_SESSION_STATE_H
#define CBM_OPERATIONS_SESSION_STATE_H

#include <stdbool.h>
#include <stdatomic.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct cbm_operation_session_state cbm_operation_session_state_t;

cbm_operation_session_state_t *cbm_operation_session_state_new(void);
void cbm_operation_session_state_free(cbm_operation_session_state_t *state);

bool cbm_operation_session_state_set_context(cbm_operation_session_state_t *state,
                                             const char *session_root,
                                             const char *allowed_root,
                                             bool allowed_root_policy_set);
void cbm_operation_session_state_clear_context(cbm_operation_session_state_t *state);

const char *cbm_operation_session_root(const cbm_operation_session_state_t *state);
const char *cbm_operation_session_project(const cbm_operation_session_state_t *state);
const char *cbm_operation_session_allowed_root(const cbm_operation_session_state_t *state);
bool cbm_operation_session_allowed_root_policy_set(const cbm_operation_session_state_t *state);

bool cbm_operation_session_request_scope_begin(cbm_operation_session_state_t *state);
void cbm_operation_session_request_scope_end(cbm_operation_session_state_t *state);
bool cbm_operation_session_cancel_active(cbm_operation_session_state_t *state);
bool cbm_operation_session_cancelled(const cbm_operation_session_state_t *state);
atomic_int *cbm_operation_session_cancel_flag(cbm_operation_session_state_t *state);

#ifdef __cplusplus
}
#endif

#endif
