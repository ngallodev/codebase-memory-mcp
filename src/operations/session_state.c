#include "operations/session_state.h"

#include "foundation/compat_thread.h"
#include "pipeline/pipeline.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

struct cbm_operation_session_state {
    char *session_root;
    char *session_project;
    char *allowed_root;
    bool allowed_root_policy_set;
    cbm_mutex_t request_scope_mutex;
    unsigned int request_scope_depth;
    atomic_int cancel_requested;
};

static char *session_state_strdup(const char *value) {
    if (!value) return NULL;
    size_t length = strlen(value);
    char *copy = malloc(length + 1);
    if (!copy) return NULL;
    memcpy(copy, value, length + 1);
    return copy;
}

cbm_operation_session_state_t *cbm_operation_session_state_new(void) {
    cbm_operation_session_state_t *state = calloc(1, sizeof(*state));
    if (!state) return NULL;
    cbm_mutex_init(&state->request_scope_mutex);
    atomic_init(&state->cancel_requested, 0);
    return state;
}

void cbm_operation_session_state_clear_context(cbm_operation_session_state_t *state) {
    if (!state) return;
    free(state->session_root);
    free(state->session_project);
    free(state->allowed_root);
    state->session_root = NULL;
    state->session_project = NULL;
    state->allowed_root = NULL;
    state->allowed_root_policy_set = false;
}

void cbm_operation_session_state_free(cbm_operation_session_state_t *state) {
    if (!state) return;
    cbm_operation_session_state_clear_context(state);
    cbm_mutex_destroy(&state->request_scope_mutex);
    free(state);
}

bool cbm_operation_session_state_set_context(cbm_operation_session_state_t *state,
                                             const char *session_root,
                                             const char *allowed_root,
                                             bool allowed_root_policy_set) {
    if (!state || !session_root || !session_root[0]) return false;

    char *root_copy = session_state_strdup(session_root);
    char *project = cbm_project_name_from_path(session_root);
    char *allowed_copy = allowed_root ? session_state_strdup(allowed_root) : NULL;
    if (!root_copy || !project || !project[0] || (allowed_root && !allowed_copy)) {
        free(root_copy);
        free(project);
        free(allowed_copy);
        return false;
    }

    free(state->session_root);
    free(state->session_project);
    free(state->allowed_root);
    state->session_root = root_copy;
    state->session_project = project;
    state->allowed_root = allowed_copy;
    state->allowed_root_policy_set = allowed_root_policy_set;
    return true;
}

const char *cbm_operation_session_root(const cbm_operation_session_state_t *state) {
    return state ? state->session_root : NULL;
}

const char *cbm_operation_session_project(const cbm_operation_session_state_t *state) {
    return state ? state->session_project : NULL;
}

const char *cbm_operation_session_allowed_root(const cbm_operation_session_state_t *state) {
    return state ? state->allowed_root : NULL;
}

bool cbm_operation_session_allowed_root_policy_set(const cbm_operation_session_state_t *state) {
    return state && state->allowed_root_policy_set;
}

bool cbm_operation_session_request_scope_begin(cbm_operation_session_state_t *state) {
    if (!state) return false;
    cbm_mutex_lock(&state->request_scope_mutex);
    bool available = state->request_scope_depth < UINT_MAX;
    if (available) {
        if (state->request_scope_depth == 0) {
            atomic_store_explicit(&state->cancel_requested, 0, memory_order_release);
        }
        state->request_scope_depth++;
    }
    cbm_mutex_unlock(&state->request_scope_mutex);
    return available;
}

void cbm_operation_session_request_scope_end(cbm_operation_session_state_t *state) {
    if (!state) return;
    cbm_mutex_lock(&state->request_scope_mutex);
    if (state->request_scope_depth > 0) {
        state->request_scope_depth--;
        if (state->request_scope_depth == 0) {
            atomic_store_explicit(&state->cancel_requested, 0, memory_order_release);
        }
    }
    cbm_mutex_unlock(&state->request_scope_mutex);
}

bool cbm_operation_session_cancel_active(cbm_operation_session_state_t *state) {
    if (!state) return false;
    cbm_mutex_lock(&state->request_scope_mutex);
    bool active = state->request_scope_depth != 0;
    if (active) {
        atomic_store_explicit(&state->cancel_requested, 1, memory_order_release);
    }
    cbm_mutex_unlock(&state->request_scope_mutex);
    return active;
}

bool cbm_operation_session_cancelled(const cbm_operation_session_state_t *state) {
    return state && atomic_load_explicit(&state->cancel_requested, memory_order_acquire) != 0;
}

atomic_int *cbm_operation_session_cancel_flag(cbm_operation_session_state_t *state) {
    return state ? &state->cancel_requested : NULL;
}
