#include "test_operation_host.h"

#include "operations/session_state.h"
#include "operations/store_host.h"

#include <stdlib.h>

struct cbm_test_operation_host {
    cbm_store_host_t *stores;
    cbm_operation_session_state_t *session;
    cbm_operation_runtime_t runtime;
    cbm_operation_context_t context;
};

static bool test_mutation_begin(void *context, const char *project) {
    (void)context;
    (void)project;
    return true;
}

static void test_mutation_end(void *context, const char *project) {
    (void)context;
    (void)project;
}

static cbm_store_t *test_store_resolve(void *context, const char *project,
                                       bool mutation_already_held, bool nonblocking_recovery,
                                       cbm_operation_store_recovery_status_t *status) {
    return cbm_store_host_resolve((cbm_store_host_t *)context, project, mutation_already_held,
                                  nonblocking_recovery, status);
}

static void test_store_invalidate(void *context) {
    cbm_store_host_invalidate((cbm_store_host_t *)context);
}

static char *test_store_error(void *context, const char *project) {
    (void)context;
    return cbm_store_host_error(project);
}

static bool test_cancelled(void *context) {
    return cbm_operation_session_cancelled((cbm_operation_session_state_t *)context);
}

cbm_test_operation_host_t *cbm_test_operation_host_new(const char *store_path) {
    cbm_test_operation_host_t *host = calloc(1, sizeof(*host));
    if (!host) return NULL;
    host->stores = store_path ? cbm_store_host_new(store_path) : cbm_store_host_new_deferred();
    host->session = cbm_operation_session_state_new();
    if (!host->stores || !host->session) {
        cbm_test_operation_host_free(host);
        return NULL;
    }
    host->runtime.mutation_begin = test_mutation_begin;
    host->runtime.mutation_try_begin = test_mutation_begin;
    host->runtime.mutation_end = test_mutation_end;
    host->runtime.store_resolve = test_store_resolve;
    host->runtime.store_invalidate = test_store_invalidate;
    host->runtime.store_error = test_store_error;
    host->runtime.store_context = host->stores;
    host->runtime.cancelled = test_cancelled;
    host->runtime.cancelled_context = host->session;
    host->runtime.cancel_flag = cbm_operation_session_cancel_flag(host->session);
    host->context.runtime = &host->runtime;
    return host;
}

void cbm_test_operation_host_free(cbm_test_operation_host_t *host) {
    if (!host) return;
    cbm_store_host_free(host->stores);
    cbm_operation_session_state_free(host->session);
    free(host);
}

char *cbm_test_operation_execute(cbm_test_operation_host_t *host, const char *operation,
                                 const char *args_json) {
    if (!host || !operation) return NULL;
    const cbm_operation_descriptor_t *descriptor = cbm_operation_find(operation);
    if (!descriptor) return NULL;
    cbm_operation_result_t result = cbm_operation_execute(&host->context, descriptor->id,
                                                          args_json ? args_json : "{}");
    char *payload = result.payload;
    result.payload = NULL;
    cbm_operation_result_dispose(&result);
    return payload;
}

cbm_store_t *cbm_test_operation_host_store(cbm_test_operation_host_t *host) {
    return host ? cbm_store_host_store(host->stores) : NULL;
}

void cbm_test_operation_host_set_project(cbm_test_operation_host_t *host, const char *project) {
    if (host) cbm_store_host_set_project(host->stores, project);
}

const char *cbm_test_operation_host_session_root(const cbm_test_operation_host_t *host) {
    return host ? cbm_operation_session_root(host->session) : NULL;
}

bool cbm_test_operation_host_set_session_context(cbm_test_operation_host_t *host,
                                                 const char *session_root,
                                                 const char *allowed_root,
                                                 bool allowed_root_policy_set) {
    if (!host || !cbm_operation_session_state_set_context(host->session, session_root, allowed_root,
                                                          allowed_root_policy_set)) return false;
    host->runtime.session_root = cbm_operation_session_root(host->session);
    host->runtime.allowed_root = cbm_operation_session_allowed_root(host->session);
    host->runtime.allowed_root_policy_set =
        cbm_operation_session_allowed_root_policy_set(host->session);
    return true;
}

void cbm_test_operation_host_evict_idle(cbm_test_operation_host_t *host, int timeout_s) {
    if (host) cbm_store_host_evict_idle(host->stores, timeout_s);
}
