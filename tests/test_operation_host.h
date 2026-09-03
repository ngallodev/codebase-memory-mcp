#ifndef CBM_TEST_OPERATION_HOST_H
#define CBM_TEST_OPERATION_HOST_H

#include "operations/operation.h"
#include "store/store.h"

typedef struct cbm_test_operation_host cbm_test_operation_host_t;

cbm_test_operation_host_t *cbm_test_operation_host_new(const char *store_path);
void cbm_test_operation_host_free(cbm_test_operation_host_t *host);
char *cbm_test_operation_execute(cbm_test_operation_host_t *host, const char *operation,
                                 const char *args_json);
cbm_store_t *cbm_test_operation_host_store(cbm_test_operation_host_t *host);
void cbm_test_operation_host_set_project(cbm_test_operation_host_t *host, const char *project);
const char *cbm_test_operation_host_session_root(const cbm_test_operation_host_t *host);
bool cbm_test_operation_host_set_session_context(cbm_test_operation_host_t *host,
                                                 const char *session_root,
                                                 const char *allowed_root,
                                                 bool allowed_root_policy_set);
void cbm_test_operation_host_evict_idle(cbm_test_operation_host_t *host, int timeout_s);

#endif
