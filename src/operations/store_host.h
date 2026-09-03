#ifndef CBM_OPERATIONS_STORE_HOST_H
#define CBM_OPERATIONS_STORE_HOST_H

#include <stdbool.h>
#include <stddef.h>
#include "operations/operation.h"
#include "store/store.h"

typedef struct cbm_store_host cbm_store_host_t;
typedef bool (*cbm_store_host_mutation_begin_fn)(void *context, const char *project);
typedef bool (*cbm_store_host_mutation_try_begin_fn)(void *context, const char *project);
typedef void (*cbm_store_host_mutation_end_fn)(void *context, const char *project);
typedef bool (*cbm_store_host_quarantine_step_fn)(void *context, const char *step);

cbm_store_host_t *cbm_store_host_new(const char *store_path);
cbm_store_host_t *cbm_store_host_new_deferred(void);
void cbm_store_host_free(cbm_store_host_t *host);

cbm_store_t *cbm_store_host_store(cbm_store_host_t *host);
const char *cbm_store_host_current_project(const cbm_store_host_t *host);
void cbm_store_host_set_project(cbm_store_host_t *host, const char *project);
void cbm_store_host_detach_project(cbm_store_host_t *host, const char *project);
void cbm_store_host_evict_idle(cbm_store_host_t *host, int timeout_s);
bool cbm_store_host_has_cached_store(cbm_store_host_t *host);
bool cbm_store_host_release_pristine_memory_store(cbm_store_host_t *host);

void cbm_store_host_set_mutation_guard(cbm_store_host_t *host,
                                       cbm_store_host_mutation_begin_fn begin,
                                       cbm_store_host_mutation_try_begin_fn try_begin,
                                       cbm_store_host_mutation_end_fn end,
                                       void *context);
void cbm_store_host_set_quarantine_step_hook(cbm_store_host_t *host,
                                             cbm_store_host_quarantine_step_fn hook,
                                             void *context);

cbm_store_t *cbm_store_host_resolve(cbm_store_host_t *host, const char *project,
                                    bool mutation_already_held, bool nonblocking_recovery,
                                    cbm_operation_store_recovery_status_t *recovery_status);
void cbm_store_host_invalidate(cbm_store_host_t *host);
char *cbm_store_host_error(const char *project);

bool cbm_store_host_db_internal_project_name(const char *full_path, char *name_out,
                                             size_t name_sz, cbm_store_t **out_store);
bool cbm_store_host_is_project_db_file(const char *name, size_t len);

#endif
