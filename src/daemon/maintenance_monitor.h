/*
 * maintenance_monitor.h — Neutral local-process maintenance observer.
 */
#ifndef CBM_DAEMON_MAINTENANCE_MONITOR_H
#define CBM_DAEMON_MAINTENANCE_MONITOR_H

#include "daemon/version_cohort.h"

#include <stdbool.h>

typedef struct cbm_daemon_maintenance_monitor cbm_daemon_maintenance_monitor_t;

/* Called once when install/update/uninstall requests an active local command
 * to stop cooperatively. Returning false does not authorize the command to
 * outlive the bounded grace period. */
typedef bool (*cbm_daemon_maintenance_cancel_fn)(void *context);

/* Start a temporary observer for a one-shot local CLI command or supervised
 * worker. manager and cancel_context are borrowed until stop. On maintenance
 * intent the observer invokes cancel once, permits the bounded worker-tree
 * containment grace, then fail-stops the process so OS/SQLite teardown releases
 * cohort and database ownership before binary mutation proceeds. */
cbm_daemon_maintenance_monitor_t *cbm_daemon_maintenance_monitor_start(
    cbm_version_cohort_manager_t *manager, cbm_daemon_maintenance_cancel_fn cancel,
    void *cancel_context, int exit_code, const char *participant);

/* Stop and join the observer before freeing any borrowed context. */
bool cbm_daemon_maintenance_monitor_stop(cbm_daemon_maintenance_monitor_t **monitor_io);

#endif /* CBM_DAEMON_MAINTENANCE_MONITOR_H */
