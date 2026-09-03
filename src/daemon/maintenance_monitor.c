/*
 * maintenance_monitor.c — Neutral maintenance observer for local CBM processes.
 */
#include "daemon/maintenance_monitor.h"

#include "foundation/compat.h"
#include "foundation/compat_thread.h"
#include "foundation/platform.h"
#include "foundation/subprocess.h"

#include <stdint.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <limits.h>

enum {
    MAINTENANCE_POLL_MS = 10,
    MAINTENANCE_IDLE_POLL_MS = 250,
    MAINTENANCE_GRACE_MS =
        CBM_SUBPROCESS_MAX_CANCEL_GRACE_MS + CBM_SUBPROCESS_FORCE_SETTLE_MS + 1000,
    MAINTENANCE_PARTICIPANT_NAME_CAP = 64,
};

struct cbm_daemon_maintenance_monitor {
    cbm_thread_t thread;
    cbm_version_cohort_manager_t *manager;
    cbm_daemon_maintenance_cancel_fn cancel;
    void *cancel_context;
    atomic_bool stopping;
    int exit_code;
    char participant[MAINTENANCE_PARTICIPANT_NAME_CAP];
};

static void *maintenance_monitor_worker(void *opaque) {
    cbm_daemon_maintenance_monitor_t *monitor = opaque;
    while (!atomic_load_explicit(&monitor->stopping, memory_order_acquire)) {
        cbm_version_cohort_maintenance_presence_t presence =
            cbm_version_cohort_maintenance_presence_terminal(monitor->manager);
        if (presence == CBM_VERSION_COHORT_MAINTENANCE_ABSENT) {
            cbm_usleep(MAINTENANCE_IDLE_POLL_MS * 1000U);
            continue;
        }
        if (presence == CBM_VERSION_COHORT_MAINTENANCE_REQUESTED) {
            if (monitor->cancel) {
                (void)monitor->cancel(monitor->cancel_context);
            }
            uint64_t now = cbm_now_ms();
            uint64_t deadline = now > UINT64_MAX - MAINTENANCE_GRACE_MS
                                    ? UINT64_MAX
                                    : now + MAINTENANCE_GRACE_MS;
            while (!atomic_load_explicit(&monitor->stopping, memory_order_acquire) &&
                   cbm_now_ms() < deadline) {
                cbm_usleep(MAINTENANCE_POLL_MS * 1000U);
            }
            if (atomic_load_explicit(&monitor->stopping, memory_order_acquire)) {
                return NULL;
            }
            _Exit(monitor->exit_code);
        }
        /* Absence could not be proven. Fail closed so native teardown releases
         * SQLite/cohort ownership before the binary mutation window proceeds. */
        _Exit(EXIT_FAILURE);
    }
    return NULL;
}

cbm_daemon_maintenance_monitor_t *cbm_daemon_maintenance_monitor_start(
    cbm_version_cohort_manager_t *manager, cbm_daemon_maintenance_cancel_fn cancel,
    void *cancel_context, int exit_code, const char *participant) {
    if (!manager || exit_code < 0 || !participant || !participant[0]) {
        return NULL;
    }
    cbm_daemon_maintenance_monitor_t *monitor = calloc(1, sizeof(*monitor));
    if (!monitor) {
        return NULL;
    }
    monitor->manager = manager;
    monitor->cancel = cancel;
    monitor->cancel_context = cancel_context;
    monitor->exit_code = exit_code;
    atomic_init(&monitor->stopping, false);
    int written = snprintf(monitor->participant, sizeof(monitor->participant), "%s", participant);
    if (written <= 0 || written >= (int)sizeof(monitor->participant) ||
        cbm_thread_create(&monitor->thread, 0, maintenance_monitor_worker, monitor) != 0) {
        free(monitor);
        return NULL;
    }
    return monitor;
}

bool cbm_daemon_maintenance_monitor_stop(cbm_daemon_maintenance_monitor_t **monitor_io) {
    if (!monitor_io || !*monitor_io) {
        return false;
    }
    cbm_daemon_maintenance_monitor_t *monitor = *monitor_io;
    atomic_store_explicit(&monitor->stopping, true, memory_order_release);
    if (cbm_thread_join(&monitor->thread) != 0) {
        return false;
    }
    free(monitor);
    *monitor_io = NULL;
    return true;
}
