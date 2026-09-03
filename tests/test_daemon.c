/*
 * test_daemon.c — Guards for shared MCP-daemon coordination and framing.
 */
#include "test_framework.h"

#include "daemon/daemon.h"
#include "operations/session_state.h"

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

typedef struct {
    size_t job_cancel_count;
    size_t shared_job_cancel_count;
    size_t lease_job_cancel_count;
    size_t shutdown_job_cancel_count;
    size_t watch_release_count;
    size_t explicit_watch_release_count;
    size_t lease_watch_release_count;
    size_t shutdown_watch_release_count;
    size_t unexpected_key_count;
} daemon_release_tracker_t;

static void record_job_cancel(const char *project_key, void *context) {
    daemon_release_tracker_t *tracker = context;
    tracker->job_cancel_count++;
    if (strcmp(project_key, "project-shared") == 0) {
        tracker->shared_job_cancel_count++;
    } else if (strcmp(project_key, "project-lease-job") == 0) {
        tracker->lease_job_cancel_count++;
    } else if (strcmp(project_key, "project-shutdown-job") == 0) {
        tracker->shutdown_job_cancel_count++;
    } else {
        tracker->unexpected_key_count++;
    }
}

static void record_watch_release(const char *project_key, void *context) {
    daemon_release_tracker_t *tracker = context;
    tracker->watch_release_count++;
    if (strcmp(project_key, "project-explicit-watch") == 0) {
        tracker->explicit_watch_release_count++;
    } else if (strcmp(project_key, "project-lease-watch") == 0) {
        tracker->lease_watch_release_count++;
    } else if (strcmp(project_key, "project-shutdown-watch") == 0) {
        tracker->shutdown_watch_release_count++;
    } else {
        tracker->unexpected_key_count++;
    }
}

static bool install_release_tracker(cbm_daemon_coordinator_t *coordinator,
                                    daemon_release_tracker_t *tracker) {
    const cbm_daemon_coordinator_hooks_t hooks = {
        .cancel_job = record_job_cancel,
        .release_watch = record_watch_release,
        .context = tracker,
    };
    return cbm_daemon_coordinator_set_hooks(coordinator, &hooks);
}

TEST(daemon_client_ids_are_connection_bound) {
    cbm_daemon_coordinator_t *c = cbm_daemon_coordinator_new(250);
    ASSERT_NOT_NULL(c);

    cbm_daemon_client_id_t a = cbm_daemon_client_connected(c, 1000);
    cbm_daemon_client_id_t b = cbm_daemon_client_connected(c, 1001);
    ASSERT_NEQ(a, CBM_DAEMON_CLIENT_ID_INVALID);
    ASSERT_NEQ(b, CBM_DAEMON_CLIENT_ID_INVALID);
    ASSERT_NEQ(a, b);
    ASSERT_EQ(cbm_daemon_active_clients(c), 2);

    cbm_daemon_subscription_id_t rejected = UINT64_MAX;
    ASSERT_EQ(cbm_daemon_job_subscribe(c, UINT64_MAX, "project-a", &rejected),
              CBM_DAEMON_SUBSCRIPTION_REJECTED);
    ASSERT_EQ(rejected, CBM_DAEMON_SUBSCRIPTION_ID_INVALID);
    ASSERT_TRUE(cbm_daemon_client_disconnected(c, a, 1010));
    ASSERT_FALSE(cbm_daemon_client_disconnected(c, a, 1011));
    rejected = UINT64_MAX;
    ASSERT_EQ(cbm_daemon_job_subscribe(c, a, "project-a", &rejected),
              CBM_DAEMON_SUBSCRIPTION_REJECTED);
    ASSERT_EQ(rejected, CBM_DAEMON_SUBSCRIPTION_ID_INVALID);

    cbm_daemon_client_id_t c_id = cbm_daemon_client_connected(c, 1012);
    ASSERT_NEQ(c_id, CBM_DAEMON_CLIENT_ID_INVALID);
    ASSERT_NEQ(c_id, a); /* closed connection IDs are never recycled */
    ASSERT_NEQ(c_id, b);
    ASSERT_EQ(cbm_daemon_active_clients(c), 2);

    cbm_daemon_coordinator_free(c);
    PASS();
}

TEST(daemon_shared_job_survives_until_final_subscriber_disconnects) {
    cbm_daemon_coordinator_t *c = cbm_daemon_coordinator_new(250);
    daemon_release_tracker_t tracker = {0};
    ASSERT_NOT_NULL(c);
    ASSERT_TRUE(install_release_tracker(c, &tracker));

    cbm_daemon_client_id_t a = cbm_daemon_client_connected(c, 2000);
    cbm_daemon_client_id_t b = cbm_daemon_client_connected(c, 2000);
    cbm_daemon_client_id_t observer = cbm_daemon_client_connected(c, 2000);
    ASSERT_NEQ(a, CBM_DAEMON_CLIENT_ID_INVALID);
    ASSERT_NEQ(b, CBM_DAEMON_CLIENT_ID_INVALID);
    ASSERT_NEQ(observer, CBM_DAEMON_CLIENT_ID_INVALID);

    cbm_daemon_subscription_id_t a_first = CBM_DAEMON_SUBSCRIPTION_ID_INVALID;
    cbm_daemon_subscription_id_t a_second = CBM_DAEMON_SUBSCRIPTION_ID_INVALID;
    cbm_daemon_subscription_id_t b_subscription = CBM_DAEMON_SUBSCRIPTION_ID_INVALID;
    ASSERT_EQ(cbm_daemon_job_subscribe(c, a, "project-shared", &a_first),
              CBM_DAEMON_SUBSCRIPTION_STARTED);
    ASSERT_EQ(cbm_daemon_job_subscribe(c, a, "project-shared", &a_second),
              CBM_DAEMON_SUBSCRIPTION_JOINED);
    ASSERT_EQ(cbm_daemon_job_subscribe(c, b, "project-shared", &b_subscription),
              CBM_DAEMON_SUBSCRIPTION_JOINED);
    ASSERT_NEQ(a_first, CBM_DAEMON_SUBSCRIPTION_ID_INVALID);
    ASSERT_NEQ(a_second, CBM_DAEMON_SUBSCRIPTION_ID_INVALID);
    ASSERT_NEQ(b_subscription, CBM_DAEMON_SUBSCRIPTION_ID_INVALID);
    ASSERT_NEQ(a_first, a_second);
    ASSERT_NEQ(a_first, b_subscription);
    ASSERT_NEQ(a_second, b_subscription);
    ASSERT_EQ(cbm_daemon_active_jobs(c), 1);
    ASSERT_EQ(cbm_daemon_job_state(c, "project-shared"), CBM_DAEMON_JOB_RUNNING);
    ASSERT_EQ(cbm_daemon_job_subscribers(c, "project-shared"), 3);

    ASSERT_FALSE(cbm_daemon_job_unsubscribe(c, b, a_first));
    ASSERT_EQ(cbm_daemon_job_subscribers(c, "project-shared"), 3);
    ASSERT_TRUE(cbm_daemon_job_unsubscribe(c, a, a_first));
    ASSERT_FALSE(cbm_daemon_job_unsubscribe(c, a, a_first));
    ASSERT_EQ(cbm_daemon_job_subscribers(c, "project-shared"), 2);
    ASSERT_EQ(tracker.job_cancel_count, 0);

    ASSERT_TRUE(cbm_daemon_client_disconnected(c, a, 2001));
    ASSERT_EQ(cbm_daemon_active_jobs(c), 1);
    ASSERT_EQ(cbm_daemon_job_subscribers(c, "project-shared"), 1);
    ASSERT_EQ(tracker.job_cancel_count, 0);

    /* The final subscription requests cancellation even while the daemon
     * remains RUNNING because unrelated clients are still connected. */
    ASSERT_TRUE(cbm_daemon_job_unsubscribe(c, b, b_subscription));
    ASSERT_FALSE(cbm_daemon_job_unsubscribe(c, b, b_subscription));
    ASSERT_EQ(cbm_daemon_coordinator_state(c), CBM_DAEMON_COORDINATOR_RUNNING);
    ASSERT_EQ(cbm_daemon_active_jobs(c), 1);
    ASSERT_EQ(cbm_daemon_job_subscribers(c, "project-shared"), 0);
    ASSERT_EQ(cbm_daemon_job_state(c, "project-shared"), CBM_DAEMON_JOB_CANCEL_REQUESTED);
    ASSERT_EQ(tracker.job_cancel_count, 1);
    ASSERT_EQ(tracker.shared_job_cancel_count, 1);
    ASSERT_EQ(tracker.unexpected_key_count, 0);

    /* Cancellation is asynchronous: the job remains active until its worker
     * has actually been reaped, and duplicate owner cleanup cannot re-fire it. */
    ASSERT_TRUE(cbm_daemon_client_disconnected(c, b, 2002));
    ASSERT_EQ(tracker.job_cancel_count, 1);
    ASSERT_EQ(cbm_daemon_active_clients(c), 1);
    ASSERT_EQ(cbm_daemon_active_jobs(c), 1);
    ASSERT_TRUE(cbm_daemon_job_reaped(c, "project-shared", 2003));
    ASSERT_FALSE(cbm_daemon_job_reaped(c, "project-shared", 2004));
    ASSERT_EQ(cbm_daemon_active_jobs(c), 0);
    ASSERT_EQ(cbm_daemon_job_state(c, "project-shared"), CBM_DAEMON_JOB_NONE);

    cbm_daemon_coordinator_free(c);
    PASS();
}

TEST(daemon_watch_subscription_ids_are_connection_bound) {
    cbm_daemon_coordinator_t *c = cbm_daemon_coordinator_new(250);
    daemon_release_tracker_t tracker = {0};
    ASSERT_NOT_NULL(c);
    ASSERT_TRUE(install_release_tracker(c, &tracker));

    cbm_daemon_client_id_t a = cbm_daemon_client_connected(c, 3000);
    cbm_daemon_client_id_t b = cbm_daemon_client_connected(c, 3000);
    cbm_daemon_subscription_id_t a_watch = CBM_DAEMON_SUBSCRIPTION_ID_INVALID;
    cbm_daemon_subscription_id_t b_watch = CBM_DAEMON_SUBSCRIPTION_ID_INVALID;
    ASSERT_EQ(cbm_daemon_watch_subscribe(c, a, "project-explicit-watch", &a_watch),
              CBM_DAEMON_SUBSCRIPTION_STARTED);
    ASSERT_EQ(cbm_daemon_watch_subscribe(c, b, "project-explicit-watch", &b_watch),
              CBM_DAEMON_SUBSCRIPTION_JOINED);
    ASSERT_NEQ(a_watch, CBM_DAEMON_SUBSCRIPTION_ID_INVALID);
    ASSERT_NEQ(b_watch, CBM_DAEMON_SUBSCRIPTION_ID_INVALID);
    ASSERT_NEQ(a_watch, b_watch);
    ASSERT_EQ(cbm_daemon_watch_subscribers(c, "project-explicit-watch"), 2);

    ASSERT_FALSE(cbm_daemon_watch_unsubscribe(c, b, a_watch));
    ASSERT_TRUE(cbm_daemon_watch_unsubscribe(c, a, a_watch));
    ASSERT_FALSE(cbm_daemon_watch_unsubscribe(c, a, a_watch));
    ASSERT_EQ(cbm_daemon_active_watches(c), 1);
    ASSERT_EQ(cbm_daemon_watch_subscribers(c, "project-explicit-watch"), 1);
    ASSERT_EQ(tracker.watch_release_count, 0);

    ASSERT_TRUE(cbm_daemon_watch_unsubscribe(c, b, b_watch));
    ASSERT_FALSE(cbm_daemon_watch_unsubscribe(c, b, b_watch));
    ASSERT_EQ(cbm_daemon_active_watches(c), 0);
    ASSERT_EQ(cbm_daemon_watch_subscribers(c, "project-explicit-watch"), 0);
    ASSERT_EQ(tracker.watch_release_count, 1);
    ASSERT_EQ(tracker.explicit_watch_release_count, 1);
    ASSERT_EQ(tracker.unexpected_key_count, 0);

    cbm_daemon_coordinator_free(c);
    PASS();
}

TEST(daemon_heartbeat_extends_lease_and_expiry_releases_connection) {
    cbm_daemon_coordinator_t *c = cbm_daemon_coordinator_new(250);
    daemon_release_tracker_t tracker = {0};
    ASSERT_NOT_NULL(c);
    ASSERT_TRUE(install_release_tracker(c, &tracker));

    cbm_daemon_client_id_t owner = cbm_daemon_client_connected(c, 1000);
    cbm_daemon_client_id_t observer = cbm_daemon_client_connected(c, 1200);
    cbm_daemon_subscription_id_t job_subscription = CBM_DAEMON_SUBSCRIPTION_ID_INVALID;
    cbm_daemon_subscription_id_t watch_subscription = CBM_DAEMON_SUBSCRIPTION_ID_INVALID;
    ASSERT_EQ(cbm_daemon_job_subscribe(c, owner, "project-lease-job", &job_subscription),
              CBM_DAEMON_SUBSCRIPTION_STARTED);
    ASSERT_EQ(cbm_daemon_watch_subscribe(c, owner, "project-lease-watch", &watch_subscription),
              CBM_DAEMON_SUBSCRIPTION_STARTED);

    ASSERT_TRUE(cbm_daemon_client_heartbeat(c, owner, 1200));
    ASSERT_EQ(cbm_daemon_expire_leases(c, 1250), 0);
    ASSERT_EQ(cbm_daemon_active_clients(c), 2);
    ASSERT_TRUE(cbm_daemon_client_heartbeat(c, observer, 1449));
    ASSERT_EQ(cbm_daemon_expire_leases(c, 1449), 0);

    ASSERT_EQ(cbm_daemon_expire_leases(c, 1450), 1);
    ASSERT_EQ(cbm_daemon_active_clients(c), 1);
    ASSERT_EQ(cbm_daemon_coordinator_state(c), CBM_DAEMON_COORDINATOR_RUNNING);
    ASSERT_FALSE(cbm_daemon_client_heartbeat(c, owner, 1451));
    ASSERT_FALSE(cbm_daemon_client_disconnected(c, owner, 1451));
    ASSERT_EQ(cbm_daemon_job_subscribers(c, "project-lease-job"), 0);
    ASSERT_EQ(cbm_daemon_job_state(c, "project-lease-job"), CBM_DAEMON_JOB_CANCEL_REQUESTED);
    ASSERT_EQ(cbm_daemon_active_jobs(c), 1);
    ASSERT_EQ(cbm_daemon_active_watches(c), 0);
    ASSERT_EQ(tracker.job_cancel_count, 1);
    ASSERT_EQ(tracker.lease_job_cancel_count, 1);
    ASSERT_EQ(tracker.watch_release_count, 1);
    ASSERT_EQ(tracker.lease_watch_release_count, 1);
    ASSERT_EQ(tracker.unexpected_key_count, 0);
    ASSERT_TRUE(cbm_daemon_job_reaped(c, "project-lease-job", 1452));
    ASSERT_EQ(cbm_daemon_active_jobs(c), 0);

    cbm_daemon_coordinator_free(c);
    PASS();
}

TEST(daemon_last_client_stops_immediately_and_releases_owned_work) {
    cbm_daemon_coordinator_t *c = cbm_daemon_coordinator_new(250);
    daemon_release_tracker_t tracker = {0};
    ASSERT_NOT_NULL(c);
    ASSERT_TRUE(install_release_tracker(c, &tracker));

    cbm_daemon_client_id_t client = cbm_daemon_client_connected(c, 5000);
    cbm_daemon_subscription_id_t job_subscription = CBM_DAEMON_SUBSCRIPTION_ID_INVALID;
    cbm_daemon_subscription_id_t watch_subscription = CBM_DAEMON_SUBSCRIPTION_ID_INVALID;
    ASSERT_NEQ(client, CBM_DAEMON_CLIENT_ID_INVALID);
    ASSERT_EQ(cbm_daemon_coordinator_state(c), CBM_DAEMON_COORDINATOR_RUNNING);
    ASSERT_EQ(cbm_daemon_job_subscribe(c, client, "project-shutdown-job", &job_subscription),
              CBM_DAEMON_SUBSCRIPTION_STARTED);
    ASSERT_EQ(cbm_daemon_watch_subscribe(c, client, "project-shutdown-watch", &watch_subscription),
              CBM_DAEMON_SUBSCRIPTION_STARTED);
    ASSERT_NEQ(job_subscription, CBM_DAEMON_SUBSCRIPTION_ID_INVALID);
    ASSERT_NEQ(watch_subscription, CBM_DAEMON_SUBSCRIPTION_ID_INVALID);
    ASSERT_EQ(cbm_daemon_active_jobs(c), 1);
    ASSERT_EQ(cbm_daemon_active_watches(c), 1);
    ASSERT_EQ(cbm_daemon_watch_subscribers(c, "project-shutdown-watch"), 1);

    ASSERT_TRUE(cbm_daemon_client_disconnected(c, client, 5200));
    ASSERT_EQ(cbm_daemon_active_clients(c), 0);
    ASSERT_EQ(cbm_daemon_coordinator_state(c), CBM_DAEMON_COORDINATOR_STOPPING);
    ASSERT_EQ(cbm_daemon_active_jobs(c), 1);
    ASSERT_EQ(cbm_daemon_active_watches(c), 0);
    ASSERT_EQ(cbm_daemon_job_subscribers(c, "project-shutdown-job"), 0);
    ASSERT_EQ(cbm_daemon_job_state(c, "project-shutdown-job"), CBM_DAEMON_JOB_CANCEL_REQUESTED);
    ASSERT_EQ(cbm_daemon_watch_subscribers(c, "project-shutdown-watch"), 0);
    ASSERT_EQ(tracker.job_cancel_count, 1);
    ASSERT_EQ(tracker.shutdown_job_cancel_count, 1);
    ASSERT_EQ(tracker.watch_release_count, 1);
    ASSERT_EQ(tracker.shutdown_watch_release_count, 1);
    ASSERT_EQ(tracker.unexpected_key_count, 0);

    /* There is no post-client grace before STOPPING, but process exit waits
     * until the cancelled worker is confirmed reaped. */
    ASSERT_FALSE(cbm_daemon_should_exit(c, 5200));
    ASSERT_FALSE(cbm_daemon_should_exit(c, 10000));

    /* STOPPING is terminal for this daemon instance.  A late connection or a
     * stale connection ID cannot resurrect it or enqueue fresh work. */
    ASSERT_EQ(cbm_daemon_client_connected(c, 5201), CBM_DAEMON_CLIENT_ID_INVALID);
    cbm_daemon_subscription_id_t rejected = UINT64_MAX;
    ASSERT_EQ(cbm_daemon_job_subscribe(c, client, "project-too-late", &rejected),
              CBM_DAEMON_SUBSCRIPTION_REJECTED);
    ASSERT_EQ(rejected, CBM_DAEMON_SUBSCRIPTION_ID_INVALID);
    rejected = UINT64_MAX;
    ASSERT_EQ(cbm_daemon_watch_subscribe(c, client, "project-too-late", &rejected),
              CBM_DAEMON_SUBSCRIPTION_REJECTED);
    ASSERT_EQ(rejected, CBM_DAEMON_SUBSCRIPTION_ID_INVALID);
    ASSERT_FALSE(cbm_daemon_client_disconnected(c, client, 5201));
    ASSERT_EQ(tracker.job_cancel_count, 1);
    ASSERT_EQ(tracker.watch_release_count, 1);

    ASSERT_TRUE(cbm_daemon_job_reaped(c, "project-shutdown-job", 5202));
    ASSERT_EQ(cbm_daemon_active_jobs(c), 0);
    ASSERT_EQ(cbm_daemon_job_state(c, "project-shutdown-job"), CBM_DAEMON_JOB_NONE);
    ASSERT_TRUE(cbm_daemon_should_exit(c, 5202));

    cbm_daemon_coordinator_free(c);
    PASS();
}

TEST(daemon_completed_job_is_not_cancelled_on_later_disconnect) {
    cbm_daemon_coordinator_t *c = cbm_daemon_coordinator_new(250);
    daemon_release_tracker_t tracker = {0};
    ASSERT_NOT_NULL(c);
    ASSERT_TRUE(install_release_tracker(c, &tracker));

    cbm_daemon_client_id_t client = cbm_daemon_client_connected(c, 6000);
    cbm_daemon_subscription_id_t subscription = CBM_DAEMON_SUBSCRIPTION_ID_INVALID;
    ASSERT_EQ(cbm_daemon_job_subscribe(c, client, "project-normal", &subscription),
              CBM_DAEMON_SUBSCRIPTION_STARTED);
    ASSERT_NEQ(subscription, CBM_DAEMON_SUBSCRIPTION_ID_INVALID);
    ASSERT_EQ(cbm_daemon_job_state(c, "project-normal"), CBM_DAEMON_JOB_RUNNING);

    ASSERT_TRUE(cbm_daemon_job_completed(c, "project-normal", 6100));
    ASSERT_FALSE(cbm_daemon_job_completed(c, "project-normal", 6101));
    ASSERT_FALSE(cbm_daemon_job_reaped(c, "project-normal", 6101));
    ASSERT_EQ(cbm_daemon_active_jobs(c), 0);
    ASSERT_EQ(cbm_daemon_job_subscribers(c, "project-normal"), 0);
    ASSERT_EQ(cbm_daemon_job_state(c, "project-normal"), CBM_DAEMON_JOB_NONE);

    ASSERT_TRUE(cbm_daemon_client_disconnected(c, client, 6200));
    ASSERT_EQ(cbm_daemon_coordinator_state(c), CBM_DAEMON_COORDINATOR_STOPPING);
    ASSERT_EQ(tracker.job_cancel_count, 0);
    ASSERT_EQ(tracker.unexpected_key_count, 0);
    ASSERT_TRUE(cbm_daemon_should_exit(c, 6200));

    cbm_daemon_coordinator_free(c);
    PASS();
}

TEST(daemon_frame_header_rejects_wrong_protocol_and_oversize) {
    uint8_t header[CBM_DAEMON_FRAME_HEADER_SIZE];
    cbm_daemon_frame_t frame;

    ASSERT_TRUE(cbm_daemon_frame_header_encode(header, CBM_DAEMON_FRAME_REQUEST, 0, 1234));
    ASSERT_TRUE(cbm_daemon_frame_header_decode(header, &frame));
    ASSERT_EQ(frame.type, CBM_DAEMON_FRAME_REQUEST);
    ASSERT_EQ(frame.flags, 0);
    ASSERT_EQ(frame.length, 1234);

    header[4] = (uint8_t)(CBM_DAEMON_RENDEZVOUS_FRAME_VERSION + 1);
    ASSERT_FALSE(cbm_daemon_frame_header_decode(header, &frame));

    ASSERT_FALSE(cbm_daemon_frame_header_encode(header, CBM_DAEMON_FRAME_REQUEST, 0,
                                                CBM_DAEMON_MAX_FRAME_SIZE + 1));
    PASS();
}

TEST(daemon_sessions_keep_distinct_roots_and_allowed_root_policy) {
    cbm_operation_session_state_t *a = cbm_operation_session_state_new();
    cbm_operation_session_state_t *b = cbm_operation_session_state_new();
    ASSERT_NOT_NULL(a);
    ASSERT_NOT_NULL(b);
    ASSERT_TRUE(cbm_operation_session_state_set_context(a, "/tmp/cbm-session-a", "/tmp", true));
    ASSERT_TRUE(cbm_operation_session_state_set_context(b, "/tmp/cbm-session-b", NULL, false));
    ASSERT_STR_EQ(cbm_operation_session_root(a), "/tmp/cbm-session-a");
    ASSERT_STR_EQ(cbm_operation_session_allowed_root(a), "/tmp");
    ASSERT_STR_EQ(cbm_operation_session_root(b), "/tmp/cbm-session-b");
    ASSERT_NULL(cbm_operation_session_allowed_root(b));
    ASSERT_STR_NEQ(cbm_operation_session_project(a), cbm_operation_session_project(b));
    cbm_operation_session_state_free(a);
    cbm_operation_session_state_free(b);
    PASS();
}

SUITE(daemon) {
    RUN_TEST(daemon_client_ids_are_connection_bound);
    RUN_TEST(daemon_shared_job_survives_until_final_subscriber_disconnects);
    RUN_TEST(daemon_watch_subscription_ids_are_connection_bound);
    RUN_TEST(daemon_heartbeat_extends_lease_and_expiry_releases_connection);
    RUN_TEST(daemon_last_client_stops_immediately_and_releases_owned_work);
    RUN_TEST(daemon_completed_job_is_not_cancelled_on_later_disconnect);
    RUN_TEST(daemon_frame_header_rejects_wrong_protocol_and_oversize);
    RUN_TEST(daemon_sessions_keep_distinct_roots_and_allowed_root_policy);
}
