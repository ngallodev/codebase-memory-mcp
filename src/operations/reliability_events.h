#ifndef CBM_OPERATIONS_RELIABILITY_EVENTS_H
#define CBM_OPERATIONS_RELIABILITY_EVENTS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Stable machine-readable event vocabulary for runtime assurance. Event names
 * are public diagnostic identifiers: append deliberately; do not repurpose an
 * existing name for a different condition. */
typedef enum cbm_reliability_event {
    CBM_RELIABILITY_EVENT_MUTATION_REQUESTED = 0,
    CBM_RELIABILITY_EVENT_MUTATION_COALESCED,
    CBM_RELIABILITY_EVENT_MUTATION_CONFLICT,
    CBM_RELIABILITY_EVENT_MUTATION_LOCK_WAIT,
    CBM_RELIABILITY_EVENT_MUTATION_LOCK_TIMEOUT,
    CBM_RELIABILITY_EVENT_SQLITE_BUSY,
    CBM_RELIABILITY_EVENT_SQLITE_LOCKED,
    CBM_RELIABILITY_EVENT_STORE_INTEGRITY_OK,
    CBM_RELIABILITY_EVENT_STORE_INTEGRITY_TRANSIENT,
    CBM_RELIABILITY_EVENT_STORE_INTEGRITY_CORRUPT,
    CBM_RELIABILITY_EVENT_STORE_QUARANTINE,
    CBM_RELIABILITY_EVENT_STORE_REBUILD_REQUESTED,
    CBM_RELIABILITY_EVENT_STORE_REBUILD_COMPLETED,
    CBM_RELIABILITY_EVENT_STORE_WAL_STARVING,
    CBM_RELIABILITY_EVENT_STORE_CHECKPOINT,
    CBM_RELIABILITY_EVENT_INDEX_WORKER_CRASH,
    CBM_RELIABILITY_EVENT_INDEX_PUBLISH,
    CBM_RELIABILITY_EVENT_COUNT
} cbm_reliability_event_t;

typedef struct cbm_reliability_record {
    cbm_reliability_event_t event;
    const char *project;
    const char *operation;
    const char *reason;
    int sqlite_code;          /* 0 when not applicable. */
    uint64_t elapsed_ms;      /* 0 when not measured/applicable. */
    bool retry;
} cbm_reliability_record_t;

typedef struct cbm_reliability_summary {
    uint64_t counts[CBM_RELIABILITY_EVENT_COUNT];
    size_t files_scanned;
    size_t records;
    size_t malformed_records;
    bool truncated;
} cbm_reliability_summary_t;

const char *cbm_reliability_event_name(cbm_reliability_event_t event);

/* Best-effort persistent runtime assurance. Each process owns its own bounded
 * NDJSON file, which avoids cross-process append locking. Recording must never
 * fail the underlying operation. Source/query contents are never accepted by
 * this API; callers provide only stable metadata/reason fields. */
void cbm_reliability_record(const cbm_reliability_record_t *record);

/* Aggregate bounded recent history from the cache. This is read-only and does
 * not create the cache or diagnostic directory. max_files==0 uses the default
 * bounded scan limit. Returns false only when arguments are invalid; a missing
 * history directory is a valid empty summary. */
bool cbm_reliability_read_summary(const char *cache_dir, size_t max_files,
                                  cbm_reliability_summary_t *summary_out);

#endif
