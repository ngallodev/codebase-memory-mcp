#include "operations/reliability_events.h"

#include "foundation/compat_fs.h"
#include "foundation/platform.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
#include <process.h>
#define getpid _getpid
#else
#include <sys/stat.h>
#include <unistd.h>
#endif

enum {
    RELIABILITY_DIR_MODE = 0700,
    RELIABILITY_FILE_CAP = 512 * 1024,
    RELIABILITY_DEFAULT_SCAN_FILES = 64,
    RELIABILITY_RETENTION_FILES = 96,
    RELIABILITY_RETENTION_SECONDS = 7 * 24 * 60 * 60,
    RELIABILITY_LINE_CAP = 2048,
    RELIABILITY_PATH_CAP = 4096,
};

static const char *const k_event_names[CBM_RELIABILITY_EVENT_COUNT] = {
    "mutation.requested",       "mutation.coalesced",       "mutation.conflict",
    "mutation.lock_wait",       "mutation.lock_timeout",    "sqlite.busy",
    "sqlite.locked",            "store.integrity.ok",       "store.integrity.transient",
    "store.integrity.corrupt",  "store.quarantine",         "store.rebuild.requested",
    "store.rebuild.completed",  "store.wal.starving",       "store.checkpoint",
    "index.worker.crash",       "index.publish",
};

static char g_process_log_path[RELIABILITY_PATH_CAP];
static bool g_process_log_initialized = false;
static bool g_process_log_disabled = false;

const char *cbm_reliability_event_name(cbm_reliability_event_t event) {
    if (event < 0 || event >= CBM_RELIABILITY_EVENT_COUNT) {
        return NULL;
    }
    return k_event_names[event];
}

static bool reliability_dir_path(const char *cache_dir, char *out, size_t out_size) {
    if (!cache_dir || !cache_dir[0] || !out || out_size == 0) {
        return false;
    }
    int written = snprintf(out, out_size, "%s/reliability-events", cache_dir);
    return written > 0 && (size_t)written < out_size;
}

static void json_string(FILE *sink, const char *value) {
    (void)fputc('"', sink);
    const unsigned char *cursor = (const unsigned char *)(value ? value : "");
    while (*cursor) {
        switch (*cursor) {
        case '"': (void)fputs("\\\"", sink); break;
        case '\\': (void)fputs("\\\\", sink); break;
        case '\n': (void)fputs("\\n", sink); break;
        case '\r': (void)fputs("\\r", sink); break;
        case '\t': (void)fputs("\\t", sink); break;
        default:
            if (*cursor < 0x20) {
                (void)fprintf(sink, "\\u%04x", (unsigned int)*cursor);
            } else {
                (void)fputc((int)*cursor, sink);
            }
            break;
        }
        cursor++;
    }
    (void)fputc('"', sink);
}

static bool reliability_file_name(const char *name) {
    size_t length = name ? strlen(name) : 0;
    const char suffix[] = ".ndjson";
    return length > sizeof(suffix) && strncmp(name, "events-", 7) == 0 &&
           strcmp(name + length - (sizeof(suffix) - 1), suffix) == 0;
}

typedef struct reliability_file_info {
    char path[RELIABILITY_PATH_CAP];
    int64_t mtime_ns;
} reliability_file_info_t;

static int reliability_file_newest_first(const void *left, const void *right) {
    const reliability_file_info_t *a = left;
    const reliability_file_info_t *b = right;
    if (a->mtime_ns > b->mtime_ns) return -1;
    if (a->mtime_ns < b->mtime_ns) return 1;
    return strcmp(a->path, b->path);
}

static reliability_file_info_t *reliability_list_files(const char *directory, size_t *count_out) {
    *count_out = 0;
    cbm_dir_t *dir = cbm_opendir(directory);
    if (!dir) return NULL;
    reliability_file_info_t *files = NULL;
    size_t count = 0;
    size_t capacity = 0;
    cbm_dirent_t *entry = NULL;
    while ((entry = cbm_readdir(dir)) != NULL) {
        if (!reliability_file_name(entry->name)) continue;
        char path[RELIABILITY_PATH_CAP];
        int written = snprintf(path, sizeof(path), "%s/%s", directory, entry->name);
        if (written <= 0 || (size_t)written >= sizeof(path)) continue;
        cbm_path_info_t info;
        if (cbm_path_info_utf8(path, &info) != 0 || !info.is_regular || info.is_symlink) continue;
        if (count == capacity) {
            size_t next = capacity ? capacity * 2 : 16;
            reliability_file_info_t *grown = realloc(files, next * sizeof(*grown));
            if (!grown) {
                free(files);
                cbm_closedir(dir);
                return NULL;
            }
            files = grown;
            capacity = next;
        }
        (void)snprintf(files[count].path, sizeof(files[count].path), "%s", path);
        files[count].mtime_ns = info.mtime_ns;
        count++;
    }
    cbm_closedir(dir);
    if (count > 1) qsort(files, count, sizeof(*files), reliability_file_newest_first);
    *count_out = count;
    return files;
}

static void reliability_prune(const char *directory) {
    size_t count = 0;
    reliability_file_info_t *files = reliability_list_files(directory, &count);
    if (!files) return;
    int64_t now_ns = (int64_t)time(NULL) * INT64_C(1000000000);
    int64_t max_age_ns = (int64_t)RELIABILITY_RETENTION_SECONDS * INT64_C(1000000000);
    for (size_t i = 0; i < count; ++i) {
        bool over_count = i >= RELIABILITY_RETENTION_FILES;
        bool expired = files[i].mtime_ns > 0 && now_ns > files[i].mtime_ns &&
                       now_ns - files[i].mtime_ns > max_age_ns;
        if ((over_count || expired) && strcmp(files[i].path, g_process_log_path) != 0) {
            (void)cbm_unlink(files[i].path);
        }
    }
    free(files);
}

static bool reliability_process_log_init(void) {
    if (g_process_log_initialized) return !g_process_log_disabled;
    g_process_log_initialized = true;
    const char *cache_dir = cbm_resolve_cache_dir();
    char directory[RELIABILITY_PATH_CAP];
    if (!reliability_dir_path(cache_dir, directory, sizeof(directory)) ||
        !cbm_mkdir_p(directory, RELIABILITY_DIR_MODE)) {
        g_process_log_disabled = true;
        return false;
    }
#ifndef _WIN32
    (void)chmod(directory, RELIABILITY_DIR_MODE);
#endif
    long long epoch = (long long)time(NULL);
    int written = snprintf(g_process_log_path, sizeof(g_process_log_path),
                           "%s/events-%lld-%d.ndjson", directory, epoch, (int)getpid());
    if (written <= 0 || (size_t)written >= sizeof(g_process_log_path)) {
        g_process_log_disabled = true;
        return false;
    }
    reliability_prune(directory);
    return true;
}

void cbm_reliability_record(const cbm_reliability_record_t *record) {
    if (!record || !cbm_reliability_event_name(record->event) || !reliability_process_log_init()) {
        return;
    }
    int64_t size = cbm_file_size(g_process_log_path);
    if (size >= RELIABILITY_FILE_CAP) return;
    FILE *sink = cbm_fopen(g_process_log_path, "ab");
    if (!sink) return;
#ifndef _WIN32
    (void)chmod(g_process_log_path, 0600);
#endif
    (void)fprintf(sink, "{\"timestamp\":%lld,\"pid\":%d,\"event\":",
                  (long long)time(NULL), (int)getpid());
    json_string(sink, cbm_reliability_event_name(record->event));
    if (record->project && record->project[0]) {
        (void)fputs(",\"project\":", sink); json_string(sink, record->project);
    }
    if (record->operation && record->operation[0]) {
        (void)fputs(",\"operation\":", sink); json_string(sink, record->operation);
    }
    if (record->reason && record->reason[0]) {
        (void)fputs(",\"reason\":", sink); json_string(sink, record->reason);
    }
    if (record->sqlite_code != 0) {
        (void)fprintf(sink, ",\"sqlite_code\":%d", record->sqlite_code);
    }
    if (record->elapsed_ms != 0) {
        (void)fprintf(sink, ",\"elapsed_ms\":%" PRIu64, record->elapsed_ms);
    }
    if (record->retry) (void)fputs(",\"retry\":true", sink);
    (void)fputs("}\n", sink);
    (void)fclose(sink);
}

static int reliability_event_from_line(const char *line) {
    const char marker[] = "\"event\":\"";
    const char *start = strstr(line, marker);
    if (!start) return -1;
    start += sizeof(marker) - 1;
    for (int event = 0; event < CBM_RELIABILITY_EVENT_COUNT; ++event) {
        const char *name = k_event_names[event];
        size_t length = strlen(name);
        if (strncmp(start, name, length) == 0 && start[length] == '"') return event;
    }
    return -1;
}

bool cbm_reliability_read_summary(const char *cache_dir, size_t max_files,
                                  cbm_reliability_summary_t *summary_out) {
    if (!summary_out) return false;
    memset(summary_out, 0, sizeof(*summary_out));
    char directory[RELIABILITY_PATH_CAP];
    if (!reliability_dir_path(cache_dir, directory, sizeof(directory))) return true;
    size_t count = 0;
    reliability_file_info_t *files = reliability_list_files(directory, &count);
    if (!files) return true;
    size_t limit = max_files ? max_files : RELIABILITY_DEFAULT_SCAN_FILES;
    if (count > limit) summary_out->truncated = true;
    size_t scan = count < limit ? count : limit;
    char line[RELIABILITY_LINE_CAP];
    for (size_t i = 0; i < scan; ++i) {
        FILE *source = cbm_fopen(files[i].path, "rb");
        if (!source) continue;
        summary_out->files_scanned++;
        while (fgets(line, sizeof(line), source)) {
            size_t length = strlen(line);
            if (length == 0 || line[length - 1] != '\n') {
                summary_out->malformed_records++;
                int ch = 0;
                while ((ch = fgetc(source)) != '\n' && ch != EOF) {}
                continue;
            }
            int event = reliability_event_from_line(line);
            if (event < 0) {
                summary_out->malformed_records++;
                continue;
            }
            summary_out->counts[event]++;
            summary_out->records++;
        }
        (void)fclose(source);
    }
    free(files);
    return true;
}
