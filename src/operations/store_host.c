#include "operations/store_host.h"

#include "foundation/compat.h"
#include "foundation/compat_fs.h"
#include "foundation/constants.h"
#include "foundation/log.h"
#include "foundation/mem.h"
#include "foundation/platform.h"
#include "foundation/str_util.h"
#include "foundation/workspace.h"

#include <errno.h>
#include <stdatomic.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#ifdef _WIN32
#include "foundation/win_utf8.h"
#include <io.h>
#include <process.h>
#include <windows.h>
#define getpid _getpid
#else
#include <fcntl.h>
#include <unistd.h>
#endif

static char *host_strdup(const char *s) {
    if (!s) return NULL;
    size_t n = strlen(s) + 1;
    char *out = malloc(n);
    if (out) memcpy(out, s, n);
    return out;
}

struct cbm_store_host {
    cbm_store_t *store;
    bool owns_store;
    char *current_project;
    time_t store_last_used;
    cbm_store_host_mutation_begin_fn mutation_begin;
    cbm_store_host_mutation_try_begin_fn mutation_try_begin;
    cbm_store_host_mutation_end_fn mutation_end;
    void *mutation_context;
    cbm_store_host_quarantine_step_fn quarantine_step;
    void *quarantine_context;
};

static const char *cache_dir(char *buf, size_t bufsz) {
    const char *dir = cbm_resolve_cache_dir();
    if (!dir) dir = cbm_tmpdir();
    snprintf(buf, bufsz, "%s", dir);
    return buf;
}

static const char *project_db_path(const char *project, char *buf, size_t bufsz) {
    if (!cbm_validate_project_name(project)) {
        buf[0] = '\0';
        return buf;
    }
    char dir[CBM_SZ_1K];
    cache_dir(dir, sizeof(dir));
    snprintf(buf, bufsz, "%s/%s.db", dir, project);
    return buf;
}

bool cbm_store_host_is_project_db_file(const char *name, size_t len) {
    if (!name || len < 4 || strcmp(name + len - 3, ".db") != 0) return false;
    if (name[0] == '_' || strncmp(name, ":memory:", 8) == 0) return false;
    return true;
}

bool cbm_store_host_db_internal_project_name(const char *full_path, char *name_out,
                                             size_t name_sz, cbm_store_t **out_store) {
    if (out_store) *out_store = NULL;
    cbm_store_t *st = cbm_store_open_path_query(full_path);
    if (!st) return false;
    cbm_project_t *projs = NULL;
    int n = 0;
    bool ok = false;
    if (cbm_store_list_projects(st, &projs, &n) == CBM_STORE_OK) {
        int primary = -1;
        int primary_count = 0;
        for (int i = 0; i < n; i++) {
            if (projs[i].name && projs[i].name[0] && !strstr(projs[i].name, "::")) {
                primary = i;
                primary_count++;
            }
        }
        if (primary_count == 1) {
            snprintf(name_out, name_sz, "%s", projs[primary].name);
            ok = true;
        }
    }
    cbm_store_free_projects(projs, n);
    if (ok && out_store) *out_store = st;
    else cbm_store_close(st);
    return ok;
}

static cbm_store_t *resolve_store_fallback_scan(const char *project) {
    char dir_path[CBM_SZ_1K];
    cache_dir(dir_path, sizeof(dir_path));
    cbm_dir_t *d = cbm_opendir(dir_path);
    if (!d) return NULL;
    cbm_store_t *found = NULL;
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(d)) != NULL) {
        const char *n = entry->name;
        size_t len = strlen(n);
        if (!cbm_store_host_is_project_db_file(n, len)) continue;
        char full_path[CBM_SZ_2K];
        snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, n);
        char iname[CBM_SZ_1K];
        cbm_store_t *st = NULL;
        if (cbm_store_host_db_internal_project_name(full_path, iname, sizeof(iname), &st)) {
            if (strcmp(iname, project) == 0) {
                found = st;
                break;
            }
            cbm_store_close(st);
        }
    }
    cbm_closedir(d);
    return found;
}

static bool reserve_unique_corrupt_pending(const char *path, char *pending, size_t pending_size,
                                           char *backup, size_t backup_size) {
    static atomic_uint_fast64_t sequence = 0;
    for (unsigned int attempt = 0; attempt < 128; attempt++) {
        uint64_t token = cbm_now_ns() ^ ((uint64_t)(unsigned int)getpid() << 32) ^
                         atomic_fetch_add_explicit(&sequence, 1, memory_order_relaxed);
        int bw = snprintf(backup, backup_size, "%s.corrupt.%016llx", path,
                          (unsigned long long)token);
        int pw = snprintf(pending, pending_size, "%s.corrupt.pending.%016llx", path,
                          (unsigned long long)token);
        if (bw <= 0 || (size_t)bw >= backup_size || pw <= 0 || (size_t)pw >= pending_size) return false;
        if (cbm_file_exists(backup)) continue;
#ifdef _WIN32
        wchar_t *wide = cbm_path_to_wide(pending);
        HANDLE file = wide ? CreateFileW(wide, GENERIC_READ | GENERIC_WRITE, 0, NULL, CREATE_NEW,
                                         FILE_ATTRIBUTE_NORMAL, NULL)
                           : INVALID_HANDLE_VALUE;
        DWORD err = file == INVALID_HANDLE_VALUE ? GetLastError() : ERROR_SUCCESS;
        free(wide);
        if (file != INVALID_HANDLE_VALUE) { CloseHandle(file); return true; }
        if (err != ERROR_FILE_EXISTS && err != ERROR_ALREADY_EXISTS) return false;
#else
        int fd = open(pending, O_WRONLY | O_CREAT | O_EXCL, 0600);
        if (fd >= 0) { (void)close(fd); return true; }
        if (errno != EEXIST) return false;
#endif
    }
    return false;
}

static void discard_corrupt_pending(const char *pending) {
    if (!pending) return;
    (void)cbm_remove_db_sidecars(pending);
    (void)cbm_unlink(pending);
}

#ifndef _WIN32
static bool sync_parent_directory(const char *path) {
    char directory[CBM_SZ_2K];
    int written = snprintf(directory, sizeof(directory), "%s", path ? path : "");
    if (written <= 0 || (size_t)written >= sizeof(directory)) return false;
    char *slash = strrchr(directory, '/');
    if (!slash) snprintf(directory, sizeof(directory), ".");
    else if (slash == directory) slash[1] = '\0';
    else *slash = '\0';
    int fd = open(directory, O_RDONLY);
    if (fd < 0) return false;
    int rc;
    do { rc = fsync(fd); } while (rc != 0 && errno == EINTR);
    (void)close(fd);
    return rc == 0;
}
#endif

static bool publish_corrupt_backup(const char *pending, const char *backup) {
#ifdef _WIN32
    wchar_t *wp = cbm_path_to_wide(pending);
    wchar_t *wb = cbm_path_to_wide(backup);
    bool ok = wp && wb && MoveFileExW(wp, wb, MOVEFILE_WRITE_THROUGH) != 0;
    free(wp); free(wb); return ok;
#else
    if (link(pending, backup) != 0) return false;
    if (!sync_parent_directory(backup)) { (void)cbm_unlink(backup); return false; }
    (void)cbm_unlink(pending);
    (void)sync_parent_directory(backup);
    return true;
#endif
}

static bool quarantine_step_allowed(cbm_store_host_t *host, const char *step) {
    return !host || !host->quarantine_step || host->quarantine_step(host->quarantine_context, step);
}

static bool quarantine_corrupt_store(cbm_store_host_t *host, const char *project,
                                     const char *path, char *backup_out,
                                     size_t backup_out_size) {
    char backup[CBM_SZ_2K];
    char pending[CBM_SZ_2K];
    if (!path || !path[0]) {
        cbm_log_error("store.auto_clean_failed", "project", project, "path", "", "reason",
                      "empty store path");
        return false;
    }
    if (!reserve_unique_corrupt_pending(path, pending, sizeof(pending), backup, sizeof(backup))) {
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "cannot reserve unique backup");
        return false;
    }

    if (cbm_store_backup_path(path, pending) != CBM_STORE_OK ||
        cbm_store_prepare_path_for_replace(pending) != CBM_STORE_OK) {
        discard_corrupt_pending(pending);
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "cannot create self-contained recovery snapshot");
        return false;
    }

    cbm_store_t *snapshot = cbm_store_open_path_query(pending);
    if (!snapshot) {
        discard_corrupt_pending(pending);
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "recovery snapshot cannot be reopened");
        return false;
    }
    cbm_store_close(snapshot);

    if (!quarantine_step_allowed(host, "before_snapshot_publish") ||
        !publish_corrupt_backup(pending, backup)) {
        discard_corrupt_pending(pending);
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "cannot atomically publish recovery snapshot");
        return false;
    }
    discard_corrupt_pending(pending);

    if (!quarantine_step_allowed(host, "after_snapshot_publish")) {
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "backup complete; live generation retained", "backup", backup);
        return false;
    }

    if (cbm_unlink(path) != 0 && errno != ENOENT) {
        cbm_log_error("store.auto_clean_failed", "project", project, "path", path, "reason",
                      "backup complete; live database removal failed", "backup", backup);
        return false;
    }
    if (cbm_remove_db_sidecars(path) != 0) {
        cbm_log_error("store.auto_clean_sidecars", "project", project, "path", path, "reason",
                      "backup complete; stale sidecar cleanup deferred");
    }

    if (backup_out && backup_out_size > 0) {
        snprintf(backup_out, backup_out_size, "%s", backup);
    }
    return true;
}

static bool mutation_begin(cbm_store_host_t *host, const char *project, bool nonblocking) {
    if (!host->mutation_begin) return true;
    if (nonblocking) return host->mutation_try_begin && host->mutation_try_begin(host->mutation_context, project);
    return host->mutation_begin(host->mutation_context, project);
}

static void mutation_end(cbm_store_host_t *host, const char *project) {
    if (host->mutation_end) host->mutation_end(host->mutation_context, project);
}

static cbm_store_host_t *host_alloc(void) {
    cbm_store_host_t *host = calloc(1, sizeof(*host));
    if (host) host->owns_store = true;
    return host;
}

cbm_store_host_t *cbm_store_host_new(const char *store_path) {
    cbm_store_host_t *host = host_alloc();
    if (!host) return NULL;
    if (store_path) {
        host->store = cbm_store_open(store_path);
        host->current_project = host_strdup(store_path);
    } else host->store = cbm_store_open_memory();
    return host;
}

cbm_store_host_t *cbm_store_host_new_deferred(void) { return host_alloc(); }

void cbm_store_host_free(cbm_store_host_t *host) {
    if (!host) return;
    if (host->owns_store && host->store) cbm_store_close(host->store);
    free(host->current_project);
    free(host);
}

cbm_store_t *cbm_store_host_store(cbm_store_host_t *host) { return host ? host->store : NULL; }
const char *cbm_store_host_current_project(const cbm_store_host_t *host) { return host ? host->current_project : NULL; }

void cbm_store_host_set_project(cbm_store_host_t *host, const char *project) {
    if (!host) return;
    free(host->current_project);
    host->current_project = project ? host_strdup(project) : NULL;
}

void cbm_store_host_detach_project(cbm_store_host_t *host, const char *project) {
    if (!host || !project || !project[0]) return;
    if (host->current_project && strcmp(host->current_project, project) == 0) cbm_store_host_invalidate(host);
}

void cbm_store_host_evict_idle(cbm_store_host_t *host, int timeout_s) {
    if (!host || !host->store || host->store_last_used == 0) return;
    if ((time(NULL) - host->store_last_used) < timeout_s) return;
    cbm_store_host_invalidate(host);
    host->store_last_used = 0;
}

bool cbm_store_host_has_cached_store(cbm_store_host_t *host) { return host && host->store; }

bool cbm_store_host_release_pristine_memory_store(cbm_store_host_t *host) {
    const char *db_path = host && host->store ? cbm_store_db_path(host->store) : NULL;
    if (!host || !host->owns_store || !host->store || host->current_project || host->store_last_used != 0 || db_path) return false;
    cbm_store_close(host->store); host->store = NULL; return true;
}

void cbm_store_host_set_mutation_guard(cbm_store_host_t *host,
                                       cbm_store_host_mutation_begin_fn begin,
                                       cbm_store_host_mutation_try_begin_fn try_begin,
                                       cbm_store_host_mutation_end_fn end,
                                       void *context) {
    if (!host || ((begin == NULL) != (end == NULL))) return;
    host->mutation_begin = begin;
    host->mutation_try_begin = begin ? try_begin : NULL;
    host->mutation_end = end;
    host->mutation_context = begin ? context : NULL;
}

void cbm_store_host_set_quarantine_step_hook(cbm_store_host_t *host,
                                             cbm_store_host_quarantine_step_fn hook,
                                             void *context) {
    if (!host) return;
    host->quarantine_step = hook;
    host->quarantine_context = hook ? context : NULL;
}

void cbm_store_host_invalidate(cbm_store_host_t *host) {
    if (!host) return;
    if (host->owns_store && host->store) cbm_store_close(host->store);
    host->store = NULL;
    free(host->current_project);
    host->current_project = NULL;
}

cbm_store_t *cbm_store_host_resolve(cbm_store_host_t *host, const char *project,
                                    bool mutation_already_held, bool nonblocking_recovery,
                                    cbm_operation_store_recovery_status_t *recovery_status) {
    if (recovery_status) *recovery_status = CBM_OPERATION_STORE_RECOVERY_NONE;
    if (!host || !project) return NULL;
    host->store_last_used = time(NULL);
    if (host->current_project && strcmp(host->current_project, project) == 0 && host->store) return host->store;
    cbm_store_host_invalidate(host);
    char path[CBM_SZ_1K];
    project_db_path(project, path, sizeof(path));
    host->store = path[0] ? cbm_store_open_path_query(path) : NULL;
    if (host->store && !cbm_store_check_integrity(host->store)) {
        cbm_store_close(host->store); host->store = NULL;
        bool acquired = mutation_already_held || mutation_begin(host, project, nonblocking_recovery);
        if (!acquired) {
            if (recovery_status) *recovery_status = host->mutation_try_begin ? CBM_OPERATION_STORE_RECOVERY_BUSY
                                                                            : CBM_OPERATION_STORE_RECOVERY_TRY_GUARD_UNAVAILABLE;
            return NULL;
        }
        host->store = cbm_store_open_path_query(path);
        cbm_integrity_verdict_t verdict = host->store ? cbm_store_check_integrity_verdict(host->store)
                                                       : CBM_INTEGRITY_TRANSIENT;
        if (verdict == CBM_INTEGRITY_TRANSIENT) {
            if (host->store) cbm_store_close(host->store);
            host->store = NULL;
            if (recovery_status) *recovery_status = CBM_OPERATION_STORE_RECOVERY_BUSY;
            if (!mutation_already_held) mutation_end(host, project);
            return NULL;
        }
        if (verdict != CBM_INTEGRITY_OK) {
            if (host->store) cbm_store_close(host->store);
            host->store = NULL;
            char backup[CBM_SZ_2K] = {0};
            bool q = quarantine_corrupt_store(host, project, path, backup, sizeof(backup));
            cbm_log_error("store.auto_clean", "project", project, "path", path, "action",
                          q ? "corrupt generation quarantined" : "corrupt generation preserved",
                          "backup", q ? backup : "none");
        }
        if (!mutation_already_held) mutation_end(host, project);
        if (!host->store) return NULL;
    }
    if (host->store) {
        cbm_project_t proj = {0};
        if (cbm_store_get_project(host->store, project, &proj) == CBM_STORE_OK) {
            cbm_project_free_fields(&proj);
            free(host->current_project);
            host->current_project = host_strdup(project);
            return host->store;
        }
        cbm_store_close(host->store); host->store = NULL;
    }
    cbm_store_t *scanned = resolve_store_fallback_scan(project);
    if (scanned) {
        host->store = scanned;
        free(host->current_project);
        host->current_project = host_strdup(project);
    }
    return host->store;
}

static int collect_db_project_names(char *out, size_t out_sz) {
    char dir_path[CBM_SZ_1K];
    cache_dir(dir_path, sizeof(dir_path));
    cbm_dir_t *d = cbm_opendir(dir_path);
    if (!d) return 0;
    int count = 0;
    size_t offset = 0;
    cbm_dirent_t *entry;
    while ((entry = cbm_readdir(d)) != NULL) {
        const char *n = entry->name;
        size_t len = strlen(n);
        if (!cbm_store_host_is_project_db_file(n, len)) continue;
        char full_path[CBM_SZ_2K];
        char iname[CBM_SZ_1K];
        snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, n);
        if (!cbm_store_host_db_internal_project_name(full_path, iname, sizeof(iname), NULL)) continue;
        size_t need = strlen(iname) + 2 + (count ? 1u : 0u);
        if (offset + need + 1 > out_sz) break;
        if (count) out[offset++] = ',';
        int wrote = snprintf(out + offset, out_sz - offset, "\"%s\"", iname);
        if (wrote <= 0) break;
        offset += (size_t)wrote;
        count++;
    }
    cbm_closedir(d);
    return count;
}

char *cbm_store_host_error(const char *project) {
    if (!project) {
        return host_strdup("{\"error\":\"missing required argument: project\",\"hint\":\"Pass the project as the \\\"project\\\" argument, e.g. {\\\"project\\\":\\\"<name from list_projects>\\\"}. Run list_projects to see indexed projects.\"}");
    }
    char projects[CBM_SZ_4K] = "";
    int count = collect_db_project_names(projects, sizeof(projects));
    enum { ERR_BUF_SZ = 5120 };
    char buf[ERR_BUF_SZ];
    if (count > 0) {
        snprintf(buf, sizeof(buf),
                 "{\"error\":\"project not found or not indexed\",\"hint\":\"Use list_projects to see all indexed projects, then pass it as the \\\"project\\\" argument.\",\"available_projects\":[%s],\"count\":%d}",
                 projects, count);
    } else {
        snprintf(buf, sizeof(buf),
                 "{\"error\":\"project not found or not indexed\",\"hint\":\"No projects indexed yet. Call index_repository first.\"}");
    }
    return host_strdup(buf);
}
