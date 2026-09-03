#include "operations/index_admission.h"

#include "discover/discover.h"
#include "foundation/platform.h"

#include <limits.h>

bool cbm_auto_index_within_file_limit(const char *root_path, int file_limit,
                                      int *file_count_out) {
    if (file_count_out) *file_count_out = -1;
    if (!root_path || !root_path[0] || file_limit < 0) return false;
    enum { AUTO_INDEX_COUNT_TIMEOUT_MS = 5000 };
    cbm_discover_opts_t options = {
        .mode = CBM_MODE_FULL,
        .ignore_file = NULL,
        .max_file_size = 0,
    };
    int count = -1;
    cbm_discover_status_t status = cbm_discover_count_bounded(
        root_path, &options, file_limit, cbm_now_ms() + AUTO_INDEX_COUNT_TIMEOUT_MS, &count);
    if (file_count_out) {
        *file_count_out = status == CBM_DISCOVER_LIMIT_EXCEEDED
                              ? (file_limit < INT_MAX ? file_limit + 1 : INT_MAX)
                              : count;
    }
    return status == CBM_DISCOVER_OK;
}
