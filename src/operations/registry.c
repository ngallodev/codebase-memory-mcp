#include "operations/operation.h"

#include <string.h>

static const cbm_operation_descriptor_t k_operations[] = {
    {CBM_OPERATION_PROJECTS, "projects", "list_projects", "List indexed projects", false, true},
    {CBM_OPERATION_STATUS, "status", "index_status", "Show index status", true, true},
    {CBM_OPERATION_COVERAGE, "coverage", "check_index_coverage", "Check index coverage", true,
     true},
    {CBM_OPERATION_SEARCH, "search", "search_graph", "Search indexed graph", true, true},
    {CBM_OPERATION_SNIPPET, "snippet", "get_code_snippet", "Read an indexed source snippet",
     true, true},
    {CBM_OPERATION_TRACE, "trace", "trace_path", "Trace indexed relationships", true, true},
};

const cbm_operation_descriptor_t *cbm_operation_descriptor(cbm_operation_id_t id) {
    for (size_t i = 0; i < sizeof(k_operations) / sizeof(k_operations[0]); ++i) {
        if (k_operations[i].id == id) {
            return &k_operations[i];
        }
    }
    return NULL;
}

const cbm_operation_descriptor_t *cbm_operation_find(const char *name) {
    if (!name) {
        return NULL;
    }
    for (size_t i = 0; i < sizeof(k_operations) / sizeof(k_operations[0]); ++i) {
        if (strcmp(k_operations[i].name, name) == 0 || strcmp(k_operations[i].legacy_name, name) == 0) {
            return &k_operations[i];
        }
    }
    return NULL;
}

size_t cbm_operation_count(void) {
    return sizeof(k_operations) / sizeof(k_operations[0]);
}

const cbm_operation_descriptor_t *cbm_operation_at(size_t index) {
    if (index >= cbm_operation_count()) {
        return NULL;
    }
    return &k_operations[index];
}
