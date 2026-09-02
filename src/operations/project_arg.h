#ifndef CBM_OPERATIONS_PROJECT_ARG_H
#define CBM_OPERATIONS_PROJECT_ARG_H

#ifdef __cplusplus
extern "C" {
#endif

/* Resolve the canonical project argument accepted by read/admin operations.
 * Accepts project/project_name/project_id/projectName, canonicalizes path-shaped
 * values to the persisted project key, and preserves the historical unique
 * cache-filename tail fallback. Caller owns the returned string. */
char *cbm_operation_project_arg(const char *args_json);

#ifdef __cplusplus
}
#endif

#endif
