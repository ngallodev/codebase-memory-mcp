/*
 * foundation_main.c — Minimal runner for the fast foundation-only test target.
 *
 * Keep this runner intentionally independent from tests/test_main.c: the full
 * runner also doubles as daemon/index/UI subprocess probes and therefore pulls
 * in production subsystems that a foundation-only binary must not link.
 */
int tf_pass_count = 0;
int tf_fail_count = 0;
int tf_skip_count = 0;

#include "test_framework.h"

extern void suite_arena(void);
extern void suite_hash_table(void);
extern void suite_dyn_array(void);
extern void suite_str_intern(void);
extern void suite_log(void);
extern void suite_str_util(void);
extern void suite_workspace(void);
extern void suite_platform(void);
extern void suite_diagnostics(void);
extern void suite_dump_verify(void);
extern void suite_subprocess(void);
extern void suite_private_file_lock(void);
extern void suite_lock_registry(void);

int main(void) {
    printf("\n  codebase-memory-cli  foundation test suite\n");
    RUN_SUITE(arena);
    RUN_SUITE(hash_table);
    RUN_SUITE(dyn_array);
    RUN_SUITE(str_intern);
    RUN_SUITE(log);
    RUN_SUITE(str_util);
    RUN_SUITE(workspace);
    RUN_SUITE(platform);
    RUN_SUITE(diagnostics);
    RUN_SUITE(dump_verify);
    RUN_SUITE(subprocess);
    RUN_SUITE(private_file_lock);
    RUN_SUITE(lock_registry);
    TEST_SUMMARY();
}
