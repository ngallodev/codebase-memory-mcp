/* test_legacy_agent_profiles.c — Historical profile byte contracts used by ownership-aware cleanup. */
#include "test_framework.h"

#include <cli/legacy_agent_profiles.h>
#include <yyjson/yyjson.h>

#include <stdlib.h>
#include <string.h>

static const cbm_graph_profile_dialect_t handoff_only_dialects[] = {
    CBM_GRAPH_DIALECT_AUGMENT,
    CBM_GRAPH_DIALECT_CURSOR,
    CBM_GRAPH_DIALECT_ROVO,
    CBM_GRAPH_DIALECT_POCHI,
};

static int profile_has_mutator(const char *profile) {
    static const char *const mutators[] = {
        "index_repository",
        "delete_project",
        "manage_adr",
        "ingest_traces",
    };
    for (size_t i = 0U; i < sizeof(mutators) / sizeof(mutators[0]); i++) {
        if (strstr(profile, mutators[i])) {
            return 1;
        }
    }
    return 0;
}

TEST(agent_profiles_stable_tier_identity) {
    ASSERT_STR_EQ(cbm_graph_tier_slug(CBM_GRAPH_TIER_SCOUT), "codebase-memory-scout");
    ASSERT_STR_EQ(cbm_graph_tier_slug(CBM_GRAPH_TIER_VERIFY), "codebase-memory");
    ASSERT_STR_EQ(cbm_graph_tier_slug(CBM_GRAPH_TIER_AUDIT), "codebase-memory-auditor");
    ASSERT_STR_EQ(cbm_graph_tier_display_name(CBM_GRAPH_TIER_SCOUT), "Codebase Memory Scout");
    ASSERT_STR_EQ(cbm_graph_tier_display_name(CBM_GRAPH_TIER_VERIFY), "Codebase Memory Verify");
    ASSERT_STR_EQ(cbm_graph_tier_display_name(CBM_GRAPH_TIER_AUDIT), "Codebase Memory Auditor");
    ASSERT_TRUE(cbm_graph_dialect_direct_capable(CBM_GRAPH_DIALECT_CLAUDE));
    ASSERT_TRUE(cbm_graph_dialect_direct_capable(CBM_GRAPH_DIALECT_KIRO));
    ASSERT_FALSE(cbm_graph_dialect_direct_capable(CBM_GRAPH_DIALECT_AUGMENT));
    ASSERT_FALSE(cbm_graph_dialect_direct_capable(CBM_GRAPH_DIALECT_CURSOR));
    ASSERT_FALSE(cbm_graph_dialect_direct_capable(CBM_GRAPH_DIALECT_ROVO));
    ASSERT_TRUE(cbm_graph_dialect_direct_capable(CBM_GRAPH_DIALECT_OMP));
    ASSERT_FALSE(cbm_graph_dialect_direct_capable(CBM_GRAPH_DIALECT_POCHI));
    ASSERT_FALSE(cbm_graph_dialect_direct_capable(CBM_GRAPH_DIALECT_COUNT));
    ASSERT_NULL(cbm_graph_tier_slug(CBM_GRAPH_TIER_COUNT));
    ASSERT_NULL(cbm_graph_tier_display_name(CBM_GRAPH_TIER_COUNT));
    PASS();
}

TEST(agent_profiles_tiers_encode_distinct_evidence_budgets) {
    char *scout = cbm_render_graph_profile(CBM_GRAPH_DIALECT_CLAUDE, CBM_GRAPH_TIER_SCOUT,
                                           CBM_GRAPH_ACCESS_DIRECT, NULL);
    char *verify = cbm_render_graph_profile(CBM_GRAPH_DIALECT_CLAUDE, CBM_GRAPH_TIER_VERIFY,
                                            CBM_GRAPH_ACCESS_DIRECT, NULL);
    char *audit = cbm_render_graph_profile(CBM_GRAPH_DIALECT_CLAUDE, CBM_GRAPH_TIER_AUDIT,
                                           CBM_GRAPH_ACCESS_DIRECT, NULL);
    int valid = scout && verify && audit && strstr(scout, "3-4 narrow graph calls") &&
                strstr(scout, "positive, provisional") && strstr(scout, "all/none claims") &&
                !strstr(scout, "mcp__codebase-memory-mcp__query_graph") &&
                !strstr(scout, "mcp__codebase-memory-mcp__detect_changes") &&
                strstr(verify, "default tier") && strstr(verify, "task-directed evidence") &&
                strstr(verify, "scope coverage before negative claims") &&
                strstr(verify, "mcp__codebase-memory-mcp__query_graph") &&
                strstr(verify, "mcp__codebase-memory-mcp__detect_changes") &&
                strstr(audit, "bounded scope") && strstr(audit, "current graph generation") &&
                strstr(audit, "complete relevant pagination") && strstr(audit, "scope coverage") &&
                strstr(audit, "source fallback") &&
                strstr(audit, "mcp__codebase-memory-mcp__query_graph") &&
                strstr(audit, "mcp__codebase-memory-mcp__detect_changes");
    free(scout);
    free(verify);
    free(audit);
    ASSERT_TRUE(valid);
    PASS();
}

TEST(agent_profiles_handoff_only_dialects_fail_closed_for_direct_access) {
    for (size_t i = 0U; i < sizeof(handoff_only_dialects) / sizeof(handoff_only_dialects[0]); i++) {
        char *profile = cbm_render_graph_profile(handoff_only_dialects[i], CBM_GRAPH_TIER_VERIFY,
                                                 CBM_GRAPH_ACCESS_DIRECT, "/opt/cbm");
        ASSERT_NULL(profile);
    }
    PASS();
}

TEST(agent_profiles_server_level_dialects_hard_enforce_read_only_tools) {
    char *junie_scout = cbm_render_graph_profile(CBM_GRAPH_DIALECT_JUNIE, CBM_GRAPH_TIER_SCOUT,
                                                 CBM_GRAPH_ACCESS_DIRECT, NULL);
    char *junie = cbm_render_graph_profile(CBM_GRAPH_DIALECT_JUNIE, CBM_GRAPH_TIER_VERIFY,
                                           CBM_GRAPH_ACCESS_DIRECT, NULL);
    char *qoder = cbm_render_graph_profile(CBM_GRAPH_DIALECT_QODER, CBM_GRAPH_TIER_VERIFY,
                                           CBM_GRAPH_ACCESS_DIRECT, NULL);
    char *factory = cbm_render_graph_profile(CBM_GRAPH_DIALECT_FACTORY, CBM_GRAPH_TIER_VERIFY,
                                             CBM_GRAPH_ACCESS_DIRECT, NULL);
    ASSERT_NOT_NULL(junie_scout);
    ASSERT_NOT_NULL(junie);
    ASSERT_NOT_NULL(qoder);
    ASSERT_NOT_NULL(factory);
    ASSERT(strstr(junie_scout, "mcpServers: [\"codebase-memory-scout\"]") != NULL);
    ASSERT(strstr(junie, "mcpServers: [\"codebase-memory-analysis\"]") != NULL);
    ASSERT(strstr(junie, "hard-enforces the analysis tool profile") != NULL);
    ASSERT(strstr(qoder, "mcp__codebase-memory-mcp__check_index_coverage") != NULL);
    ASSERT(strstr(factory, "mcp__codebase-memory-mcp__check_index_coverage") != NULL);
    ASSERT(strstr(qoder, "mcpServers:") != NULL);
    ASSERT(strstr(qoder, "codebase-memory-mcp") != NULL);
    ASSERT(strstr(factory, "mcpServers") == NULL);
    ASSERT(strstr(junie, "instruction-enforced") == NULL);
    ASSERT(strstr(qoder, "instruction-enforced") == NULL);
    ASSERT(strstr(factory, "instruction-enforced") == NULL);
    ASSERT(!profile_has_mutator(junie));
    ASSERT(!profile_has_mutator(qoder));
    ASSERT(!profile_has_mutator(factory));
    free(junie_scout);
    free(junie);
    free(qoder);
    free(factory);
    PASS();
}

TEST(agent_profiles_kiro_is_valid_json_and_escapes_binary_path) {
    const char *binary = "/opt/cbm path/\"quoted\"";
    char *profile = cbm_render_graph_profile(CBM_GRAPH_DIALECT_KIRO, CBM_GRAPH_TIER_AUDIT,
                                             CBM_GRAPH_ACCESS_DIRECT, binary);
    ASSERT_NOT_NULL(profile);
    yyjson_doc *doc = yyjson_read(profile, strlen(profile), 0);
    yyjson_val *root = doc ? yyjson_doc_get_root(doc) : NULL;
    yyjson_val *servers = root ? yyjson_obj_get(root, "mcpServers") : NULL;
    yyjson_val *server = servers ? yyjson_obj_get(servers, "codebase-memory-mcp") : NULL;
    yyjson_val *command = server ? yyjson_obj_get(server, "command") : NULL;
    yyjson_val *args = server ? yyjson_obj_get(server, "args") : NULL;
    yyjson_val *profile_flag = args && yyjson_is_arr(args) ? yyjson_arr_get(args, 0U) : NULL;
    yyjson_val *profile_name = args && yyjson_is_arr(args) ? yyjson_arr_get(args, 1U) : NULL;
    yyjson_val *tools = root ? yyjson_obj_get(root, "tools") : NULL;
    int valid = root && yyjson_is_obj(root) && command && yyjson_is_str(command) &&
                strcmp(yyjson_get_str(command), binary) == 0 && args && yyjson_is_arr(args) &&
                yyjson_arr_size(args) == 2U && profile_flag && yyjson_is_str(profile_flag) &&
                strcmp(yyjson_get_str(profile_flag), "--tool-profile") == 0 && profile_name &&
                yyjson_is_str(profile_name) &&
                strcmp(yyjson_get_str(profile_name), "analysis") == 0 && tools &&
                yyjson_is_arr(tools) &&
                strstr(profile, "@codebase-memory-mcp/check_index_coverage") != NULL;
    yyjson_doc_free(doc);
    free(profile);
    ASSERT_TRUE(valid);
    PASS();
}

TEST(agent_profiles_codex_declares_transport_and_escapes_binary_path) {
    const char *binary = "C:\\cbm bin\\codebase-memory-mcp.exe";
    char *scout = cbm_render_graph_profile(CBM_GRAPH_DIALECT_CODEX, CBM_GRAPH_TIER_SCOUT,
                                           CBM_GRAPH_ACCESS_DIRECT, binary);
    char *verify = cbm_render_graph_profile(CBM_GRAPH_DIALECT_CODEX, CBM_GRAPH_TIER_VERIFY,
                                            CBM_GRAPH_ACCESS_DIRECT, binary);
    ASSERT_NOT_NULL(scout);
    ASSERT_NOT_NULL(verify);
    int valid = strstr(scout, "[mcp_servers.codebase-memory-mcp]\n"
                              "command = \"C:\\\\cbm bin\\\\codebase-memory-mcp.exe\"\n"
                              "args = [\"--tool-profile=scout\"]\n"
                              "enabled_tools = [") != NULL &&
                strstr(verify, "command = \"C:\\\\cbm bin\\\\codebase-memory-mcp.exe\"\n"
                               "args = [\"--tool-profile=analysis\"]\n") != NULL;
    free(scout);
    free(verify);
    ASSERT_TRUE(valid);
    ASSERT_NULL(cbm_render_graph_profile(CBM_GRAPH_DIALECT_CODEX, CBM_GRAPH_TIER_VERIFY,
                                         CBM_GRAPH_ACCESS_DIRECT, NULL));
    ASSERT_NULL(cbm_render_graph_profile(CBM_GRAPH_DIALECT_CODEX, CBM_GRAPH_TIER_VERIFY,
                                         CBM_GRAPH_ACCESS_DIRECT, ""));
    char *handoff = cbm_render_graph_profile(CBM_GRAPH_DIALECT_CODEX, CBM_GRAPH_TIER_VERIFY,
                                             CBM_GRAPH_ACCESS_HANDOFF, NULL);
    ASSERT_NOT_NULL(handoff);
    ASSERT_TRUE(strstr(handoff, "[mcp_servers.") == NULL);
    free(handoff);
    char *rc1 = cbm_render_graph_profile_codex_rc1(CBM_GRAPH_TIER_VERIFY);
    ASSERT_NOT_NULL(rc1);
    ASSERT_TRUE(strstr(rc1, "[mcp_servers.codebase-memory-mcp]\nenabled_tools = [") != NULL);
    ASSERT_TRUE(strstr(rc1, "command = ") == NULL);
    free(rc1);
    PASS();
}

/* Grok Build children reach MCP only through its search_tool/use_tool
 * dispatcher and filter inheritance per server, so the direct profile must
 * name the server for inheritance and spell out the tier's qualified tool ids
 * as the dispatcher accepts them; handoff profiles must inherit nothing. */
TEST(agent_profiles_grok_uses_dispatcher_ids_and_named_inheritance) {
    for (int tier = 0; tier < (int)CBM_GRAPH_TIER_COUNT; tier++) {
        const char *slug = cbm_graph_tier_slug((cbm_graph_tier_t)tier);
        char *direct = cbm_render_graph_profile(CBM_GRAPH_DIALECT_GROK, (cbm_graph_tier_t)tier,
                                                CBM_GRAPH_ACCESS_DIRECT, NULL);
        char *handoff = cbm_render_graph_profile(CBM_GRAPH_DIALECT_GROK, (cbm_graph_tier_t)tier,
                                                 CBM_GRAPH_ACCESS_HANDOFF, NULL);
        char name_line[128];
        snprintf(name_line, sizeof(name_line), "---\nname: %s\n", slug);
        int direct_ok =
            direct && strstr(direct, name_line) &&
            strstr(direct, "tools: read_file, grep, list_dir, search_tool, use_tool\n") &&
            strstr(direct, "mcpInheritance:\n  named:\n    - codebase-memory-mcp\n---\n") &&
            strstr(direct, "codebase-memory-mcp__search_graph") &&
            strstr(direct, "codebase-memory-mcp__check_index_coverage") &&
            !strstr(direct, "codebase-memory-mcp__*") && !profile_has_mutator(direct) &&
            (tier != (int)CBM_GRAPH_TIER_SCOUT) ==
                (strstr(direct, "codebase-memory-mcp__query_graph") != NULL);
        int handoff_ok = handoff && strstr(handoff, name_line) &&
                         strstr(handoff, "tools: read_file, grep, list_dir\n") &&
                         strstr(handoff, "mcpInheritance: none\n---\n") &&
                         !strstr(handoff, "use_tool") && !strstr(handoff, "codebase-memory-mcp__");
        free(direct);
        free(handoff);
        if (!direct_ok || !handoff_ok) {
            FAIL("Grok profiles must name the server for inheritance and list dispatcher tool ids");
        }
    }
    PASS();
}

TEST(agent_profiles_render_deterministically_and_reject_invalid_inputs) {
    char *first = cbm_render_graph_profile(CBM_GRAPH_DIALECT_QWEN, CBM_GRAPH_TIER_VERIFY,
                                           CBM_GRAPH_ACCESS_DIRECT, NULL);
    char *second = cbm_render_graph_profile(CBM_GRAPH_DIALECT_QWEN, CBM_GRAPH_TIER_VERIFY,
                                            CBM_GRAPH_ACCESS_DIRECT, NULL);
    ASSERT_NOT_NULL(first);
    ASSERT_NOT_NULL(second);
    ASSERT_STR_EQ(first, second);
    free(first);
    free(second);
    ASSERT_NULL(cbm_render_graph_profile(CBM_GRAPH_DIALECT_COUNT, CBM_GRAPH_TIER_VERIFY,
                                         CBM_GRAPH_ACCESS_DIRECT, NULL));
    ASSERT_NULL(cbm_render_graph_profile(CBM_GRAPH_DIALECT_CLAUDE, CBM_GRAPH_TIER_COUNT,
                                         CBM_GRAPH_ACCESS_DIRECT, NULL));
    ASSERT_NULL(cbm_render_graph_profile(CBM_GRAPH_DIALECT_CLAUDE, CBM_GRAPH_TIER_VERIFY,
                                         CBM_GRAPH_ACCESS_COUNT, NULL));
    ASSERT_NULL(cbm_render_graph_profile(CBM_GRAPH_DIALECT_KIRO, CBM_GRAPH_TIER_VERIFY,
                                         CBM_GRAPH_ACCESS_DIRECT, NULL));
    ASSERT_NULL(cbm_render_graph_prompt(CBM_GRAPH_TIER_COUNT, CBM_GRAPH_ACCESS_DIRECT));
    ASSERT_NULL(cbm_render_graph_prompt(CBM_GRAPH_TIER_VERIFY, CBM_GRAPH_ACCESS_COUNT));
    PASS();
}

TEST(agent_profiles_omp_direct_has_prefixed_tools_and_handoff_excludes_mcp) {
    char *direct = cbm_render_graph_profile(CBM_GRAPH_DIALECT_OMP, CBM_GRAPH_TIER_VERIFY,
                                            CBM_GRAPH_ACCESS_DIRECT, NULL);
    ASSERT_NOT_NULL(direct);
    ASSERT_NOT_NULL(strstr(direct, "mcp__codebase_memory_mcp_check_index_coverage"));
    ASSERT_NOT_NULL(strstr(direct, "mcp__codebase_memory_mcp_search_graph"));
    ASSERT_NOT_NULL(strstr(direct, "read-summarize: false"));
    ASSERT_NOT_NULL(strstr(direct, "autoloadSkills: [codebase-memory]"));
    ASSERT_NULL(strstr(direct, "mcp__codebase-memory-mcp__"));
    ASSERT(!profile_has_mutator(direct));
    free(direct);
    char *handoff = cbm_render_graph_profile(CBM_GRAPH_DIALECT_OMP, CBM_GRAPH_TIER_VERIFY,
                                             CBM_GRAPH_ACCESS_HANDOFF, NULL);
    ASSERT_NOT_NULL(handoff);
    ASSERT_NULL(strstr(handoff, "mcp__codebase_memory_mcp_"));
    ASSERT_NULL(strstr(handoff, "mcp__codebase-memory-mcp__"));
    free(handoff);
    PASS();
}

SUITE(legacy_agent_profiles) {
    RUN_TEST(agent_profiles_stable_tier_identity);
    RUN_TEST(agent_profiles_tiers_encode_distinct_evidence_budgets);
    RUN_TEST(agent_profiles_handoff_only_dialects_fail_closed_for_direct_access);
    RUN_TEST(agent_profiles_server_level_dialects_hard_enforce_read_only_tools);
    RUN_TEST(agent_profiles_kiro_is_valid_json_and_escapes_binary_path);
    RUN_TEST(agent_profiles_codex_declares_transport_and_escapes_binary_path);
    RUN_TEST(agent_profiles_omp_direct_has_prefixed_tools_and_handoff_excludes_mcp);
    RUN_TEST(agent_profiles_grok_uses_dispatcher_ids_and_named_inheritance);
    RUN_TEST(agent_profiles_render_deterministically_and_reject_invalid_inputs);
}
