# Deprecated MCP-dependent tests

These test sources are deprecated for the CLI fork. They depend on the
historical `cbm_mcp_*` API, MCP headers, or the MCP test harness and must not be
used as evidence for CLI production behavior. They remain in the repository
only until they are migrated to neutral operation APIs or retired.

The production build intentionally leaves `MCP_SRCS` empty. Do not restore MCP
linkage to make these tests pass.

## Inventory

- `tests/test_mcp.c`
- `tests/test_agent_clients.c`
- `tests/test_cli.c`
- `tests/test_convergence_probe.c`
- `tests/test_daemon.c`
- `tests/test_daemon_application.c`
- `tests/test_edge_imports.c`
- `tests/test_edge_structural.c`
- `tests/test_edge_types_probe.c`
- `tests/test_grammar_probe_a.c`
- `tests/test_grammar_probe_b.c`
- `tests/test_grammar_probe_c.c`
- `tests/test_grammar_probe_d.c`
- `tests/test_grammar_probe_e.c`
- `tests/test_grammar_probe_f.c`
- `tests/test_grammar_probe_g.c`
- `tests/test_incremental.c`
- `tests/test_index_format.c`
- `tests/test_index_resilience.c`
- `tests/test_integration.c`
- `tests/test_lang_contract.c`
- `tests/test_lsp_resolution_probe.c`
- `tests/test_main.c`
- `tests/test_matrix_known_classes.c`
- `tests/test_matrix_new_constructs.c`
- `tests/test_node_creation_probe.c`
- `tests/repro/repro_grammar_build.c`
- `tests/repro/repro_grammar_config.c`
- `tests/repro/repro_harness.h`
- `tests/repro/repro_issue431.c`
- `tests/repro/repro_issue434.c`
- `tests/repro/repro_issue480.c`
- `tests/repro/repro_issue514.c`
- `tests/repro/repro_issue520.c`
- `tests/repro/repro_issue521.c`
- `tests/repro/repro_issue546.c`
- `tests/repro/repro_issue557.c`
- `tests/repro/repro_issue581.c`
- `tests/repro/repro_issue627.c`
- `tests/repro/repro_main.c`

This inventory is maintained by searching test sources for `cbm_mcp_*`, MCP
headers, and the shared MCP harness. A source is deprecated if it contains a
runtime dependency, even when only one helper or probe uses MCP.
