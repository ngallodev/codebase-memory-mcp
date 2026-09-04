# Linux Release Qualification

Status: **IN PROGRESS**

This document records phase-end Linux qualification evidence for the CLI-first release candidate line. It is intentionally separate from migration history.

## Qualification policy

- Run product and test targets with their production/project compiler flags.
- Treat failures as evidence and classify them before changing code.
- Do not restore retired MCP/protocol behavior to satisfy stale tests.
- Platform-only Windows skips are acceptable on Linux when explicitly marked `SKIP_PLATFORM`.
- A production build is not considered verified until `build/c/codebase-memory-cli` links successfully.

## Fast sanitized foundation gate

Command:

```sh
make -f Makefile.cbm test-foundation
```

The foundation target now has its own minimal runner and incremental object graph. It no longer links the full `tests/test_main.c`, whose daemon/index/UI subprocess roles are intentionally outside the foundation boundary.

Current result:

```text
312 passed, 0 failed, 2 platform-only skips
```

The two skips are native-Windows-only process/locking probes.

During qualification this gate exposed and corrected two stale test-infrastructure contracts:

1. `test-foundation` incorrectly used the global test runner and therefore referenced subsystems it did not link.
2. `tests/test_log.c` still asserted the removed `mcp.request` observability event after that API had been deliberately deleted.

Neither finding required product architecture changes.

## Release and fixture contracts

The following contracts pass on the consolidated source:

```text
tests/test_runtime_isolation_contract.sh
tests/test_security_fuzz_harness.sh
tests/test_version_metadata_contract.sh
tests/test_language_count_contract.sh
tests/test_smoke_fixture_contract.sh
tests/test_release_archive_extractor_contract.sh
tests/test_release_candidate_derivation_contract.sh
tests/test_release_gate_chain_contract.sh
tests/test_vt_release_notes_contract.sh
scripts/check-no-test-skips.sh
```

`test_smoke_fixture_contract.sh` was reconciled with the supported CLI-first smoke topology. It now protects the actual release properties: canonical CLI invariants, installer round-trip, native Windows PowerShell installation, isolated PATH verification, single-binary packaging, and proxy-safe loopback requests.

`test_vt_release_notes_contract.sh` now uses `codebase-memory-cli` artifact names while retaining the historical GitHub repository slug where appropriate.

## Optimized production build

Command:

```sh
make -f Makefile.cbm cbm
```

The build has progressed incrementally through production sources and the large vendored grammar set with no compiler correctness failure. In the current execution environment, repeated bounded compiler windows expire during the optimized vendored/grammar build before final link.

Status: **NOT YET VERIFIED IN THIS PHASE**.

This is an execution-duration/build-ergonomics limitation, not a claimed pass. CP48 previously established an explicit Linux production link on an earlier consolidated state; current release qualification still requires a final link of the current candidate.

The GitHub RC build is expected to provide the authoritative release build evidence. A local final link should still be obtained when practical, but release qualification must use the immutable GitHub-produced artifact bytes.

## Remaining Linux phase-end work

1. Obtain final optimized link for the consolidated candidate, locally or through the GitHub RC workflow.
2. Build the full sanitized test runner.
3. Run targeted integration/E2E suites first.
4. Run the broad Linux suite as a dedicated phase-end pass.
5. Classify every failure as product defect, stale/test defect, environment limitation, or accepted known issue.
6. Rerun affected gates after any code change.

Do not expand test count solely to improve coverage statistics; prefer existing integration/E2E correctness guards.

## CP64 broad-runner progress and RC hold preparation

The broad sanitized `build/c/test-runner` build was resumed incrementally across three bounded compiler windows. It progressed through production, pipeline, and test objects with no compiler correctness failure, but did not yet reach the vendored link boundary in this environment. This remains **IN PROGRESS**, not a pass.

In parallel, the GitHub release workflow was reconciled with the external native-Windows qualification contract. Releases now default to a draft hold after CI verification; npm/PyPI publication and public un-drafting are deferred until `promote-qualified-release.yml` validates qualification evidence produced on `luigi.home.arpa` against the exact Windows ZIP and executable hashes.

## RC authoritative-build boundary

The local broad sanitized build remains useful diagnostic evidence, but it is not allowed to delay the release-qualification handoff indefinitely when the only blocker is bounded local compiler time and no compiler/source failure has surfaced.

For an actual release candidate, the GitHub release workflow is the authoritative final Linux/all-platform build and test venue because it:

- builds the immutable artifacts that are later qualified;
- executes the release test matrix, smoke, soak, and security gates;
- records the workflow run against the exact dispatched source commit;
- leaves the release in DRAFT for external Windows qualification.

A GitHub RC workflow failure is qualification evidence and must be fixed in a new source commit/new RC. A successful workflow supplies the final-link/build evidence that the current local execution environment has been unable to complete within bounded compiler windows.
