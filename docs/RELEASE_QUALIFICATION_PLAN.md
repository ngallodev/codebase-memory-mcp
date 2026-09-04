# Release Qualification Plan

Status: **ACTIVE — supersedes migration-era remaining-work sequencing**

## Purpose

The CLI-first migration is architecture-complete enough that remaining work is release qualification, not further MCP migration. Structural changes after this point require evidence from build, reliability, performance, native-Windows, usability, or release validation.

The qualification model is:

```text
source commit
    |
    v
GitHub release-candidate workflow
    |
    +-- production Windows artifact + checksums/provenance
    |
    v
luigi.home.arpa (authoritative Windows 11 x64 qualification host)
    |
    +-- portable-artifact functional/reliability validation
    +-- installed-product validation
    +-- comparative performance trials
    |
    v
immutable evidence bundle
    |
    v
release decision
```

## Architecture freeze

The active product architecture is considered frozen unless qualification evidence identifies a concrete defect:

```text
CLI / hooks / UI / agent skill
              |
              v
      coordination daemon
              |
              v
      neutral operations
              |
              v
 store / index / recovery kernel
```

Do not add new compatibility layers, protocol fronts, benchmark commands, or diagnostic subsystems merely to satisfy old migration plans.

## Release-candidate identity

A release candidate is qualified only when all tests use immutable bytes produced by the GitHub release workflow.

Required identity evidence:

- release/tag name;
- source commit SHA;
- GitHub workflow/run identifier when available;
- archive filename;
- archive SHA-256;
- `codebase-memory-cli.exe` SHA-256;
- `codebase-memory-cli --version` output;
- release manifest/checksum material shipped by the workflow.

A locally rebuilt executable is development evidence, not release qualification evidence.

## Qualification gates

### Gate 1 — Linux consolidation

Required before or alongside RC publication:

- optimized production binary links successfully;
- targeted integration/E2E tests pass;
- broad Linux suite is run as a dedicated phase-end pass;
- failures are classified as product defect, test defect/stale contract, environment limitation, or accepted known issue;
- no stale test is kept merely to preserve test count.

### Gate 2 — GitHub release candidate

The release workflow must build immutable Windows artifact bytes and associated checksums/provenance, then stop at a **DRAFT external-qualification hold** by default. Those exact draft bytes become qualification input. Registry publication and public un-drafting are prohibited until external evidence is attached and verified.

### Gate 3 — Native Windows functional/reliability qualification

Authoritative host: `luigi.home.arpa`.

Validate both:

1. portable GitHub artifact directly;
2. installed product through supported install/update/uninstall flow.

Detailed procedure: `docs/WINDOWS_RELEASE_VALIDATION.md`.

### Gate 4 — Comparative Windows performance

Compare:

- **BASELINE:** most recent publicly released production-worthy version;
- **CANDIDATE:** current release candidate.

If no suitable CLI-first public baseline exists, the first fully qualified release becomes baseline generation zero and comparative gating begins with the next release.

Detailed protocol: `docs/WINDOWS_BENCHMARK_PROTOCOL.md`.

### Gate 5 — Agent usability

Use real user stories, not command-existence checks. A test agent should be able to:

- discover/index a repository;
- answer narrow code questions before broadening;
- inspect architecture and relationships;
- retrieve bounded snippets/source;
- detect changes;
- recover from stale/broken index state;
- use cross-repository intelligence selectively;
- produce bounded, useful output without MCP-era ceremony.

Only evidence from this phase should drive further CLI UX changes.

### Gate 6 — Product agent skill

Create the small production skill only after the CLI has passed usability qualification. The skill teaches workflow and judgment, not an exhaustive command reference.

### Gate 7 — Release decision and promotion

Release only when the evidence bundle is complete and all blockers are dispositioned. The Windows agent attaches `qualification-manifest.json` and `qualification-summary.md` to the GitHub draft. `.github/workflows/promote-qualified-release.yml` re-verifies that evidence against the exact Windows ZIP/executable hashes before publishing npm/PyPI or un-drafting the release.

## Evidence bundle contract

Each qualification run writes a self-contained directory:

```text
qualification/
  manifest.json
  SUMMARY.md
  machine/
    system.txt
    powershell.txt
    storage.txt
  artifacts/
    checksums.txt
    release-metadata.txt
  functional/
  recovery/
  windows-guards/
  install/
  benchmark/
    corpus.json
    baseline/
    candidate/
    comparison/
  logs/
```

`manifest.json` is the authoritative run index. It should include release identity, host identity, corpus generation, start/end timestamps, test results, benchmark result locations, and known exceptions.

Raw logs are evidence. Do not retain only summarized pass/fail output.

## Frozen benchmark corpus

The first Windows benchmark generation (`windows-corpus-1`) uses five fixed repository identities:

- `codebase-memory-cli` (`https://github.com/DeusData/codebase-memory-mcp.git`, historical repository slug retained);
- `specgen-aw` (`https://github.com/ngallodev-software/specgen-aw.git`);
- `agent-workflow` (`https://github.com/ngallodev-software/agent-workflow.git`);
- `agent-workflow-spec-contracts` (`https://github.com/ngallodev-software/agent-workflow-spec-contracts.git`);
- `herdr` (`https://github.com/herdrdev/herdr.git`).

Exact commit SHAs are not selected from remote HEAD during planning. They are recorded from the actual clean checkouts frozen for qualification on `luigi.home.arpa`. After that freeze, any commit change creates a new corpus generation and requires new baseline capture.

## Change/retest policy

If qualification finds a defect and the candidate changes:

- publish a new immutable RC artifact;
- do not overwrite or reuse the old evidence directory;
- rerun every gate affected by the change;
- always rerun artifact identity checks;
- rerun performance comparison when performance-sensitive code, build flags, compiler/toolchain, SQLite/indexing, daemon IPC, storage, or query behavior changed.

## Remaining sequence

1. Finish this qualification-plan re-baseline.
2. Complete Linux consolidation/build reliability evidence.
3. Publish GitHub RC.
4. Run native Windows qualification on `luigi.home.arpa`.
5. Run Windows baseline/candidate benchmarking on the frozen corpus.
6. Run agent usability user stories.
7. Make evidence-driven changes only.
8. Create production agent skill.
9. Requalify affected gates and make release decision.

## RC dispatch boundary

RC dispatch is now a distinct evidence-bearing step. Use `scripts/ci/prepare-rc-dispatch.py` and `docs/agents/GITHUB_RC_DISPATCH_AGENT.md` rather than hand-assembling workflow inputs.

The dispatch record binds the clean source commit, explicit RC tag, release workflow hash, corpus-generation manifest hash, and external-qualification hold. The first CLI-first candidate should normally use the next deliberate prerelease in the current release line (recommended `v0.11.0-rc.1` after public `v0.10.8`), but the operator must select the tag explicitly and verify it is unused before dispatch. Because `v0.10.8` predates the comparable CLI-first workflow surface, the first qualified CLI release must explicitly use `BASELINE_ZERO`; the next release uses that qualified CLI release as its immutable comparative baseline.
