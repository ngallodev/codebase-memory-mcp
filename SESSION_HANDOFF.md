# Session Handoff

Date: 2026-09-02

## Repository

- Local path: `/lump/apps/codebase-memory-cli`
- Fork: `https://github.com/ngallodev/codebase-memory-cli`
- Upstream: `https://github.com/DeusData/codebase-memory-mcp`
- Current branch: `release-tooling`
- Last committed revision: `43f27d4d`
- Existing CLI PR: `https://github.com/ngallodev/codebase-memory-cli/pull/3`

## Current project state

The CP35 Windows-validation overlay was applied and committed. The CLI builds
as `build/c/codebase-memory-cli`. Existing untracked overlay archives and
`.codebase-memory/` were intentionally preserved.

## Jenkins task in progress

Requested: a Jenkins pipeline that builds and tests the local `release-tooling`
branch, triggered by script only, with no SCM polling.

Intended files:

- `Jenkinsfile`: local checkout of `release-tooling`, then `scripts/build.sh`
  and `scripts/test.sh`.
- `scripts/jenkins-local-job.xml`: Pipeline SCM job definition with an empty
  `<triggers/>` element.
- `scripts/jenkins-local-job.sh`: `configure`, `inspect`, and `trigger`
  commands.

## Blocker

Jenkins CLI/server availability was not completed. `/usr/bin/jenkins` exists,
but `http://localhost:8080` refused connections and Docker access was denied.
The active checkout also has no committed `release-tooling` pipeline files yet.

The workspace mapping was stale during the prior attempt and wrote temporary
Jenkins files under `/lump/apps/codebase-memory-mcp`; inspect and reconcile
those files before copying anything into this repository.

## Next actions

1. Confirm the active checkout path and inspect the temporary files under the
   old path.
2. Add the three Jenkins files to this repository using the normal patch path.
3. Commit them on `release-tooling`.
4. Start or connect to the local Jenkins server through the supported Jenkins
   CLI/API.
5. Configure the job, inspect that `<triggers/>` contains no `pollSCM`, and
   trigger it explicitly with the script.
6. Record the Jenkins build URL/result and any test failures here.
