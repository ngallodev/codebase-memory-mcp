# Repository agent instructions

## Local development builds and tests

- Prefer `scripts/build-dev.sh` for local production builds. It keeps `build/c`
  and rebuilds only changed or stale translation units through Make dependency
  files and the compiler cache.
- Prefer `scripts/test.sh --suites <suite>` for local validation. This uses the
  incremental test runner; choose the smallest relevant suite set.
- Do not use `scripts/build.sh` or the default `scripts/test.sh` for ordinary
  iteration. Those are clean, complete venue gates for GitHub, Jenkins, and
  release validation.
- Run the clean venue gates before delivery when the change requires full
  integration evidence.
