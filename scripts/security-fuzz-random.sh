#!/usr/bin/env bash
set -euo pipefail
command -v python3 >/dev/null 2>&1 || { echo 'FAIL: python3 missing' >&2; exit 1; }
BINARY="${1:?usage: security-fuzz-random.sh <binary-path> [duration_seconds]}"
DURATION="${2:-60}"
[[ -x "$BINARY" ]] || { echo "FAIL: binary not executable: $BINARY" >&2; exit 2; }
SEED="${CBM_FUZZ_SEED:-$$}"
# shellcheck source=test-runtime.sh
source "$(dirname "${BASH_SOURCE[0]}")/test-runtime.sh"
cbm_test_runtime_init
TMP="$CBM_TEST_RUNTIME_ROOT"
trap 'cbm_test_runtime_cleanup "$BINARY"' EXIT
export HOME="$TMP/home" XDG_CACHE_HOME="$CBM_CACHE_DIR"
mkdir -p "$HOME" "$TMP/repo"
printf 'int widget(void){return 1;}\n' > "$TMP/repo/main.c"
"$BINARY" index "$TMP/repo" --json >/dev/null
printf 'fuzz seed: %s\n' "$SEED"
END=$((SECONDS + DURATION)); ITER=0
while (( SECONDS < END )); do
  ITER=$((ITER + 1))
  mapfile -t ARGS < <(python3 - "$SEED" "$ITER" <<'PY'
import random,string,sys
random.seed(int(sys.argv[1])+int(sys.argv[2]))
commands=['search','source-search','query','snippet','coverage','changes','outline','status','projects','doctor','definitely-not-a-command']
print(random.choice(commands))
for _ in range(random.randint(0,5)):
    n=random.randint(0,300)
    alphabet=string.ascii_letters+string.digits+" ;|&$(){}[]<>\\\"'../:-_*?"
    print(''.join(random.choice(alphabet) for _ in range(n)))
if random.random()<0.5: print('--json')
PY
)
  ec=0
  if command -v timeout >/dev/null 2>&1; then
    timeout 3 "$BINARY" "${ARGS[@]}" >/dev/null 2>&1 || ec=$?
  else
    perl -e 'alarm(3); exec @ARGV' -- "$BINARY" "${ARGS[@]}" >/dev/null 2>&1 || ec=$?
  fi
  case "$ec" in 134|136|137|139) echo "FAIL: crash exit=$ec iteration=$ITER args=${ARGS[*]}" >&2; exit 1;; esac
done
printf 'PASS: random CLI fuzz (%d iterations, seed %s)\n' "$ITER" "$SEED"
