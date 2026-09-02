#!/usr/bin/env bash
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
job_name="${JENKINS_JOB_NAME:-codebase-memory-cli-release-tooling}"
jenkins_url="${JENKINS_TARGET_URL:-http://192.168.88.146:8080}"
cli="${JENKINS_CLI:-/tmp/codebase-memory-cli-jenkins-cli.jar}"
job_dir="${JENKINS_HOME:-/var/lib/jenkins}/jobs/$job_name"
config="$job_dir/config.xml"

if [[ -r /home/nate/.config/osint-suite/jenkins.env ]]; then
  # Reuse the host's existing Jenkins credentials when no explicit ones were set.
  source /home/nate/.config/osint-suite/jenkins.env
fi
jenkins_user="${JENKINS_USER:-${OSINT_JENKINS_USER:-}}"
jenkins_token="${JENKINS_TOKEN:-${JENKINS_API_TOKEN:-${OSINT_JENKINS_TOKEN:-}}}"

cli() {
  if [[ -n "$jenkins_user" && -n "$jenkins_token" ]]; then
    java -jar "$cli" -s "$jenkins_url" -auth "$jenkins_user:$jenkins_token" "$@"
  else
    java -jar "$cli" -s "$jenkins_url" "$@"
  fi
}

case "${1:-}" in
  configure)
    mkdir -p "$(dirname "$cli")"
    if [[ ! -s "$cli" ]]; then
      curl -fsS "$jenkins_url/jnlpJars/jenkins-cli.jar" -o "$cli"
    fi
    if cli get-job "$job_name" >/dev/null 2>&1; then
      cli update-job "$job_name" < "$root/scripts/jenkins-local-job.xml"
    else
      cli create-job "$job_name" < "$root/scripts/jenkins-local-job.xml"
    fi
    echo "configured $job_name"
    ;;
  inspect)
    test -r "$config" && rg -n '<triggers|pollSCM|release-tooling|file://' "$config"
    ;;
  trigger)
    cli build "$job_name" -s
    echo "triggered $jenkins_url/job/$job_name/"
    ;;
  *) echo "usage: $0 configure|inspect|trigger" >&2; exit 2 ;;
esac
