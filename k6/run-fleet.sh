#!/usr/bin/env bash
# Runs one k6 script against every environment, one environment at a time, writing
# results/<env>-<script>.json. Extra arguments go to k6, environment variables pass through.
#   k6/run-fleet.sh queue -e KIND=async -e COUNT=1000
#   VERSIONS="12 14" k6/run-fleet.sh compat
set -u
cd "$(dirname "$0")/.."
script=${1:?usage: k6/run-fleet.sh <script> [k6 args...]}
shift
mkdir -p results

# Environments created later got generated hostnames.
url() {
  case $1 in
    11) echo https://python-cloud-queues-python-3-11-iutzvn.laravel-demo.cloud ;;
    12) echo https://python-cloud-queues-python-3-12-e8ecok.laravel-demo.cloud ;;
    13) echo https://python-cloud-queues-python-3-13-ehqncb.laravel-demo.cloud ;;
    *) echo "https://python-cloud-queues-3-$1.laravel-demo.cloud" ;;
  esac
}

failed=()
for v in ${VERSIONS:-10 11 12 13 14}; do
  echo "=== 3.$v: $script ==="
  BASE_URL=$(url "$v") ENV_NAME="3-$v" k6 run "$@" "k6/$script.js" || failed+=("3.$v")
done

if [ ${#failed[@]} -gt 0 ]; then
  echo "failed: ${failed[*]}"
  exit 1
fi
echo "all environments passed"
