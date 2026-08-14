#!/usr/bin/env bash
#
# AUTHORITATIVE ENVIRONMENT VALIDATION
#
# Runs validate_env.sh inside the server image, on the bench network, with the
# SAME ulimits docker-compose applies to the server under test.
#
# This is the check that counts. Running the validator in a host shell answers a
# question about the wrong machine:
#
#   * On Windows and macOS the containers run inside a Linux VM (WSL2 or
#     LinuxKit). A Git Bash or macOS shell has no /proc/sys and its ulimits
#     belong to an emulation layer nothing under test ever touches.
#   * Even on native Linux, the host's descriptor limit is not the container's.
#     compose sets nofile=65535 for the server, so a host soft limit of 1024
#     would report a failure that will not occur, and a generous host limit
#     could hide one that will.
#
# The container also shares the kernel clock with every other container on the
# host, so the clock measurements taken here are the ones the latency
# measurement in Section 3.4.1 actually depends on.
#
# Usage:
#   ./scripts/validate_in_container.sh
#   ./scripts/validate_in_container.sh --json results/environment.json

set -uo pipefail
cd "$(dirname "$0")/.."

IMAGE="realtime-bench/server:1.0.0"
JSON_ARGS=()
OUT_HOST=""

if [[ "${1:-}" == "--json" ]]; then
  OUT_HOST="${2:-results/environment.json}"
  mkdir -p "$(dirname "$OUT_HOST")"
  JSON_ARGS=(--json /out/$(basename "$OUT_HOST"))
fi

if ! docker image inspect "$IMAGE" >/dev/null 2>&1; then
  echo "Building $IMAGE (first run only)..."
  docker compose --profile ws build ws || {
    echo "ERROR: build failed. Fix the build before validating." >&2
    exit 1
  }
fi

echo "Running validation inside $IMAGE with compose's ulimits..."
echo

MOUNTS=(-v "$(pwd)/scripts:/scripts:ro")
if [[ -n "$OUT_HOST" ]]; then
  MOUNTS+=(-v "$(pwd)/$(dirname "$OUT_HOST"):/out")
fi

docker run --rm -t \
  --ulimit nofile=65535:65535 \
  "${MOUNTS[@]}" \
  --entrypoint bash \
  "$IMAGE" /scripts/validate_env.sh "${JSON_ARGS[@]}"

status=$?

if [[ -n "$OUT_HOST" && -f "$OUT_HOST" ]]; then
  echo
  echo "Environment record written to $OUT_HOST"
  echo "Archive this with your results: every reported figure should ship with"
  echo "the environment that produced it (Section 3.6.3)."
fi

exit $status
