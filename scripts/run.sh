#!/usr/bin/env bash
set -euo pipefail

IMAGE_NAME="${IMAGE_NAME:-bq-runner}"
IMAGE_TAG="${IMAGE_TAG:-latest}"
CONTAINER_NAME="${CONTAINER_NAME:-bq-runner}"
PORT="${PORT:-3000}"
MODE="${MODE:-mock}"

podman run \
    --rm \
    --name "${CONTAINER_NAME}" \
    -p "${PORT}:3000" \
    "${IMAGE_NAME}:${IMAGE_TAG}" \
    --mode "${MODE}" \
    --port 3000
