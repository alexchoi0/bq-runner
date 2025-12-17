#!/usr/bin/env bash
set -euo pipefail

CONTAINER_NAME="${CONTAINER_NAME:-bq-runner}"

if podman ps -q --filter "name=${CONTAINER_NAME}" | grep -q .; then
    echo "Stopping ${CONTAINER_NAME}..."
    podman stop "${CONTAINER_NAME}"
    echo "Stopped."
else
    echo "Container ${CONTAINER_NAME} is not running."
fi
