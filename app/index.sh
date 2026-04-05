#!/bin/bash
set -euo pipefail

DIR="$(dirname "$0")"
bash "${DIR}/create_index.sh" "${1:-/input/data}"
bash "${DIR}/store_index.sh"
