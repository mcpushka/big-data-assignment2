#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

echo "Waiting for Cassandra..."
for i in $(seq 1 60); do
  if python3 -c "from cassandra.cluster import Cluster; Cluster(['cassandra-server']).connect().shutdown()" 2>/dev/null; then
    echo "Cassandra is up."
    break
  fi
  echo "  attempt $i/60 ..."
  sleep 3
done

SEARCH_VENV="${SEARCH_VENV:-/tmp/search_venv}"
# shellcheck source=/dev/null
source "${SEARCH_VENV}/bin/activate"
python3 store_index.py "/indexer/index/part-*" "cassandra-server"
