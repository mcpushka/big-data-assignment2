#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

SEARCH_VENV="${SEARCH_VENV:-/tmp/search_venv}"
# shellcheck source=/dev/null
source "${SEARCH_VENV}/bin/activate"

export PYSPARK_DRIVER_PYTHON="$(which python3)"
export PYSPARK_PYTHON=./.venv/bin/python

export N_DOCS="${N_DOCS:-100}"

hdfs dfs -mkdir -p /input || true

if [ "${PREPARE_FROM_TXT:-0}" = "1" ]; then
  echo "PREPARE_FROM_TXT=1: use bundled /app/data/*.txt"
  export PREPARE_FROM_TXT=1
else
  if [ -f ./a.parquet ]; then
    echo "Uploading local a.parquet to hdfs:///a.parquet"
    hdfs dfs -put -f ./a.parquet /a.parquet
  elif ! hdfs dfs -test -e /a.parquet 2>/dev/null; then
    echo "No /a.parquet on HDFS and no local ./a.parquet — using /app/data/*.txt via Spark"
    export PREPARE_FROM_TXT=1
  fi
fi

hdfs dfs -rm -r -f /input/data 2>/dev/null || true

spark-submit \
  --master yarn \
  --deploy-mode client \
  --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
  --archives /app/.venv.tar.gz#.venv \
  prepare_data.py

echo "Sample /data:"
hdfs dfs -ls /data | head || true
echo "Sample /input/data:"
hdfs dfs -ls /input/data | head || true
echo "prepare_data.sh finished."
