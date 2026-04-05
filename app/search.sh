#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

if [ $# -lt 1 ]; then
  echo "Usage: $0 \"your search query\"" >&2
  exit 1
fi

SEARCH_VENV="${SEARCH_VENV:-/tmp/search_venv}"
# shellcheck source=/dev/null
source "${SEARCH_VENV}/bin/activate"

export PYSPARK_DRIVER_PYTHON="$(which python3)"
QUERY="$*"

SEARCH_USE_SPARK="${SEARCH_USE_SPARK:-1}"
SPARK_SEARCH_MASTER="${SPARK_SEARCH_MASTER:-yarn}"

echo "search.sh: BM25 query (SEARCH_USE_SPARK=${SEARCH_USE_SPARK}, master=${SPARK_SEARCH_MASTER}): ${QUERY}"

run_with_stdin() {
  printf '%s' "${QUERY}" | "$@"
}

if [ "${SEARCH_USE_SPARK}" = "1" ] || [ "${SEARCH_USE_SPARK}" = "true" ]; then
  export BM25_QUERY="${QUERY}"
  if [ "${SPARK_SEARCH_MASTER}" = "yarn" ]; then
    export PYSPARK_PYTHON=./.venv/bin/python
    spark-submit \
      --master yarn \
      --deploy-mode client \
      --conf spark.yarn.appMasterEnv.PYSPARK_PYTHON=./.venv/bin/python \
      --archives /app/.venv.tar.gz#.venv \
      --num-executors 1 \
      --executor-memory 512m \
      --driver-memory 512m \
      query.py
  else
    export PYSPARK_PYTHON="$(which python3)"
    export SPARK_LOCAL_IP=127.0.0.1
    export SPARK_PUBLIC_DNS=127.0.0.1
    export SPARK_QUERY_LOCAL_BIND=1
    spark-submit \
      --master "${SPARK_SEARCH_MASTER}" \
      --conf spark.driver.host=127.0.0.1 \
      --conf spark.driver.bindAddress=127.0.0.1 \
      --conf spark.ui.enabled=false \
      query.py
  fi
else
  unset BM25_QUERY
  run_with_stdin python3 query.py
fi
