#!/bin/bash
set -euo pipefail

cd "$(dirname "$0")"

INPUT_PATH="${1:-/input/data}"

H="${HADOOP_HOME:-/usr/local/hadoop}"
STREAM_JAR=""
for lib in "${H}/share/hadoop/tools/lib" "${H}/tools/lib" "/usr/local/hadoop/share/hadoop/tools/lib"; do
  if [ -d "${lib}" ]; then
    cand="$(find "${lib}" -maxdepth 1 -type f -name 'hadoop-streaming-*.jar' ! -name '*sources*' 2>/dev/null | head -1 || true)"
    if [ -n "${cand}" ]; then
      STREAM_JAR="${cand}"
      break
    fi
  fi
done
if [ -z "${STREAM_JAR}" ]; then
  echo "ERROR: hadoop-streaming jar not found under ${H}/share/hadoop/tools/lib. Set HADOOP_HOME." >&2
  exit 1
fi

echo "Using streaming jar: ${STREAM_JAR}"
echo "Input HDFS path: ${INPUT_PATH}"

hdfs dfs -rm -r -f /tmp/indexer/mr1 /indexer/index
hdfs dfs -mkdir -p /indexer

MAP_SCRIPT="$(pwd)/mapreduce/mapper1.py"
RED_SCRIPT="$(pwd)/mapreduce/reducer1.py"
chmod +x "${MAP_SCRIPT}" "${RED_SCRIPT}"

hadoop jar "${STREAM_JAR}" \
  -D mapreduce.job.name=search_index_mr1 \
  -D mapreduce.job.reduces=1 \
  -files "${MAP_SCRIPT},${RED_SCRIPT}" \
  -mapper "python3 mapper1.py" \
  -reducer "python3 reducer1.py" \
  -input "${INPUT_PATH}" \
  -output /tmp/indexer/mr1

hdfs dfs -mkdir -p /indexer/index
hdfs dfs -cp -f /tmp/indexer/mr1/part-* /indexer/index/
hdfs dfs -rm -r -f /tmp/indexer/mr1

echo "Index written to hdfs:///indexer/index"
hdfs dfs -ls /indexer/index
