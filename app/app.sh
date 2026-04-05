#!/bin/bash

service ssh restart
bash start-services.sh

hdfs dfsadmin -safemode wait || true

export SEARCH_VENV="${SEARCH_VENV:-/tmp/search_venv}"
rm -rf "${SEARCH_VENV}"
python3 -m venv "${SEARCH_VENV}"
# shellcheck source=/dev/null
source "${SEARCH_VENV}/bin/activate"

pip install -U pip wheel setuptools
pip install -r requirements.txt

venv-pack -o /app/.venv.tar.gz

bash prepare_data.sh
bash index.sh

export SPARK_SEARCH_MASTER="${SPARK_SEARCH_MASTER:-local[*]}"
bash search.sh "history"
echo "For YARN search: unset SPARK_SEARCH_MASTER; bash search.sh \"your phrase\"  —  http://localhost:8088/"
