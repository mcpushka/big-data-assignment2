# big-data-assignment2 — search engine (MapReduce + Cassandra + Spark)

## What it does

- **Prepare:** PySpark on YARN builds HDFS `/data` and `/input/data` from `app/data/*.txt` or optional `a.parquet` (Kaggle Wikipedia).
- **Index:** Hadoop Streaming (`mapreduce/mapper1.py`, `reducer1.py`) → HDFS `/indexer/index`.
- **Store:** `store_index.py` loads the index into Cassandra (`search_engine`).
- **Search:** `query.py` — BM25 top-10 over Cassandra; `search.sh` defaults to **`spark-submit --master yarn`** (RDD). No Pandas.

## How to run

Needs **Docker** and **Docker Compose**. From repo root:

```bash
docker compose up
```

First run is slow (HDFS, MR, Cassandra). Optional: put `a.parquet` in `app/`; else sample `.txt` is used.

**Search on YARN** (while `cluster-master` is up); RM: http://localhost:8088/

```bash
docker exec -it cluster-master bash -lc 'cd /app && source "${SEARCH_VENV:-/tmp/search_venv}/bin/activate" && unset SPARK_SEARCH_MASTER && bash search.sh "your query"'
```

`app.sh` runs the demo search with **`SPARK_SEARCH_MASTER=local[*]`** so compose finishes; **`search.sh` alone** still defaults to YARN.

**If search hangs on YARN in Docker:** use `SPARK_SEARCH_MASTER=local[*]` or `SEARCH_USE_SPARK=0`.

**Windows:** use LF line endings in `app/*.sh`; venv lives in `/tmp/search_venv` inside the container.

## Deliverables

Per course: `create_index.sh`, `store_index.sh`, `index.sh`, `mapreduce/mapper1.py`, `reducer1.py`, `query.py`, `search.sh`, `start-services.sh`, `app.sh`, `requirements.txt`, `report.pdf`.
