# Big-Data-Assignment2 — search engine (MapReduce + Cassandra + Spark)

## What it does

- **Prepare:** PySpark on YARN builds HDFS `/data` and `/input/data` from `app/data/*.txt`.
- **Index:** Hadoop Streaming (`mapreduce/mapper1.py`, `reducer1.py`) -> HDFS `/indexer/index`.
- **Store:** `store_index.py` loads the index into Cassandra (`search_engine`).
- **Search:** `query.py` — BM25 top-10 over Cassandra; `search.sh` defaults to `spark-submit --master yarn` (RDD). No Pandas.

## How to run

Needs Docker and Docker Compose. From repo root:

```bash
docker compose up
```

First run is slow (HDFS, MR, Cassandra).
