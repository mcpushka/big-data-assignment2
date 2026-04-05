#!/usr/bin/env python3
import math
import os
import re
import sys


def tokenize(text):
    """Tokenize text into lowercase alphanumeric tokens."""
    return re.findall(r"[a-z0-9]+", text.lower())


def _read_query():
    """Read query from BM25_QUERY or piped stdin."""
    q = (os.environ.get("BM25_QUERY") or "").strip()
    if q:
        return q
    if not sys.stdin.isatty():
        return sys.stdin.read().strip()
    return ""


def _print_results_block(query_text: str) -> None:
    """Print the labeled search result header block."""
    print("--- Search results ---", flush=True)
    print(f"Query: {query_text}", flush=True)
    print("BM25 top-10:", flush=True)


def _rank_pure(postings, df_by_term, n_docs, dl_avg, k1, b, query_text: str):
    """Compute BM25 in-process and print top-10 doc id and title lines."""
    doc_score = {}
    doc_title = {}
    for doc_id, term, tf, dl, title in postings:
        dfm = df_by_term.get(term)
        if dfm is None or dfm <= 0:
            continue
        idf = math.log(n_docs / dfm)
        denom = k1 * ((1.0 - b) + b * (dl / dl_avg)) + tf
        w = idf * (k1 + 1.0) * tf / denom if denom else 0.0
        doc_score[doc_id] = doc_score.get(doc_id, 0.0) + w
        if title:
            doc_title[doc_id] = title
        elif doc_id not in doc_title:
            doc_title[doc_id] = ""
    top = sorted(doc_score.items(), key=lambda x: -x[1])[:10]
    _print_results_block(query_text)
    for doc_id, _ in top:
        print(f"{doc_id}\t{doc_title.get(doc_id, '')}", flush=True)


def _rank_spark(postings, df_by_term, n_docs, dl_avg, k1, b, query_text: str):
    """Compute BM25 with Spark RDD and print top-10 doc id and title lines."""
    from pyspark.sql import SparkSession

    print(
        f"query.py: Spark RDD over {len(postings)} postings, {len(df_by_term)} terms...",
        flush=True,
    )
    builder = SparkSession.builder.appName("bm25_query")
    if os.environ.get("SPARK_QUERY_LOCAL_BIND") == "1":
        builder = builder.config("spark.driver.host", "127.0.0.1").config(
            "spark.driver.bindAddress", "127.0.0.1"
        )
    spark = builder.getOrCreate()
    sc = spark.sparkContext

    bc_df = sc.broadcast(df_by_term)
    bc_n = sc.broadcast(n_docs)
    bc_dl_avg = sc.broadcast(dl_avg)
    bc_k1 = sc.broadcast(k1)
    bc_b = sc.broadcast(b)

    def score_piece(item):
        """BM25 score increment for one posting tuple (doc_id, term, tf, dl, title)."""
        doc_id, term, tf, dl, title = item
        dfm = bc_df.value.get(term)
        if dfm is None or dfm <= 0:
            return doc_id, (0.0, title)
        N = bc_n.value
        dl_avg_v = bc_dl_avg.value
        k1_v = bc_k1.value
        b_v = bc_b.value
        idf = math.log(N / dfm)
        denom = k1_v * ((1.0 - b_v) + b_v * (dl / dl_avg_v)) + tf
        w = idf * (k1_v + 1.0) * tf / denom if denom else 0.0
        return doc_id, (w, title)

    combined = (
        sc.parallelize(postings)
        .map(score_piece)
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] if a[1] else b[1]))
    )
    top = combined.takeOrdered(10, key=lambda x: -x[1][0])
    _print_results_block(query_text)
    for doc_id, (score, title) in top:
        print(f"{doc_id}\t{title}", flush=True)

    spark.stop()


def main():
    """Load postings for query terms from Cassandra and run BM25 ranking."""
    query = _read_query()
    if not query:
        print("Empty query.", file=sys.stderr)
        sys.exit(1)

    terms = [t for t in tokenize(query) if t]
    if not terms:
        print("No usable terms in query.", file=sys.stderr)
        sys.exit(1)

    cassandra_host = os.environ.get("CASSANDRA_HOST", "cassandra-server")
    k1 = float(os.environ.get("BM25_K1", "1.0"))
    b = float(os.environ.get("BM25_B", "0.75"))

    from cassandra.cluster import Cluster

    cluster = Cluster([cassandra_host])
    cs = cluster.connect("search_engine")

    row = cs.execute(
        "SELECT n_docs, total_dl FROM corpus_stats WHERE id = %s", ("global",)
    ).one()
    if row is None:
        print("Index not loaded (corpus_stats missing). Run index.sh first.", file=sys.stderr)
        sys.exit(2)

    n_docs = int(row.n_docs)
    total_dl = int(row.total_dl)
    dl_avg = (total_dl / n_docs) if n_docs else 1.0

    postings = []
    df_by_term = {}

    for t in terms:
        v = cs.execute("SELECT df FROM vocabulary WHERE term = %s", (t,)).one()
        if v is None or v.df == 0:
            continue
        df_by_term[t] = int(v.df)
        for pr in cs.execute("SELECT doc_id, tf, dl, title FROM postings WHERE term = %s", (t,)):
            postings.append((pr.doc_id, t, int(pr.tf), int(pr.dl), pr.title or ""))

    cluster.shutdown()

    if not postings:
        print("--- Search results ---", flush=True)
        print(f"Query: {query}", flush=True)
        print("No postings for query terms.", flush=True)
        return

    _sus = (os.environ.get("SEARCH_USE_SPARK") or "1").lower()
    use_spark = _sus not in ("0", "false", "no")
    print(
        f"query.py: {len(postings)} postings, mode={'Spark RDD' if use_spark else 'in-process BM25'}",
        flush=True,
    )
    if use_spark:
        _rank_spark(postings, df_by_term, n_docs, dl_avg, k1, b, query)
    else:
        _rank_pure(postings, df_by_term, n_docs, dl_avg, k1, b, query)


if __name__ == "__main__":
    main()
