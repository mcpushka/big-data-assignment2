#!/usr/bin/env python3
import subprocess
import sys

from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement


def hdfs_cat(glob_path: str) -> str:
    """Read concatenated HDFS files matching glob_path via hdfs dfs -cat."""
    return subprocess.check_output(
        ["hdfs", "dfs", "-cat", glob_path],
        text=True,
        stderr=subprocess.STDOUT,
    )


def main():
    """Rebuild search_engine keyspace tables from MR index lines on HDFS."""
    index_glob = sys.argv[1] if len(sys.argv) > 1 else "/indexer/index/part-*"
    host = sys.argv[2] if len(sys.argv) > 2 else "cassandra-server"

    raw = hdfs_cat(index_glob)
    vocab_rows = []
    posting_rows = []
    stats = None

    for line in raw.splitlines():
        if not line:
            continue
        if line.startswith("STATS\t"):
            _, n_s, total_dl_s = line.split("\t", 2)
            stats = (int(n_s), int(total_dl_s))
        elif line.startswith("VOCAB\t"):
            _, term, df_s = line.split("\t", 2)
            vocab_rows.append((term, int(df_s)))
        elif line.startswith("POSTING\t"):
            _, term, doc_id, tf_s, dl_s, title = line.split("\t", 5)
            posting_rows.append((term, doc_id, int(tf_s), int(dl_s), title))

    if stats is None:
        print("ERROR: no STATS line in index output", file=sys.stderr)
        sys.exit(1)

    n_docs, total_dl = stats
    cluster = Cluster([host])
    session = cluster.connect()

    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS search_engine
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
        """
    )
    session.set_keyspace("search_engine")

    session.execute("DROP TABLE IF EXISTS postings")
    session.execute("DROP TABLE IF EXISTS vocabulary")
    session.execute("DROP TABLE IF EXISTS corpus_stats")

    session.execute(
        """
        CREATE TABLE corpus_stats (
            id text PRIMARY KEY,
            n_docs int,
            total_dl bigint
        )
        """
    )
    session.execute(
        """
        CREATE TABLE vocabulary (
            term text PRIMARY KEY,
            df int
        )
        """
    )
    session.execute(
        """
        CREATE TABLE postings (
            term text,
            doc_id text,
            tf int,
            dl int,
            title text,
            PRIMARY KEY (term, doc_id)
        )
        """
    )

    session.execute(
        SimpleStatement(
            "INSERT INTO corpus_stats (id, n_docs, total_dl) VALUES (%s, %s, %s)"
        ),
        ("global", n_docs, total_dl),
    )

    batch_size = 100
    for i in range(0, len(vocab_rows), batch_size):
        chunk = vocab_rows[i : i + batch_size]
        for term, df in chunk:
            session.execute(
                SimpleStatement(
                    "INSERT INTO vocabulary (term, df) VALUES (%s, %s)"
                ),
                (term, df),
            )

    for i in range(0, len(posting_rows), batch_size):
        chunk = posting_rows[i : i + batch_size]
        for term, doc_id, tf, dl, title in chunk:
            session.execute(
                SimpleStatement(
                    "INSERT INTO postings (term, doc_id, tf, dl, title) VALUES (%s, %s, %s, %s, %s)"
                ),
                (term, doc_id, tf, dl, title),
            )

    cluster.shutdown()
    print(
        f"Loaded corpus_stats (n_docs={n_docs}, total_dl={total_dl}), "
        f"{len(vocab_rows)} vocab terms, {len(posting_rows)} postings."
    )


if __name__ == "__main__":
    main()
