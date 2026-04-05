#!/usr/bin/env python3
import glob
import os
import re

from pathvalidate import sanitize_filename
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


def clean_wiki_text(s: str) -> str:
    """Normalize Wikipedia HTML-ish text to a single plain line."""
    if s is None:
        return ""
    s = str(s)
    s = re.sub(r"<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _split_txt_path(path: str, text: str):
    """Parse id_title.txt filename pattern and body into (id, title, text) or None."""
    name = os.path.basename(path)
    if not name.endswith(".txt"):
        return None
    base = name[:-4]
    u = base.find("_")
    if u < 0:
        return None
    doc_id, title_slug = base[:u], base[u + 1 :]
    title = title_slug.replace("_", " ")
    text_clean = (
        (text or "")
        .replace("\n", " ")
        .replace("\r", " ")
        .replace("\t", " ")
    )
    if not text_clean.strip():
        return None
    return (doc_id, title, text_clean)


def _write_triples_to_hdfs(jfs, Path, records):
    """Write each document as one file under HDFS /data."""
    for doc_id, title, text in records:
        slug = str(title).replace(" ", "_")
        base = sanitize_filename(f"{doc_id}_{slug}")
        safe_name = f"{base}.txt" if not base.endswith(".txt") else base
        p = Path(f"/data/{safe_name}")
        out = jfs.create(p, True)
        out.write(bytearray(str(text).encode("utf-8")))
        out.close()


def prepare_from_txt(spark: SparkSession) -> None:
    """Materialize local .txt samples to HDFS /data and one TSV part under /input/data."""
    sc = spark.sparkContext
    n_docs = int(os.environ.get("N_DOCS", "100"))
    local_dir = os.environ.get("LOCAL_TXT_DIR", "/app/data")
    pattern = os.path.join(local_dir, "*.txt")
    paths = sorted(glob.glob(pattern))
    records = []
    for p in paths:
        with open(p, encoding="utf-8", errors="replace") as f:
            raw = f.read()
        row = _split_txt_path(p, raw)
        if row is not None:
            records.append(row)
    total = len(records)
    if total == 0:
        raise RuntimeError(
            "No .txt documents under {} ({}). Mount data/ into the container.".format(
                local_dir, pattern
            )
        )
    if n_docs > 0 and total > n_docs:
        records = records[:n_docs]

    jfs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    Path = sc._jvm.org.apache.hadoop.fs.Path
    if jfs.exists(Path("/data")):
        jfs.delete(Path("/data"), True)
    jfs.mkdirs(Path("/data"))
    _write_triples_to_hdfs(jfs, Path, records)

    rows = sc.parallelize(records)

    def to_line(triple):
        """Build one tab-separated id, title, body row for MapReduce input."""
        doc_id, title, text = triple
        tit = title.replace("\t", " ")
        return f"{doc_id}\t{tit}\t{text}"

    lines = rows.map(to_line)
    out_path = Path("/input/data")
    if jfs.exists(out_path):
        jfs.delete(out_path, True)
    lines.coalesce(1).saveAsTextFile("hdfs:///input/data")


def prepare_from_parquet(spark: SparkSession, parquet_path: str, n_docs: int) -> None:
    """Load Wikipedia parquet, write HDFS /data files and one TSV part under /input/data."""
    sc = spark.sparkContext
    df = spark.read.parquet(parquet_path).select("id", "title", "text")
    df = df.filter(col("text").isNotNull())

    clean_udf = udf(clean_wiki_text, StringType())
    df = df.withColumn("text", clean_udf(col("text")))
    df = df.filter(col("text") != "")
    if n_docs > 0:
        df = df.limit(n_docs)

    def to_line(row):
        """Build one tab-separated id, title, body row from a DataFrame row."""
        t = (row.text or "").replace("\n", " ").replace("\r", " ").replace("\t", " ")
        tit = (str(row.title) if row.title is not None else "").replace("\n", " ").replace(
            "\r", " "
        ).replace("\t", " ")
        return f"{row.id}\t{tit}\t{t}"

    collected = df.collect()
    jfs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    Path = sc._jvm.org.apache.hadoop.fs.Path
    for row in collected:
        text = row.text or ""
        if not str(text).strip():
            continue
        doc_id = str(row.id)
        title = str(row.title) if row.title is not None else ""
        slug = title.replace(" ", "_")
        base = sanitize_filename(f"{doc_id}_{slug}")
        safe_name = f"{base}.txt" if not base.endswith(".txt") else base
        p = Path(f"/data/{safe_name}")
        out = jfs.create(p, True)
        out.write(bytearray(str(text).encode("utf-8")))
        out.close()

    lines = sc.parallelize([to_line(r) for r in collected])
    out_path = Path("/input/data")
    if jfs.exists(out_path):
        jfs.delete(out_path, True)
    lines.coalesce(1).saveAsTextFile("hdfs:///input/data")


def main():
    """Run txt or parquet preparation into HDFS paths expected by index.sh."""
    n_docs = int(os.environ.get("N_DOCS", "100"))
    parquet_path = os.environ.get("PARQUET_PATH", "hdfs:///a.parquet")
    from_txt = os.environ.get("PREPARE_FROM_TXT", "").lower() in ("1", "true", "yes")

    spark = (
        SparkSession.builder.appName("data_preparation")
        .config("spark.sql.parquet.enableVectorizedReader", "true")
        .getOrCreate()
    )

    sc = spark.sparkContext
    hdfs = sc._jvm.org.apache.hadoop.fs.FileSystem.get(sc._jsc.hadoopConfiguration())
    Path = sc._jvm.org.apache.hadoop.fs.Path

    if from_txt:
        prepare_from_txt(spark)
    else:
        if not hdfs.exists(Path("/a.parquet")):
            raise FileNotFoundError(
                "No hdfs:///a.parquet — upload it or set PREPARE_FROM_TXT=1 for /app/data/*.txt."
            )
        if hdfs.exists(Path("/data")):
            hdfs.delete(Path("/data"), True)
        hdfs.mkdirs(Path("/data"))
        prepare_from_parquet(spark, parquet_path, n_docs)

    spark.stop()


if __name__ == "__main__":
    main()
