#!/usr/bin/env python3
import sys


def flush_docstats(buffer):
    """Print one STATS line from buffered __DOCSTAT__ value rows."""
    if not buffer:
        return
    n_docs = 0
    total_dl = 0
    for v in buffer:
        cols = v.split("\t")
        if len(cols) >= 2:
            total_dl += int(cols[1])
            n_docs += 1
    print(f"STATS\t{n_docs}\t{total_dl}")


def flush_term(term, buffer):
    """Print VOCAB plus POSTING lines for one reduce key."""
    postings = []
    for v in buffer:
        if not v.startswith("POSTING\t"):
            continue
        _, doc_id, tf_s, dl_s, title = v.split("\t", 4)
        postings.append((doc_id, int(tf_s), int(dl_s), title))
    df = len(postings)
    print(f"VOCAB\t{term}\t{df}")
    for doc_id, tf, dl, title in postings:
        print(f"POSTING\t{term}\t{doc_id}\t{tf}\t{dl}\t{title}")


def main():
    """Group sorted mapper output by key and flush STATS or term postings."""
    current_key = None
    buffer = []

    for line in sys.stdin:
        line = line.rstrip("\n")
        if not line:
            continue
        key, _, value = line.partition("\t")
        if current_key is not None and key != current_key:
            if current_key == "__DOCSTAT__":
                flush_docstats(buffer)
            else:
                flush_term(current_key, buffer)
            buffer = []
        current_key = key
        buffer.append(value)

    if current_key is not None:
        if current_key == "__DOCSTAT__":
            flush_docstats(buffer)
        else:
            flush_term(current_key, buffer)


if __name__ == "__main__":
    main()
