#!/usr/bin/env python3
import sys
import re
from collections import Counter


def tokenize(text):
    """Tokenize text into lowercase alphanumeric tokens."""
    return re.findall(r"[a-z0-9]+", text.lower())


def main():
    """Emit __DOCSTAT__ and POSTING lines per input TSV document line."""
    for raw in sys.stdin:
        raw = raw.rstrip("\n")
        if not raw:
            continue
        parts = raw.split("\t", 2)
        if len(parts) < 3:
            continue
        doc_id, title, text = parts[0], parts[1], parts[2]
        tokens = tokenize(text)
        dl = len(tokens)
        if dl == 0:
            continue
        print(f"__DOCSTAT__\t{doc_id}\t{dl}")
        title_esc = title.replace("\t", " ").replace("\n", " ")
        for term, tf in Counter(tokens).items():
            print(f"{term}\tPOSTING\t{doc_id}\t{tf}\t{dl}\t{title_esc}")


if __name__ == "__main__":
    main()
