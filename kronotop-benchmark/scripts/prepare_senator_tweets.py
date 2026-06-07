#!/usr/bin/env python3
"""
Converts the senator-tweets Parquet file to JSONL format for use with
the kronotop-benchmark vector tweets command.

Dependencies: pyarrow

Usage:
    python prepare_senator_tweets.py \
        --parquet data/train-00000-of-00001.parquet \
        --output train.jsonl
"""

import argparse
import json
import sys

import pyarrow.parquet as pq


def convert(parquet_path: str, output_path: str) -> None:
    reader = pq.ParquetFile(parquet_path)
    total = 0

    with open(output_path, "w", encoding="utf-8") as out:
        for batch in reader.iter_batches(batch_size=1000):
            rows = batch.to_pydict()
            n = len(rows["id"])
            for i in range(n):
                doc = {
                    "date": rows["date"][i],
                    "id": rows["id"][i],
                    "username": rows["username"][i],
                    "text": rows["text"][i],
                    "party": rows["party"][i],
                    "labels": rows["labels"][i],
                    "embeddings": [float(v) for v in rows["embeddings"][i]],
                }
                out.write(json.dumps(doc) + "\n")
            total += n
            if total % 10000 == 0:
                print(f"  {total:,} rows written...")

    print(f"Done: {total:,} rows → {output_path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Convert senator-tweets Parquet to JSONL")
    parser.add_argument("--parquet", required=True, help="Path to input .parquet file")
    parser.add_argument("--output", default="train.jsonl", help="Output JSONL path (default: train.jsonl)")
    args = parser.parse_args()

    try:
        convert(args.parquet, args.output)
    except FileNotFoundError:
        print(f"Error: file not found: {args.parquet}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
