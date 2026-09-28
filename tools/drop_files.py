#!/usr/bin/env python3
"""Landing simulator for Assignment 2C.

Plays the upstream system: downloads the public source files once into a local cache,
then drops them into the Unity Catalog landing Volume one batch at a time. It runs on
your laptop, which has normal internet access (Free Edition compute may not).

    pip install requests databricks-sdk pyarrow
    databricks configure                          # workspace host + personal access token
    python tools/drop_files.py --download-only     # fill the cache first (large)
    python tools/drop_files.py --env dev --match 202502
    python tools/drop_files.py --env prd --batch 6 --interval 1200
"""
import argparse
import json
import pathlib
import sys
import time

import requests
from databricks.sdk import WorkspaceClient

S3 = "https://s3.amazonaws.com/tripdata"
PERIODS = [f"2024{m:02d}" for m in range(5, 13)] + [f"2025{m:02d}" for m in range(1, 7)]

# (url, landing sub-folder, file name) in drop order: month by month, New York then Jersey City
SOURCES = []
for p in PERIODS:
    SOURCES.append((f"{S3}/{p}-citibike-tripdata.zip", "tripdata_zip", f"{p}-citibike-tripdata.zip"))
    SOURCES.append((f"{S3}/JC-{p}-citibike-tripdata.csv.zip", "tripdata_zip", f"JC-{p}-citibike-tripdata.csv.zip"))

HEADERS = {"User-Agent": "bjit-a2-landing-simulator/1.0"}


def download(url: str, dest: pathlib.Path, retries: int = 6) -> pathlib.Path:
    """Download once into the cache; write to .part first; back off on errors."""
    if dest.exists() and dest.stat().st_size > 0:
        return dest
    part = dest.with_name(dest.name + ".part")
    for attempt in range(1, retries + 1):
        try:
            with requests.get(url, headers=HEADERS, stream=True, timeout=(30, 300)) as r:
                r.raise_for_status()
                with open(part, "wb") as fh:
                    for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                        fh.write(chunk)
            part.replace(dest)
            return dest
        except requests.RequestException as exc:
            wait = min(120, 2 ** attempt)
            print(f"    attempt {attempt}/{retries} failed: {exc}; retrying in {wait}s")
            time.sleep(wait)
    sys.exit(f"giving up on {url}")


def sample_parquet(src: pathlib.Path, rows: int) -> pathlib.Path:
    """Dev only: the first `rows` real rows of a Parquet file, same schema, dropped under the same name."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    out = src.with_name(f"sample{rows}_{src.name}")
    if not out.exists():
        batches, n = [], 0
        for batch in pq.ParquetFile(src).iter_batches(batch_size=min(rows, 500_000)):
            batches.append(batch)
            n += batch.num_rows
            if n >= rows:
                break
        pq.write_table(pa.Table.from_batches(batches).slice(0, rows), out)
    return out


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--profile", default="DEFAULT", help="Databricks CLI profile")
    p.add_argument("--catalog", default="workspace")
    p.add_argument("--env", default="dev", help="dev or prd: selects the <env>_landing schema")
    p.add_argument("--match", default="", help="only files whose name contains this text")
    p.add_argument("--batch", type=int, default=1, help="files per drop")
    p.add_argument("--interval", type=int, default=0, help="seconds to wait between drops")
    p.add_argument("--download-only", action="store_true", help="fill the local cache, drop nothing")
    p.add_argument("--sample-rows", type=int, default=0, help="dev only: drop the first N rows of Parquet files")
    p.add_argument("--cache", default=".landing_cache")
    args = p.parse_args()

    cache = pathlib.Path(args.cache)
    cache.mkdir(parents=True, exist_ok=True)
    state_path = cache / f"dropped_{args.env}.json"
    dropped = set(json.loads(state_path.read_text())) if state_path.exists() else set()

    todo = [s for s in SOURCES if args.match in s[2] and s[2] not in dropped]
    print(f"{len(todo)} file(s) to process")

    if args.download_only:
        for url, _, name in todo:
            print("downloading", name)
            download(url, cache / name)
        return

    w = WorkspaceClient(profile=args.profile)
    root = f"/Volumes/{args.catalog}/{args.env}_landing/raw"

    for start in range(0, len(todo), args.batch):
        for url, folder, name in todo[start:start + args.batch]:
            local = download(url, cache / name)
            if args.sample_rows and name.endswith(".parquet"):
                local = sample_parquet(local, args.sample_rows)
            w.files.create_directory(f"{root}/{folder}")
            remote = f"{root}/{folder}/{name}"
            print(f"dropping {name} ({local.stat().st_size / 1e6:,.0f} MB) -> {remote}")
            try:
                with open(local, "rb") as fh:
                    w.files.upload(remote, fh, overwrite=False)
                    # new names only: overwrites never fire triggers
            except Exception as exc:
                if "exist" not in str(exc).lower():
                    raise
                print("    already in the Volume, skipping")
            dropped.add(name)
            state_path.write_text(json.dumps(sorted(dropped), indent=1))
        if args.interval and start + args.batch < len(todo):
            print(f"waiting {args.interval}s before the next drop ...")
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
