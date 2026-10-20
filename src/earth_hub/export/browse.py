"""Read-only look at the R2 bucket: list keys, count them, print or save one object.

    python -m earth_hub.export.browse ls [PREFIX] [--limit N]
    python -m earth_hub.export.browse count [PREFIX]
    python -m earth_hub.export.browse get KEY [--out FILE]

Only lists and reads (`make r2-ls`, `make r2-count`, `make r2-get`); writing is the
exporter's job. Credentials come from .env and are never printed.
"""

import argparse
import gzip
import sys
from pathlib import Path

LIST_PAGE = 1000


def list_objects(client, bucket: str, prefix: str, limit: int | None) -> list[dict]:
    """[{key, size, modified}], in key order, at most `limit`."""
    found = []
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(
        Bucket=bucket, Prefix=prefix, PaginationConfig={"PageSize": LIST_PAGE}
    ):
        for item in page.get("Contents", []):
            found.append(
                {"key": item["Key"], "size": item["Size"], "modified": item["LastModified"]}
            )
            if limit is not None and len(found) >= limit:
                return found
    return found


def count_objects(client, bucket: str, prefix: str) -> tuple[int, int]:
    """(objects, total bytes) under the prefix."""
    objects = size = 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        for item in page.get("Contents", []):
            objects += 1
            size += item["Size"]
    return objects, size


def read_object(client, bucket: str, key: str) -> bytes | None:
    """The object's bytes, gunzipped when the key ends in .gz; None when it does not exist."""
    try:
        body = client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except client.exceptions.NoSuchKey:
        return None
    return gzip.decompress(body) if key.endswith(".gz") else body


def main(argv: list[str] | None = None) -> int:
    from earth_hub.config import get_settings
    from earth_hub.export.bucket import MissingSettings, make_s3_client

    parser = argparse.ArgumentParser(description="Look at the R2 bucket (read-only).")
    commands = parser.add_subparsers(dest="command", required=True)
    ls = commands.add_parser("ls", help="list keys under a prefix")
    ls.add_argument("prefix", nargs="?", default="")
    ls.add_argument("--limit", type=int, default=50, help="at most this many (0: all)")
    count = commands.add_parser("count", help="count objects and bytes under a prefix")
    count.add_argument("prefix", nargs="?", default="")
    get = commands.add_parser("get", help="print one object (gunzipped if .gz), or save it")
    get.add_argument("key")
    get.add_argument("--out", type=Path, help="write the object to this file instead")
    args = parser.parse_args(argv)

    settings = get_settings()
    try:
        client = make_s3_client(settings)
    except MissingSettings as exc:
        print(f"r2: {exc}", file=sys.stderr)
        return 2
    bucket = settings.r2_bucket

    if args.command == "ls":
        for item in list_objects(client, bucket, args.prefix, args.limit or None):
            print(f"{item['modified']:%Y-%m-%d %H:%M:%S}  {item['size']:>10}  {item['key']}")
        return 0
    if args.command == "count":
        objects, size = count_objects(client, bucket, args.prefix)
        print(f"{objects} objects, {size:,} bytes under {args.prefix or '(the whole bucket)'}")
        return 0
    body = read_object(client, bucket, args.key)
    if body is None:
        print(f"r2: no object {args.key!r}", file=sys.stderr)
        return 1
    if args.out:
        args.out.write_bytes(body)
        print(f"{len(body):,} bytes written to {args.out}")
        return 0
    try:
        sys.stdout.write(body.decode())
    except UnicodeDecodeError:
        print(f"r2: {args.key} is binary; save it with --out FILE", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
