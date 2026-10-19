"""Write dashboard/wrangler.jsonc from the template, with the bucket from R2_BUCKET.

    uv run python scripts/dashboard_config.py [--bucket NAME] [--out PATH]

The bucket name lives in .env like the other R2 settings, so it is never committed.
"""

import argparse
import re
import sys
from pathlib import Path

DASHBOARD = Path(__file__).resolve().parents[1] / "dashboard"
TEMPLATE = DASHBOARD / "wrangler.template.jsonc"
BUCKET_NAME = re.compile(r"^[a-z0-9][a-z0-9-]{1,61}[a-z0-9]$")  # R2's naming rules


def render(bucket: str, *, remote: bool = True) -> str:
    """`remote`: whether `wrangler dev` uses the real bucket (deploys always do)."""
    if not BUCKET_NAME.match(bucket):
        raise ValueError(f"not a valid R2 bucket name: {bucket!r}")
    text = TEMPLATE.read_text().replace("{{R2_BUCKET}}", bucket)
    return text.replace("{{REMOTE}}", "true" if remote else "false")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bucket", help="default: R2_BUCKET from .env")
    parser.add_argument("--out", type=Path, default=DASHBOARD / "wrangler.jsonc")
    args = parser.parse_args(argv)
    bucket = args.bucket
    if bucket is None:
        from earth_hub.config import get_settings

        bucket = get_settings().r2_bucket
    if not bucket:
        print("dashboard: set R2_BUCKET in .env (see .env.example)", file=sys.stderr)
        return 2
    args.out.write_text(render(bucket))
    print(f"wrote {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
