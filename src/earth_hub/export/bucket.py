"""The few S3 operations the exporter needs, on Cloudflare R2 (or any S3-compatible store).

R2 evaluates `If-None-Match` and `If-Match` on PutObject atomically, which makes the run lock
safe. Its ETags are not documented as MD5, so the exporter never compares content by ETag:
it keeps its own fingerprints in a manifest.
"""

import base64
import hashlib
from collections.abc import Iterable, Iterator

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from earth_hub.config import Settings

DELETE_BATCH = 1000  # DeleteObjects limit


class PreconditionFailed(Exception):
    """A conditional write lost: the object exists (If-None-Match) or changed (If-Match)."""


class MissingSettings(Exception):
    pass


def make_s3_client(settings: Settings):
    missing = [
        name
        for name, value in (
            ("R2_BUCKET", settings.r2_bucket),
            ("R2_ACCESS_KEY_ID", settings.r2_access_key_id.get_secret_value()),
            ("R2_SECRET_ACCESS_KEY", settings.r2_secret_access_key.get_secret_value()),
            (
                "R2_ACCOUNT_ID or R2_ENDPOINT_URL",
                settings.r2_account_id or settings.r2_endpoint_url,
            ),
        )
        if not value
    ]
    if missing:
        raise MissingSettings(f"set {', '.join(missing)} in .env (see .env.example)")
    endpoint = (
        settings.r2_endpoint_url or f"https://{settings.r2_account_id}.r2.cloudflarestorage.com"
    )
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name="auto",
        aws_access_key_id=settings.r2_access_key_id.get_secret_value(),
        aws_secret_access_key=settings.r2_secret_access_key.get_secret_value(),
        config=Config(
            signature_version="s3v4",
            retries={"max_attempts": 5, "mode": "standard"},
            max_pool_connections=settings.export_workers + 4,
            # Checksums only where the API requires them; R2 rejects some of boto3's defaults.
            request_checksum_calculation="when_required",
            response_checksum_validation="when_required",
        ),
    )


class Bucket:
    def __init__(self, client, name: str):
        self.client = client
        self.name = name

    def keys(self, prefix: str) -> Iterator[str]:
        paginator = self.client.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=self.name, Prefix=prefix):
            for item in page.get("Contents", []):
                yield item["Key"]

    def get(self, key: str) -> tuple[bytes, str] | None:
        """(body, etag), or None when the object does not exist."""
        try:
            response = self.client.get_object(Bucket=self.name, Key=key)
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise
        return response["Body"].read(), response["ETag"]

    def get_if_changed(self, key: str, etag: str | None) -> tuple[bytes, str] | None | str:
        """Like get, but only the ETag when the object still has `etag` (a conditional
        request: the body is not sent again)."""
        if etag is None:
            return self.get(key)
        try:
            response = self.client.get_object(Bucket=self.name, Key=key, IfNoneMatch=etag)
        except ClientError as exc:
            code = exc.response["Error"]["Code"]
            if code in ("304", "NotModified"):
                return etag
            if code in ("NoSuchKey", "404"):
                return None
            raise
        return response["Body"].read(), response["ETag"]

    def put(
        self,
        key: str,
        body: bytes,
        *,
        content_type: str = "application/json",
        content_encoding: str | None = None,
        if_none_match: bool = False,
        if_match: str | None = None,
    ) -> str:
        """Write the object and return its ETag. Content-MD5 makes the store check that
        what arrived is what was sent."""
        params = {
            "Bucket": self.name,
            "Key": key,
            "Body": body,
            "ContentType": content_type,
            "ContentMD5": base64.b64encode(hashlib.md5(body).digest()).decode(),
        }
        if content_encoding:
            params["ContentEncoding"] = content_encoding
        if if_none_match:
            params["IfNoneMatch"] = "*"
        if if_match:
            params["IfMatch"] = if_match
        try:
            return self.client.put_object(**params)["ETag"]
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("PreconditionFailed", "412"):
                raise PreconditionFailed(key) from None
            raise

    def delete(self, keys: Iterable[str]) -> list[str]:
        """Delete objects in batches; returns the keys that could not be deleted."""
        keys = list(keys)
        failed: list[str] = []
        for start in range(0, len(keys), DELETE_BATCH):
            batch = keys[start : start + DELETE_BATCH]
            response = self.client.delete_objects(
                Bucket=self.name,
                Delete={"Objects": [{"Key": k} for k in batch], "Quiet": True},
            )
            failed += [e["Key"] for e in response.get("Errors", [])]
        return failed
