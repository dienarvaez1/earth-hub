import boto3
import pytest
from moto import mock_aws
from support.export import BUCKET, NOW, FakeApi, FlakyBucket, make_row

from earth_hub.export.r2 import Exporter


class FakeProducer:
    """Stands in for confluent_kafka.Producer; delivery reports are served on flush()."""

    def __init__(self, fail_keys: set[bytes] = frozenset(), drop_keys: set[bytes] = frozenset()):
        self.fail_keys = set(fail_keys)  # broker reports an error
        self.drop_keys = set(drop_keys)  # no report before the flush timeout
        self.produced: list[tuple[str, bytes, bytes]] = []
        self._pending = []
        self.flush_calls: list[float] = []

    def produce(self, topic, *, key, value, on_delivery):
        self._pending.append((topic, key, value, on_delivery))

    def poll(self, timeout):
        return 0

    def flush(self, timeout):
        self.flush_calls.append(timeout)
        still_pending = []
        for topic, key, value, callback in self._pending:
            if key in self.drop_keys:
                still_pending.append((topic, key, value, callback))
            elif key in self.fail_keys:
                callback("Broker: Not enough in-sync replicas", None)
            else:
                self.produced.append((topic, key, value))
                callback(None, None)
        self._pending = still_pending
        return len(still_pending)


@pytest.fixture
def fake_producer_cls() -> type[FakeProducer]:
    """The FakeProducer class, for tests that configure failures."""
    return FakeProducer


@pytest.fixture
def fake_producer() -> FakeProducer:
    return FakeProducer()


# The R2 exporter: a fake export API and an in-memory S3 (moto).


@pytest.fixture
def s3():
    with mock_aws():
        client = boto3.client("s3", region_name="us-east-1")
        client.create_bucket(Bucket=BUCKET)
        yield client


@pytest.fixture
def bucket(s3):
    return FlakyBucket(s3, BUCKET, failing=set())


@pytest.fixture
def api():
    return FakeApi([make_row(i) for i in range(1, 8)])


@pytest.fixture
def export(api, bucket, tmp_path):
    def run(*, clock=lambda: NOW, page_size=3, checkpoint_pages=1, **kwargs):
        exporter = Exporter(
            api.client(),
            bucket,
            api_url="http://api.test/api/v1",
            workers=4,
            page_size=page_size,
            checkpoint_pages=checkpoint_pages,
            clock=clock,
            git_commit="abc1234",
            local_dir=tmp_path / "exports",
        )
        return exporter.run(**kwargs)

    return run
