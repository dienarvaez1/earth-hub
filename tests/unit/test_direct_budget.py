"""Direct mode's R2 operations, counted per round and projected over a month (REQ-MODE-007).

R2's free tier allows 1,000,000 Class A operations (writes and listings) and 10,000,000 Class
B operations (reads) a month; deletes are free. The live load, measured on 20 October 2026
over one day of full mode: messages from the live feed in 437 distinct minutes (537 messages),
and 464 catch-up messages over 24 hourly passes, plus one deleted-events check.
"""

from datetime import UTC, datetime, timedelta

import pytest
from support.export import BUCKET, start_direct

from earth_hub.direct.runner import DirectRunner
from earth_hub.export.bucket import Bucket
from earth_hub.ingestion.events import build_message

FREE_CLASS_A = 1_000_000
FREE_CLASS_B = 10_000_000
FEED_ROUNDS_PER_DAY = 437
CATCHUP_PASSES_PER_DAY = 24
LOAD_FACTOR = 3
DAYS = 31

NOW = datetime(2026, 10, 16, 9, 0, tzinfo=UTC)


class CountingBucket(Bucket):
    def __init__(self, client, name):
        super().__init__(client, name)
        self.class_a = self.class_b = 0

    def put(self, key, body, **kwargs):
        self.class_a += 1
        return super().put(key, body, **kwargs)

    def keys(self, prefix):
        self.class_a += 1  # one listing request per 1,000 keys; these tests list fewer
        yield from super().keys(prefix)

    def get(self, key):
        self.class_b += 1
        return super().get(key)

    def get_if_changed(self, key, etag):
        if etag is not None:
            self.class_b += 1  # with no ETag it calls get, counted there
        return super().get_if_changed(key, etag)

    def take(self) -> tuple[int, int]:
        counts = (self.class_a, self.class_b)
        self.class_a = self.class_b = 0
        return counts


@pytest.mark.req("REQ-MODE-007")
def test_direct_mode_stays_well_inside_the_free_operations(
    s3, tmp_path, make_envelope, make_feature
):
    start_direct(s3)
    bucket = CountingBucket(s3, BUCKET)
    runner = DirectRunner(bucket, workdir=tmp_path, workers=2)
    # Some history first, so rounds update a real ID map, manifest and snapshot.
    runner.process([make_envelope(make_feature(f"h{i}")) for i in range(50)], now=NOW)
    bucket.take()

    # Live rounds: a new event, or a revision of one, about 1.2 messages a round.
    feed = []
    for n in range(20):
        values = [make_envelope(make_feature(f"live{n}", updated=1792137700000 + n))]
        if n % 5 == 0:
            values.append(make_envelope(make_feature(f"h{n}", updated=1792137800000 + n)))
        runner.process(values, now=NOW + timedelta(minutes=n + 1))
        feed.append(bucket.take())

    # A catch-up pass: what is stored is asked first, then about 20 messages in one round,
    # then its checkpoint.
    messages = [
        build_message(
            make_feature(f"h{i}", updated=1792137900000), fetched_at=NOW, feed_generated_ms=None
        )
        for i in range(20)
    ]
    to_send = runner.needed(messages)
    runner.process([m.to_json() for m in to_send], source="catchup", now=NOW + timedelta(hours=1))
    runner.set_checkpoint("catchup", NOW + timedelta(hours=1))
    catchup = bucket.take()

    per_round_a = max(a for a, _ in feed)
    per_round_b = max(b for _, b in feed)
    month_a = (
        LOAD_FACTOR
        * DAYS
        * (FEED_ROUNDS_PER_DAY * per_round_a + CATCHUP_PASSES_PER_DAY * catchup[0])
    )
    month_b = (
        LOAD_FACTOR
        * DAYS
        * (FEED_ROUNDS_PER_DAY * per_round_b + CATCHUP_PASSES_PER_DAY * catchup[1])
    )
    print(f"per live round: A {per_round_a}, B {per_round_b}; per catch-up pass: {catchup}")
    print(f"a month at {LOAD_FACTOR}x: A {month_a:,}, B {month_b:,}")
    # About 10 writes a live round: the lock, the redo record, the rows, the ID map, the
    # manifest, the snapshot and its meta.json, the log and the state (16% of the free
    # allowance at today's load).
    assert month_a < FREE_CLASS_A / 2
    assert month_b < FREE_CLASS_B / 10
