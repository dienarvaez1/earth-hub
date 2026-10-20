"""Random but realistic message streams, shared by the tests that compare two ways of
processing the same messages: batched or one by one (test_batch.py), PostgreSQL or the bucket
(test_direct_parity.py)."""

import json


def random_stream(rng, ns: str, make_envelope, make_feature, merges: bool) -> list[bytes]:
    """A random but realistic message stream over a small pool of events in namespace `ns`."""
    pool = [f"{ns}e{i}" for i in range(12)]
    latest: dict[str, dict] = {}
    extras: dict[str, list[str]] = {}  # IDs an event gained, which USGS may delete on their own
    sent: list[bytes] = []
    for _ in range(80):
        choice = rng.random()
        event_id = rng.choice(pool)
        if choice < 0.40 or event_id not in latest:  # new event, or a newer revision
            base = latest.get(event_id) or make_feature(event_id, [event_id], updated=1792137700000)
            feature = json.loads(json.dumps(base))
            if event_id in latest:
                feature["properties"]["updated"] += rng.randint(1, 5) * 1000
                feature["properties"]["mag"] = round(rng.uniform(-1, 8), 2)
            if rng.random() < 0.15:  # gains an extra, never-seen ID
                extra = f"{ns}x{len(sent)}"  # unique: a shared ID would be a merge
                feature["properties"]["ids"] = feature["properties"]["ids"] + extra + ","
                extras.setdefault(event_id, []).append(extra)
            if rng.random() < 0.05:
                feature["properties"]["status"] = "deleted"
            latest[event_id] = feature
            sent.append(make_envelope(feature))
        elif choice < 0.55:  # an older revision arrives late
            older = json.loads(json.dumps(latest[event_id]))
            older["properties"]["updated"] -= rng.randint(1, 3) * 1000
            sent.append(make_envelope(older))
        elif choice < 0.70 and sent:  # an exact duplicate of something already sent
            sent.append(rng.choice(sent))
        elif choice < 0.78:  # invalid
            bad = json.loads(json.dumps(latest[event_id]))
            bad["geometry"]["coordinates"][1] = 120.0
            sent.append(make_envelope(bad))
        elif choice < 0.82:
            sent.append(rng.choice([b"not json", b"null", b'{"x": NaN}']))
        elif merges and choice < 0.86:  # two known reports turn out to be one event
            other = rng.choice([e for e in latest if e != event_id] or [event_id])
            merged = json.loads(json.dumps(latest[event_id]))
            merged["properties"]["ids"] = f",{event_id},{other},"
            merged["properties"]["updated"] = (
                max(
                    latest[event_id]["properties"]["updated"],
                    latest[other]["properties"]["updated"],
                )
                + 1000
            )
            latest[event_id] = merged
            sent.append(make_envelope(merged))
        elif choice < 0.92 and extras.get(event_id):  # a duplicate deleted on its own (DEF-026)
            duplicate = rng.choice(extras[event_id])
            deletion = make_feature(
                duplicate,
                updated=latest[event_id]["properties"]["updated"] + rng.randint(-2, 2) * 1000,
                status="deleted",
                mag=rng.choice([1.0, -5]),  # -5: the placeholder uw deletions carry (DEF-027)
            )
            sent.append(make_envelope(deletion))
        else:
            sent.append(make_envelope(latest[event_id]))  # resend of the current version
    return sent
