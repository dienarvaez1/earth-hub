"""What backfill and catch-up need from PostgreSQL: which versions are already stored, and
which windows and checkpoints are done. All reads here are cheap, one query per window."""

from dataclasses import dataclass
from datetime import datetime

from sqlalchemy import Engine, func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert

from earth_hub.database.models import BackfillWindow, Earthquake, EarthquakeId, IngestionCheckpoint
from earth_hub.database.revisions import Revision, compare
from earth_hub.ingestion.events import EventMessage
from earth_hub.ingestion.windows import Window

windows = BackfillWindow.__table__
checkpoints = IngestionCheckpoint.__table__


@dataclass(frozen=True)
class WindowCounts:
    expected: int
    fetched: int
    published: int
    skipped: int
    rejected: int


class Progress:
    def __init__(self, engine: Engine):
        self._engine = engine

    # --- what is stored ------------------------------------------------------------------

    def needs_publish(self, messages: list[EventMessage]) -> list[EventMessage]:
        """Messages that are new, or that the consumer would apply to a stored event any of
        their IDs points to (the same rule: database/revisions.py)."""
        ids = sorted({i for m in messages for i in m.ids})
        if not ids:
            return []
        query = (
            select(
                EarthquakeId.event_id,
                Earthquake.id,
                Earthquake.preferred_event_id,
                Earthquake.revision_ids,
                Earthquake.source_updated_ms,
                Earthquake.review_status,
            )
            .join(Earthquake, Earthquake.id == EarthquakeId.earthquake_id)
            .where(EarthquakeId.event_id.in_(ids))
        )
        with self._engine.connect() as conn:
            rows = conn.execute(query).all()
        event_of = {r.event_id: r.id for r in rows}
        stored = {
            r.id: Revision(
                r.preferred_event_id,
                tuple(r.revision_ids),
                r.source_updated_ms,
                r.review_status == "deleted",
            )
            for r in rows
        }
        newest: dict[str, EventMessage] = {}
        for message in messages:
            incoming = Revision(
                message.event_id,
                tuple(dict.fromkeys([message.event_id, *message.ids])),
                message.source_updated_ms,
                (message.payload.get("properties") or {}).get("status") == "deleted",
            )
            matched = {event_of[i] for i in incoming.ids if i in event_of}
            if matched and all(compare(stored[e], incoming)[0] != "applied" for e in matched):
                continue
            current = newest.get(message.event_id)
            if current is None or message.source_updated_ms > current.source_updated_ms:
                newest[message.event_id] = message
        return list(newest.values())

    # --- windows -------------------------------------------------------------------------

    def finished(self, job: str) -> set[tuple[datetime, datetime]]:
        """(start, end) of every window that is done; open windows are fetched again."""
        with self._engine.connect() as conn:
            rows = conn.execute(
                select(windows.c.window_start, windows.c.window_end).where(
                    windows.c.job == job, windows.c.status == "done"
                )
            ).all()
        return {(r.window_start, r.window_end) for r in rows}

    def record(self, job: str, window: Window, counts: WindowCounts) -> None:
        values = {
            "job": job,
            "window_start": window.start,
            "window_end": window.end,
            "status": "open" if window.open else "done",
            **counts.__dict__,
        }
        with self._engine.begin() as conn:
            conn.execute(
                pg_insert(windows)
                .values(values)
                .on_conflict_do_update(
                    constraint="uq_backfill_windows_job_start",
                    set_={k: v for k, v in values.items() if k not in ("job", "window_start")}
                    | {"finished_at": func.now()},
                )
            )

    # --- checkpoints ---------------------------------------------------------------------

    def checkpoint(self, name: str) -> datetime | None:
        with self._engine.connect() as conn:
            return conn.execute(
                select(checkpoints.c.value).where(checkpoints.c.name == name)
            ).scalar_one_or_none()

    def set_checkpoint(self, name: str, value: datetime) -> None:
        with self._engine.begin() as conn:
            conn.execute(
                pg_insert(checkpoints)
                .values(name=name, value=value)
                .on_conflict_do_update(
                    index_elements=["name"],
                    set_={"value": value, "updated_at": func.now()},
                )
            )
