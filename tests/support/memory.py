"""In-memory stand-ins for database-backed helpers, for unit tests."""

from datetime import datetime

from earth_hub.ingestion.windows import Window


class MemoryProgress:
    """In-memory stand-in for earth_hub.database.progress.Progress."""

    def __init__(self, stored: dict[str, int] | None = None):
        self.stored = stored or {}  # event_id -> source_updated_ms already in the database
        self.windows: dict[tuple[str, datetime], tuple[Window, object]] = {}
        self.points: dict[str, datetime] = {}

    def needs_publish(self, messages):
        newest = {}
        for m in messages:
            known = [self.stored[i] for i in m.ids if i in self.stored]
            if known and m.source_updated_ms <= max(known):
                continue
            if (
                m.event_id not in newest
                or m.source_updated_ms > newest[m.event_id].source_updated_ms
            ):
                newest[m.event_id] = m
        return list(newest.values())

    def finished(self, job):
        return {
            (w.start, w.end) for (j, _), (w, _) in self.windows.items() if j == job and not w.open
        }

    def record(self, job, window, counts):
        self.windows[(job, window.start)] = (window, counts)

    def checkpoint(self, name):
        return self.points.get(name)

    def set_checkpoint(self, name, value):
        self.points[name] = value
