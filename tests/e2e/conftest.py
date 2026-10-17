import pytest
from support.fdsn import FdsnServer
from support.feed import EventFactory, FeedServer
from support.pipeline import Pipeline


@pytest.fixture
def feed() -> FeedServer:
    return FeedServer()


@pytest.fixture
def event(feature_template) -> EventFactory:
    return EventFactory(feature_template)


@pytest.fixture
def fdsn() -> FdsnServer:
    return FdsnServer()


@pytest.fixture
def pipeline(settings, make_temp_topic, db, feed, fdsn):
    """A fresh pipeline: new live and backfill topics (3 partitions each), new database, new
    consumer groups, and mock USGS services."""
    pipe = Pipeline(
        settings,
        make_temp_topic(partitions=3),
        db,
        feed,
        backfill_topic=make_temp_topic(partitions=3),
        fdsn=fdsn,
    )
    yield pipe
    pipe.close()
