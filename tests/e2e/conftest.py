import pytest
from support.feed import EventFactory, FeedServer
from support.pipeline import Pipeline


@pytest.fixture
def feed() -> FeedServer:
    return FeedServer()


@pytest.fixture
def event(feature_template) -> EventFactory:
    return EventFactory(feature_template)


@pytest.fixture
def pipeline(settings, make_temp_topic, db, feed):
    """A fresh pipeline: new topic (3 partitions), new database, new consumer group."""
    pipe = Pipeline(settings, make_temp_topic(partitions=3), db, feed)
    yield pipe
    pipe.close()
