import pytest
from support.feed import FeedServer
from support.pipeline import Pipeline


@pytest.fixture
def pipeline(settings, make_temp_topic, db):
    pipe = Pipeline(
        settings,
        make_temp_topic(partitions=3),
        db,
        FeedServer(),
        backfill_topic=make_temp_topic(partitions=3),
    )
    yield pipe
    pipe.close()
