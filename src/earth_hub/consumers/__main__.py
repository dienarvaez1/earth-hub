"""Run the consumer: `python -m earth_hub.consumers`."""

import signal
import sys

from earth_hub.config import get_settings
from earth_hub.consumers.earthquake_consumer import EarthquakeConsumer, Store, make_consumer
from earth_hub.database.engine import make_engine
from earth_hub.logging_config import configure_logging


def main() -> int:
    configure_logging()
    settings = get_settings()
    consumer = EarthquakeConsumer(
        make_consumer(
            settings.kafka_bootstrap_servers,
            settings.kafka_consumer_group,
            settings.consumer_topic,
        ),
        Store(make_engine(settings)),
        heartbeat_path=settings.consumer_heartbeat_path,
        batch_size=settings.consumer_batch_size,
    )
    for signum in (signal.SIGTERM, signal.SIGINT):
        signal.signal(signum, lambda *_: consumer.stop_event.set())
    consumer.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
