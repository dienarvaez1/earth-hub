"""Run the poller: `python -m earth_hub.ingestion [--once]`."""

import argparse
import logging
import signal
import sys
from datetime import UTC, datetime

from earth_hub.config import get_settings
from earth_hub.ingestion.poller import Poller
from earth_hub.ingestion.producer import EventPublisher, make_producer
from earth_hub.ingestion.state import STATE_WINDOW, read_topic_since, state_from_messages
from earth_hub.ingestion.usgs_client import UsgsClient, make_http_client
from earth_hub.logging_config import configure_logging

logger = logging.getLogger("earth_hub.ingestion")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Poll the USGS feed and publish to Kafka.")
    parser.add_argument("--once", action="store_true", help="run one round and exit")
    args = parser.parse_args(argv)

    configure_logging()
    settings = get_settings()

    since = datetime.now(UTC) - STATE_WINDOW
    state = state_from_messages(
        read_topic_since(settings.kafka_bootstrap_servers, settings.kafka_topic_raw, since)
    )
    logger.info("state rebuilt from topic", extra={"tracked_events": len(state)})

    publisher = EventPublisher(
        make_producer(settings.kafka_bootstrap_servers), settings.kafka_topic_raw
    )
    with make_http_client(settings.usgs_contact) as http:
        poller = Poller(
            UsgsClient(str(settings.usgs_feed_url), http),
            publisher,
            state,
            fallback_wait_s=settings.poll_interval_seconds,
            heartbeat_path=settings.poller_heartbeat_path,
        )
        if args.once:
            result = poller.run_round()
            poller.shutdown()
            return 1 if result.status == "failed" or result.failed_delivery else 0

        for signum in (signal.SIGTERM, signal.SIGINT):
            signal.signal(signum, lambda *_: poller.stop_event.set())
        poller.run_forever()
    return 0


if __name__ == "__main__":
    sys.exit(main())
