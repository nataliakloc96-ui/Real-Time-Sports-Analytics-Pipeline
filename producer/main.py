from __future__ import annotations

import time

import structlog
from dotenv import load_dotenv

load_dotenv()

from config import FootballAPIConfig, KafkaConfig
from api_client import FootballDataClient, FootballAPIError
from kafka_producer import MatchEventProducer
from schemas import match_to_event

logger = structlog.get_logger()

POLL_INTERVAL_SECONDS = 60

COMPETITIONS = ["PL", "CL", "BL1"]

def run_once(api_client: FootballDataClient, producer: MatchEventProducer) -> None:

    total_sent = 0

    for competition in COMPETITIONS:
        try:
            matches = api_client.get_matches_by_competition(competition, status="SCHEDULED")
        except FootballAPIError as exc:
            logger.error("competition_fetch_failed", competition=competition, error=str(exc))
            continue
        
        for match in matches:
            event = match_to_event(match)
            producer.send_event(event)
            total_sent += 1

    producer.flush(timeout=10.0)
    logger.info("cycle_complete", total_events=total_sent)

def main() -> None:
    api_config = FootballAPIConfig()
    kafka_config = KafkaConfig()

    logger.info(
        "producer_starting",
        topic=kafka_config.topic,
        competitions=COMPETITIONS,
        poll_interval=POLL_INTERVAL_SECONDS,
    )

    with FootballDataClient(api_config) as api_client:
        producer = MatchEventProducer(kafka_config)

        try:
            while True:
                run_once(api_client, producer)
                time.sleep(POLL_INTERVAL_SECONDS)
        except KeyboardInterrupt:
            logger.info("shutdown_requested")
        finally:
            producer.flush(timeout=15.0)
            logger.info("producer_stopped")

if __name__ == "__main__":
    main()