import os
from dataclasses import dataclass

@dataclass(frozen=True)
class FootballAPIConfig:
    base_url: str = "https://api.football-data.org/v4"
    api_key: str = os.environ["FOOTBALL_API_KEY"]
    requests_per_minute: int = 10
    timeout_seconds: float = 10.0
    max_retries: int = 5

@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    topic: str = os.environ.get("KAFKA_TOPIC", "raw_match_events")
    dlq_topic: str = os.environ.get("KAFKA_DLQ_TOPIC", "raw_match_events_dlq")