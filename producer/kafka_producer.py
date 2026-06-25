from __future__ import annotations

import json

import structlog
from confluent_kafka import Producer

from config import KafkaConfig

logger = structlog.get_logger()

class MatchEventProducer:
    def __init__(self, config: KafkaConfig):
        self.config = config
        self._producer = Producer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "client.id": "sports-pipeline-producer",
                "acks": "all",
                "enable.idempotence": True,
                "retries": 5,
                "retry.backoff.ms": 200,
                "linger.ms": 50,
                "compression.type": "lz4",
            }
        )
        self._sent_count = 0
        self._failed_count = 0

    def _delivery_callback(self, err, msg) -> None:
        if err is not None:
            self._failed_count += 1
            logger.error(
                "delivery_failed",
                topic=msg.topic(),
                key=msg.key(),
                error=str(err),
            )
        else:
            self._sent_count += 1
            logger.debug(
                "delivered",
                topic=msg.topic(),
                partition=msg.partition(),
                offset=msg.offset(),
            )
    
    def send_event(self, event: dict) -> None:
        key = str(event.get("match_id", "unknown"))

        try:
            payload = json.dumps(event).encode("utf-8")
        except (TypeError, ValueError) as exc:
            logger.error("serialization_failed", error=str(exc), event=event)
            self._send_to_dlq(key, event, reason=f"serialization_error: {exc}")
            return
        
        self._producer.produce(
            topic=self.config.topic,
            key=key,
            value=payload,
            callback=self._delivery_callback,
        )
        self._producer.poll(0)
    
    def _send_to_dlq(self, key: str, original_event: dict, reason: str) -> None:
        dlq_payload = {
            "original_event": original_event,
            "error_reason": reason,
        }
        try:
            self._producer.produce(
                topic=self.config.dlq_topic,
                key=key,
                value=json.dumps(dlq_payload, default=str).encode("utf-8"),
                callback=self._delivery_callback,
            )
            self._producer.poll(0)
        except Exception as exc:
            logger.critical("dlq_send_failed", error=str(exc), key=key)
    
    def flush(self, timeout: float = 10.0) -> int:
        remaining = self._producer.flush(timeout)
        if remaining > 0:
            logger.warning("flush_incomplete", remaining_messages=remaining)
        logger.info("flush_complete", sent=self._sent_count, failed=self._failed_count)
        return remaining