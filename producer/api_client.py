from __future__ import annotations

import structlog
import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

from config import FootballAPIConfig

logger = structlog.get_logger()

class RateLimitError(Exception):
    def __init__(self, retry_after: float | None = None):
        self.retry_after = retry_after
        super().__init__(f"Rate limit exceeded, retry_after={retry_after}")

class FootballAPIError(Exception):
    """Exception for API errors"""

class FootballDataClient:
    def __init__(self, config: FootballAPIConfig):
        self.config = config
        self._client = httpx.Client(
            base_url=config.base_url,
            headers={"X-Auth-Token": config.api_key},
            timeout=config.timeout_seconds,
        )
    def __enter__(self) -> "FootballDataClient":
        return self
    
    def __exit__(self, *exec_info) -> None:
        self._client.close()
    
    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        try:
            response = self._client.get(endpoint, params=params)

            if response.status_code == 429:
                retry_after = float(response.headers.get("Retry-After", 60))
                logger.wearning(
                    "rate_limit_hit",
                    endpoint=endpoint,
                    retry_after=retry_after,
                )
                raise RateLimitError(retry_after=retry_after)
            
            if 400 <= response.status_code <500:
                logger.error(
                    "client_error",
                    endpoint=endpoint,
                    status=response.status_code,
                    body=response.text[:500],
                )
                raise FootballAPIError(
                    f"Client error {response.status_code} on {endpoint}: {response.text[:200]}"
                )
            response.raise_for_status()

            return response.json()
        except httpx.TransportError as exc:
            logger.warning("network_error", endpoint=endpoint, error=str(exc))
            raise
    
    def get_live_matches(self) -> list[dict]:
        logger.info("fetching_live_matches")
        data = self._get("/matches", params={"status": "LIVE"})
        matches = data.get("matches", [])
        logger.info("live_matches_fetched", count=len(matches))
        return matches
    
    def get_matches_by_competition(self, competition_code: str, status: str = "SCHEDULED") -> list[dict]:
        logger.info("fetching_matches", competition=competition_code, status=status)
        data = self._get(f"/competitions/{competition_code}/matches", params={"status": status})
        matches = data.get("matches", [])
        logger.info("matches_fetched", competition=competition_code, count=len(matches))
        return matches
    
    def get_match_detail(self, match_id: int) -> dict:
        logger.info("fetching_match_detail", match_id=match_id)
        return self._get(f"/matches/{match_id}")
            