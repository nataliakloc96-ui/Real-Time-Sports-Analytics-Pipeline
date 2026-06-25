from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def match_to_event(match: dict[str, Any]) -> dict[str, Any]:
    return {
        "event_id": f"{match['id']}-{match.get('lastUpdated', '')}",
        "match_id": match["id"],
        "competition_code": match.get("competition", {}).get("code"),
        "status": match.get("status"),
        "utc_date": match.get("utcDate"),
        "matchday": match.get("matchday"),
        "home_team_id": match.get("homeTeam", {}).get("id"),
        "home_team_name": match.get("homeTeam", {}).get("name"),
        "away_team_id": match.get("awayTeam", {}).get("id"),
        "away_team_name": match.get("awayTeam", {}).get("name"),
        "score_home": match.get("score", {}).get("fullTime", {}).get("home"),
        "score_away": match.get("score", {}).get("fullTime", {}).get("away"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
    }