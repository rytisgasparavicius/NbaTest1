"""Fetch NBA league standings from nba_api and shape them for the bronze layer.

Endpoint: stats.nba.com `leaguestandingsv3` (one call -> one row per team).
"""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone

import pandas as pd

ENDPOINT = "leaguestandingsv3"


def fetch_standings(
    season: str,
    season_type: str = "Regular Season",
    retries: int = 3,
    timeout: int = 60,
) -> pd.DataFrame:
    """Call the standings endpoint and return the raw DataFrame (all columns).

    stats.nba.com is flaky and sometimes blocks cloud IP ranges, so retry with
    backoff and fail loudly instead of writing an empty table.
    """
    from nba_api.stats.endpoints import leaguestandingsv3

    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = leaguestandingsv3.LeagueStandingsV3(
                season=season, season_type=season_type, timeout=timeout
            )
            df = resp.standings.get_data_frame()
            if df.empty:
                raise ValueError(f"Endpoint returned no rows for {season} / {season_type}")
            return df
        except Exception as exc:  # network errors, timeouts, bad JSON
            last_error = exc
            if attempt < retries:
                time.sleep(2**attempt)
    raise RuntimeError(
        f"Failed to fetch {ENDPOINT} for {season} / {season_type} after {retries} attempts: {last_error}"
    ) from last_error


def to_bronze_rows(
    df: pd.DataFrame,
    season: str,
    season_type: str,
    ingested_at: datetime | None = None,
) -> list[tuple]:
    """Turn the raw frame into bronze rows: metadata + the untouched row as JSON.

    Keeping the full payload as JSON means bronze never breaks if NBA.com adds,
    removes or renames columns; silver is where we pick fields and enforce types.
    """
    ingested_at = ingested_at or datetime.now(timezone.utc)
    # df.to_json maps NaN -> null; round-trip so each row becomes its own JSON string.
    records = json.loads(df.to_json(orient="records", date_format="iso"))
    return [
        (season, season_type, ENDPOINT, ingested_at, json.dumps(rec))
        for rec in records
    ]
