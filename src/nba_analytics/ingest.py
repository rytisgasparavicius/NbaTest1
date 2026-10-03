"""Fetch NBA games from ESPN's public scoreboard API and shape them for bronze.

stats.nba.com / cdn.nba.com block cloud IP ranges (incl. Databricks on Azure),
so the pipeline uses ESPN's unofficial site API instead. It is undocumented and
may change without notice.
"""
from __future__ import annotations

import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone

import requests

ENDPOINT = "espn_scoreboard"
SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard"


def fetch_scoreboard(day: date, retries: int = 3, timeout: int = 20) -> list[dict]:
    """Return the raw ESPN `events` (games) for one calendar day (US dates)."""
    last_error: Exception | None = None
    for attempt in range(1, retries + 1):
        try:
            resp = requests.get(
                SCOREBOARD_URL, params={"dates": day.strftime("%Y%m%d")}, timeout=timeout
            )
            resp.raise_for_status()
            return resp.json().get("events", [])
        except Exception as exc:  # network errors, timeouts, bad JSON, HTTP errors
            last_error = exc
            if attempt < retries:
                time.sleep(attempt)
    raise RuntimeError(
        f"Failed to fetch ESPN scoreboard for {day} after {retries} attempts: {last_error}"
    ) from last_error


def fetch_range(start: date, end: date, max_workers: int = 6) -> dict[date, list[dict]]:
    """Fetch every day in [start, end]. Fails fast if ESPN is unreachable."""
    days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
    # Probe one day first so a blocked network fails in seconds, not minutes.
    result = {days[0]: fetch_scoreboard(days[0])}
    rest = days[1:]
    if not rest:
        return result

    pool = ThreadPoolExecutor(max_workers=max_workers)
    try:
        futures = {pool.submit(fetch_scoreboard, d): d for d in rest}
        for fut in as_completed(futures):
            result[futures[fut]] = fut.result()
    finally:
        pool.shutdown(wait=False, cancel_futures=True)
    return result


def to_bronze_rows(
    day: date, events: list[dict], ingested_at: datetime | None = None
) -> list[tuple]:
    """One bronze row per game: metadata + the untouched ESPN event as JSON.

    Keeping the full payload as JSON means bronze never breaks when ESPN adds
    or renames fields; silver is where we pick fields and enforce types.
    """
    ingested_at = ingested_at or datetime.now(timezone.utc)
    return [(day, ENDPOINT, str(ev["id"]), ingested_at, json.dumps(ev)) for ev in events]
