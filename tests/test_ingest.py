import json
from datetime import date, datetime, timezone

import pytest

from nba_analytics import ingest
from nba_analytics.ingest import ENDPOINT, fetch_range, fetch_scoreboard, to_bronze_rows


def test_to_bronze_rows_keeps_whole_event_as_json():
    ts = datetime(2026, 10, 3, tzinfo=timezone.utc)
    events = [
        {"id": "401810960", "shortName": "PHI @ WSH", "competitions": [{"x": 1}]},
        {"id": 401810961, "shortName": "A @ B"},
    ]

    rows = to_bronze_rows(date(2026, 4, 1), events, ingested_at=ts)

    assert len(rows) == 2
    game_date, endpoint, event_id, ingested_at, raw = rows[0]
    assert (game_date, endpoint, event_id, ingested_at) == (
        date(2026, 4, 1),
        ENDPOINT,
        "401810960",
        ts,
    )
    assert json.loads(raw) == events[0]
    assert rows[1][2] == "401810961"  # ids are always stored as strings


class _Resp:
    def __init__(self, payload, ok=True):
        self._payload, self._ok = payload, ok

    def raise_for_status(self):
        if not self._ok:
            raise RuntimeError("HTTP 403")

    def json(self):
        return self._payload


def test_fetch_scoreboard_passes_date_and_returns_events(monkeypatch):
    seen = {}

    def fake_get(url, params, timeout):
        seen.update(url=url, params=params)
        return _Resp({"events": [{"id": "1"}]})

    monkeypatch.setattr(ingest.requests, "get", fake_get)

    assert fetch_scoreboard(date(2026, 4, 1)) == [{"id": "1"}]
    assert seen["params"] == {"dates": "20260401"}


def test_fetch_scoreboard_fails_loudly_after_retries(monkeypatch):
    monkeypatch.setattr(ingest.requests, "get", lambda *a, **k: _Resp({}, ok=False))
    monkeypatch.setattr(ingest.time, "sleep", lambda s: None)

    with pytest.raises(RuntimeError, match="after 3 attempts"):
        fetch_scoreboard(date(2026, 4, 1))


def test_fetch_range_covers_every_day(monkeypatch):
    monkeypatch.setattr(ingest, "fetch_scoreboard", lambda d: [{"id": d.isoformat()}])

    out = fetch_range(date(2026, 4, 1), date(2026, 4, 5))

    assert sorted(out) == [date(2026, 4, d) for d in range(1, 6)]
    assert out[date(2026, 4, 3)] == [{"id": "2026-04-03"}]
