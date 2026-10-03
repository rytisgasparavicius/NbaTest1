import json
from datetime import datetime, timezone

import numpy as np
import pandas as pd

from nba_analytics.ingest import ENDPOINT, to_bronze_rows


def test_to_bronze_rows_keeps_every_column_and_nulls_nan():
    df = pd.DataFrame(
        {"TeamID": [1, 2], "TeamName": ["Celtics", "Knicks"], "LeagueRank": [np.nan, 3.0]}
    )
    ts = datetime(2026, 10, 3, tzinfo=timezone.utc)

    rows = to_bronze_rows(df, "2025-26", "Regular Season", ingested_at=ts)

    assert len(rows) == 2
    season, season_type, endpoint, ingested_at, raw = rows[0]
    assert (season, season_type, endpoint, ingested_at) == (
        "2025-26",
        "Regular Season",
        ENDPOINT,
        ts,
    )
    payload = json.loads(raw)
    assert payload == {"TeamID": 1, "TeamName": "Celtics", "LeagueRank": None}
