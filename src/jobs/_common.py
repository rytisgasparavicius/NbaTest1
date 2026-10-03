"""Shared bootstrap for the job entry points (bronze / silver / gold)."""
import argparse
import sys
from pathlib import Path

# Job files are uploaded as plain scripts, so make the `nba_analytics` package importable.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--catalog", required=True, help="Unity Catalog catalog to write to")
    p.add_argument(
        "--days-back",
        type=int,
        default=2,
        help="bronze only: fetch games from this many days ago up to today",
    )
    return p.parse_args()
