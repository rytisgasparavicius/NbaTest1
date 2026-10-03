"""Bronze: pull games from ESPN's scoreboard and append the raw payload to a Delta table."""
from datetime import date, timedelta

from _common import parse_args

from nba_analytics.ingest import fetch_range, to_bronze_rows
from nba_analytics.spark_utils import get_spark

BRONZE_SCHEMA = (
    "game_date DATE, endpoint STRING, event_id STRING, "
    "ingested_at TIMESTAMP, raw_json STRING"
)


def main() -> None:
    args = parse_args()
    spark = get_spark()
    catalog = args.catalog

    for schema in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    end = date.today()
    start = end - timedelta(days=args.days_back)
    by_day = fetch_range(start, end)

    rows = [row for day, events in sorted(by_day.items()) for row in to_bronze_rows(day, events)]
    print(f"Fetched {len(rows)} games for {start} .. {end}")
    if not rows:
        print("No games in this window; nothing to write.")
        return

    (
        spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
        .write.format("delta")
        .mode("append")
        .saveAsTable(f"`{catalog}`.`bronze`.`scoreboard_raw`")
    )
    print(f"Appended to {catalog}.bronze.scoreboard_raw")


if __name__ == "__main__":
    main()
