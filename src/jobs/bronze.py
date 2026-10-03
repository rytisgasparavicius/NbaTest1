"""Bronze: pull standings from nba_api and append the raw payload to a Delta table."""
from _common import parse_args

from nba_analytics.ingest import fetch_standings, to_bronze_rows
from nba_analytics.spark_utils import get_spark

BRONZE_SCHEMA = (
    "season STRING, season_type STRING, endpoint STRING, "
    "ingested_at TIMESTAMP, raw_json STRING"
)


def main() -> None:
    args = parse_args()
    spark = get_spark()
    catalog = args.catalog

    for schema in ("bronze", "silver", "gold"):
        spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

    raw = fetch_standings(args.season, args.season_type)
    rows = to_bronze_rows(raw, args.season, args.season_type)
    print(f"Fetched {len(rows)} rows for {args.season} / {args.season_type}")

    (
        spark.createDataFrame(rows, schema=BRONZE_SCHEMA)
        .write.format("delta")
        .mode("append")
        .saveAsTable(f"`{catalog}`.`bronze`.`standings_raw`")
    )
    print(f"Appended to {catalog}.bronze.standings_raw")


if __name__ == "__main__":
    main()
