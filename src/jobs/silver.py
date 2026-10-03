"""Silver: parse bronze JSON, pick/rename/cast columns, de-duplicate snapshots."""
from _common import parse_args

from nba_analytics.spark_utils import get_spark

# Only the fields we need; everything else stays available in bronze raw_json.
JSON_SCHEMA = (
    "TeamID BIGINT, TeamCity STRING, TeamName STRING, TeamSlug STRING, "
    "Conference STRING, Division STRING, PlayoffRank INT, DivisionRank INT, "
    "WINS INT, LOSSES INT, WinPCT DOUBLE, HOME STRING, ROAD STRING, L10 STRING, "
    "CurrentStreak INT, strCurrentStreak STRING, "
    "PointsPG DOUBLE, OppPointsPG DOUBLE, DiffPointsPG DOUBLE, "
    "ConferenceGamesBack DOUBLE"
)


def record_part(col: str, idx: int) -> str:
    """'34-7' -> wins (idx 0) or losses (idx 1) as INT."""
    return f"try_cast(split(j.{col}, '-')[{idx}] AS INT)"


def main() -> None:
    args = parse_args()
    spark = get_spark()
    c = args.catalog

    spark.sql(f"""
        CREATE OR REPLACE TABLE `{c}`.`silver`.`team_standings` AS
        WITH parsed AS (
            SELECT season, season_type, ingested_at,
                   CAST(ingested_at AS DATE) AS snapshot_date,
                   from_json(raw_json, '{JSON_SCHEMA}') AS j
            FROM `{c}`.`bronze`.`standings_raw`
        ),
        flat AS (
            SELECT
                season,
                season_type,
                snapshot_date,
                ingested_at,
                j.TeamID                                   AS team_id,
                j.TeamCity                                 AS team_city,
                j.TeamName                                 AS team_name,
                concat_ws(' ', j.TeamCity, j.TeamName)     AS team_full_name,
                j.TeamSlug                                 AS team_slug,
                j.Conference                               AS conference,
                j.Division                                 AS division,
                j.PlayoffRank                              AS conference_rank,
                j.DivisionRank                             AS division_rank,
                j.WINS                                     AS wins,
                j.LOSSES                                   AS losses,
                j.WINS + j.LOSSES                          AS games_played,
                j.WinPCT                                   AS win_pct,
                {record_part('HOME', 0)}                   AS home_wins,
                {record_part('HOME', 1)}                   AS home_losses,
                {record_part('ROAD', 0)}                   AS road_wins,
                {record_part('ROAD', 1)}                   AS road_losses,
                {record_part('L10', 0)}                    AS last10_wins,
                {record_part('L10', 1)}                    AS last10_losses,
                j.CurrentStreak                            AS current_streak,
                j.strCurrentStreak                         AS current_streak_label,
                j.PointsPG                                 AS points_pg,
                j.OppPointsPG                              AS opp_points_pg,
                j.DiffPointsPG                             AS diff_points_pg,
                j.ConferenceGamesBack                      AS conference_games_back
            FROM parsed
            WHERE j.TeamID IS NOT NULL
        )
        SELECT * FROM flat
        -- one row per team per day: keep the latest ingestion if the job ran twice
        QUALIFY row_number() OVER (
            PARTITION BY season, season_type, team_id, snapshot_date
            ORDER BY ingested_at DESC
        ) = 1
    """)
    n = spark.table(f"`{c}`.`silver`.`team_standings`").count()
    print(f"silver.team_standings rows: {n}")


if __name__ == "__main__":
    main()
