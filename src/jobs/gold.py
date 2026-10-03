"""Gold: latest standings per season, ready for BI / the dashboard."""
from _common import parse_args

from nba_analytics.spark_utils import get_spark


def main() -> None:
    args = parse_args()
    spark = get_spark()
    c = args.catalog

    spark.sql(f"""
        CREATE OR REPLACE TABLE `{c}`.`gold`.`team_standings_current` AS
        WITH latest AS (
            SELECT * FROM `{c}`.`silver`.`team_standings`
            QUALIFY snapshot_date = max(snapshot_date) OVER (PARTITION BY season, season_type)
        )
        SELECT
            season,
            season_type,
            snapshot_date,
            rank() OVER (PARTITION BY season, season_type
                         ORDER BY win_pct DESC, diff_points_pg DESC)      AS league_rank,
            conference,
            conference_rank,
            division,
            team_full_name                                                AS team,
            wins,
            losses,
            concat(wins, '-', losses)                                     AS record,
            round(win_pct, 3)                                             AS win_pct,
            round(home_wins / nullif(home_wins + home_losses, 0), 3)      AS home_win_pct,
            round(road_wins / nullif(road_wins + road_losses, 0), 3)      AS road_win_pct,
            points_pg,
            opp_points_pg,
            diff_points_pg,
            concat(last10_wins, '-', last10_losses)                       AS last10,
            current_streak_label                                          AS streak,
            conference_games_back
        FROM latest
    """)
    n = spark.table(f"`{c}`.`gold`.`team_standings_current`").count()
    print(f"gold.team_standings_current rows: {n}")


if __name__ == "__main__":
    main()
