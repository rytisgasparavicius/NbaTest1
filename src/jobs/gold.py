"""Gold: BI-ready tables derived from silver games."""
from _common import parse_args

from nba_analytics.spark_utils import get_spark


def main() -> None:
    args = parse_args()
    spark = get_spark()
    cat = args.catalog

    # 1) Finished games with derived columns (results table / charts).
    spark.sql(f"""
        CREATE OR REPLACE TABLE `{cat}`.`gold`.`game_results` AS
        SELECT
            game_id,
            game_date,
            season_year,
            season_type,
            game_label,
            away_team,
            home_team,
            away_score,
            home_score,
            concat(away_team_abbr, ' ', away_score, ' - ', home_score, ' ', home_team_abbr) AS score_line,
            IF(home_score > away_score, home_team, away_team)                  AS winner,
            abs(home_score - away_score)                                       AS margin,
            home_score + away_score                                            AS total_points,
            venue,
            attendance
        FROM `{cat}`.`silver`.`games`
        WHERE completed AND home_score IS NOT NULL AND away_score IS NOT NULL
    """)

    # 2) Regular-season standings per team per season, computed from results.
    spark.sql(f"""
        CREATE OR REPLACE TABLE `{cat}`.`gold`.`team_standings` AS
        WITH team_games AS (
            SELECT season_year, game_id, start_time_utc, true AS is_home,
                   home_team_id AS team_id, home_team AS team,
                   home_score AS pts_for, away_score AS pts_against
            FROM `{cat}`.`silver`.`games`
            WHERE completed AND counts_in_standings
                  AND home_score IS NOT NULL AND away_score IS NOT NULL
            UNION ALL
            SELECT season_year, game_id, start_time_utc, false,
                   away_team_id, away_team, away_score, home_score
            FROM `{cat}`.`silver`.`games`
            WHERE completed AND counts_in_standings
                  AND home_score IS NOT NULL AND away_score IS NOT NULL
        ),
        marked AS (
            SELECT *,
                pts_for > pts_against AS win,
                row_number() OVER (PARTITION BY season_year, team_id
                                   ORDER BY start_time_utc DESC) AS recency
            FROM team_games
        ),
        with_latest AS (
            SELECT *, first_value(win) OVER (PARTITION BY season_year, team_id
                                             ORDER BY recency) AS latest_win
            FROM marked
        ),
        agg AS (
            SELECT
                season_year, team_id, max_by(team, recency * -1) AS team,
                count(*)                                         AS games_played,
                count_if(win)                                    AS wins,
                count_if(NOT win)                                AS losses,
                count_if(win AND is_home)                        AS home_wins,
                count_if(NOT win AND is_home)                    AS home_losses,
                count_if(win AND NOT is_home)                    AS road_wins,
                count_if(NOT win AND NOT is_home)                AS road_losses,
                count_if(win AND recency <= 10)                  AS last10_wins,
                count_if(NOT win AND recency <= 10)              AS last10_losses,
                round(avg(pts_for), 1)                           AS points_pg,
                round(avg(pts_against), 1)                       AS opp_points_pg,
                -- length of the current run of identical results
                coalesce(min(CASE WHEN win <> latest_win THEN recency END) - 1, count(*))
                                                                 AS streak_len,
                first(latest_win)                                AS latest_win
            FROM with_latest
            GROUP BY season_year, team_id
        )
        SELECT
            season_year,
            rank() OVER (PARTITION BY season_year
                         ORDER BY wins / games_played DESC, points_pg - opp_points_pg DESC) AS league_rank,
            team_id,
            team,
            games_played,
            wins,
            losses,
            concat(wins, '-', losses)                                         AS record,
            round(wins / games_played, 3)                                     AS win_pct,
            round(home_wins / nullif(home_wins + home_losses, 0), 3)          AS home_win_pct,
            round(road_wins / nullif(road_wins + road_losses, 0), 3)          AS road_win_pct,
            points_pg,
            opp_points_pg,
            round(points_pg - opp_points_pg, 1)                               AS diff_points_pg,
            concat(last10_wins, '-', last10_losses)                           AS last10,
            concat(IF(latest_win, 'W', 'L'), streak_len)                      AS streak
        FROM agg
    """)

    for t in ("game_results", "team_standings"):
        n = spark.table(f"`{cat}`.`gold`.`{t}`").count()
        print(f"gold.{t} rows: {n}")


if __name__ == "__main__":
    main()
