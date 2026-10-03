"""Silver: parse bronze JSON into one typed row per game."""
from _common import parse_args

from nba_analytics.spark_utils import get_spark

# Only the fields we need; everything else stays available in bronze raw_json.
JSON_SCHEMA = """
    id STRING,
    date STRING,
    season STRUCT<year: INT, type: INT, slug: STRING>,
    status STRUCT<type: STRUCT<name: STRING, state: STRING, completed: BOOLEAN>>,
    competitions ARRAY<STRUCT<
        attendance: INT,
        neutralSite: BOOLEAN,
        notes: ARRAY<STRUCT<headline: STRING>>,
        venue: STRUCT<fullName: STRING, address: STRUCT<city: STRING, state: STRING>>,
        competitors: ARRAY<STRUCT<
            homeAway: STRING,
            score: STRING,
            winner: BOOLEAN,
            team: STRUCT<id: STRING, abbreviation: STRING, displayName: STRING>
        >>
    >>
"""


def main() -> None:
    args = parse_args()
    spark = get_spark()
    cat = args.catalog
    schema = " ".join(JSON_SCHEMA.split())

    # Timestamps below are parsed as UTC; pin the session so that is guaranteed.
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    spark.sql(f"""
        CREATE OR REPLACE TABLE `{cat}`.`silver`.`games` AS
        WITH parsed AS (
            SELECT ingested_at, from_json(raw_json, '{schema}') AS j
            FROM `{cat}`.`bronze`.`scoreboard_raw`
        ),
        flat AS (
            SELECT
                j.id                                                            AS game_id,
                to_timestamp(regexp_replace(j.date, 'Z$', ''), "yyyy-MM-dd'T'HH:mm")
                                                                                AS start_time_utc,
                j.season.year                                                   AS season_year,
                j.season.type                                                   AS season_type_id,
                j.status.type.name                                              AS status_name,
                j.status.type.state                                             AS status_state,
                coalesce(j.status.type.completed, false)                        AS completed,
                try_element_at(j.competitions, 1)                               AS comp,
                ingested_at
            FROM parsed
            WHERE j.id IS NOT NULL
        ),
        sides AS (
            SELECT *,
                try_element_at(comp.notes, 1).headline AS game_label,
                try_element_at(filter(comp.competitors, x -> x.homeAway = 'home'), 1) AS home,
                try_element_at(filter(comp.competitors, x -> x.homeAway = 'away'), 1) AS away
            FROM flat
        )
        SELECT
            game_id,
            start_time_utc,
            to_date(from_utc_timestamp(start_time_utc, 'America/New_York'))     AS game_date,
            season_year,
            CASE season_type_id WHEN 1 THEN 'Preseason'
                                WHEN 2 THEN 'Regular Season'
                                WHEN 3 THEN 'Playoffs'
                                WHEN 5 THEN 'Play-In'
                                ELSE 'Other' END                                AS season_type,
            game_label,
            -- ESPN files All-Star games and the NBA Cup final under "Regular Season",
            -- but they do not count in the official standings.
            (season_type_id = 2
                AND NOT coalesce(game_label LIKE 'NBA All-Star%', false)
                AND NOT coalesce(game_label = 'NBA Cup Championship', false)
            )                                                                   AS counts_in_standings,
            status_name,
            status_state,
            completed,
            home.team.id                                                        AS home_team_id,
            home.team.abbreviation                                              AS home_team_abbr,
            home.team.displayName                                               AS home_team,
            try_cast(home.score AS INT)                                         AS home_score,
            away.team.id                                                        AS away_team_id,
            away.team.abbreviation                                              AS away_team_abbr,
            away.team.displayName                                               AS away_team,
            try_cast(away.score AS INT)                                         AS away_score,
            comp.venue.fullName                                                 AS venue,
            comp.venue.address.city                                             AS venue_city,
            comp.attendance                                                     AS attendance,
            comp.neutralSite                                                    AS neutral_site,
            ingested_at
        FROM sides
        WHERE home.team.id IS NOT NULL AND away.team.id IS NOT NULL
        -- the same game is fetched on several runs: keep the latest snapshot
        QUALIFY row_number() OVER (PARTITION BY game_id ORDER BY ingested_at DESC) = 1
    """)
    n = spark.table(f"`{cat}`.`silver`.`games`").count()
    print(f"silver.games rows: {n}")


if __name__ == "__main__":
    main()
