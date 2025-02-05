
-- drop table players;
-- create table players (
-- 	player_name TEXT,
-- 	height TEXT,
-- 	college TEXT,
-- 	country TEXT,
-- 	draft_year TEXT,
-- 	draft_round TEXT,
-- 	draft_number TEXT,
-- 	season_status season_status[],
--     scoring_class scoring_class,
--     years_since_last_active INTEGER,
-- 	current_season INTEGER,
-- 	is_active boolean,
-- 	PRIMARY KEY(player_name, current_season)
-- );

-- INSERT INTO players
-- WITH years AS (
--     SELECT *
--     FROM GENERATE_SERIES(1996, 2022) AS season
-- ), p AS (
--     SELECT
--         player_name,
--         MIN(season) AS first_season
--     FROM player_seasons
--     GROUP BY player_name
-- ), players_and_seasons AS (
--     SELECT *
--     FROM p
--     JOIN years y
--         ON p.first_season <= y.season
-- ), windowed AS (
--     SELECT
--         pas.player_name,
--         pas.season,
--         ARRAY_REMOVE(
--             ARRAY_AGG(
--                 CASE
--                     WHEN ps.season IS NOT NULL
--                         THEN ROW(
--                             ps.season,
--                             ps.gp,
--                             ps.pts,
--                             ps.reb,
--                             ps.ast
--                         )::season_status
--                 END)
--             OVER (PARTITION BY pas.player_name ORDER BY COALESCE(pas.season, ps.season)),
--             NULL
--         ) AS seasons
--     FROM players_and_seasons pas
--     LEFT JOIN player_seasons ps
--         ON pas.player_name = ps.player_name
--         AND pas.season = ps.season
--     ORDER BY pas.player_name, pas.season
-- ), static AS (
--     SELECT
--         player_name,
--         MAX(height) AS height,
--         MAX(college) AS college,
--         MAX(country) AS country,
--         MAX(draft_year) AS draft_year,
--         MAX(draft_round) AS draft_round,
--         MAX(draft_number) AS draft_number
--     FROM player_seasons
--     GROUP BY player_name
-- )
-- SELECT
--     w.player_name,
--     s.height,
--     s.college,
--     s.country,
--     s.draft_year,
--     s.draft_round,
--     s.draft_number,
--     seasons AS season_status,
--     CASE
--         WHEN (seasons[CARDINALITY(seasons)]::season_status).pts > 20 THEN 'star'
--         WHEN (seasons[CARDINALITY(seasons)]::season_status).pts > 15 THEN 'good'
--         WHEN (seasons[CARDINALITY(seasons)]::season_status).pts > 10 THEN 'average'
--         ELSE 'bad'
--     END::scoring_class AS scoring_class,
--     w.season - (seasons[CARDINALITY(seasons)]::season_status).season as years_since_last_active,
--     w.season,
--     (seasons[CARDINALITY(seasons)]::season_status).season = season AS is_active
-- FROM windowed w
-- JOIN static s
--     ON w.player_name = s.player_name;

-- insert into players_scd
-- with with_previous as (
-- 	select
-- 		player_name,
-- 		current_season,
-- 		scoring_class,
-- 		is_active,
-- 		LAG(scoring_class, 1) OVER (partition by player_name order by current_season) as previous_scoring_class,
-- 		LAG(is_active, 1) OVER (partition by player_name order by current_season) as previous_is_active
-- 	from players
-- 	where current_season <= 2021
-- ),

-- with_indicator as (
-- 	select *,
-- 	case
-- 		when scoring_class <> previous_scoring_class THEN 1
-- 		when is_active <> previous_is_active THEN 1
-- 		else 0
-- 	end as change_indicator
-- 	from with_previous
-- ),

-- with_streaks as (
-- 	select *,
-- 	sum(change_indicator)
-- 		over (partition by player_name order by current_season) as streak_indicator
-- 	from with_indicator
-- )

-- select player_name,
-- 	scoring_class,
-- 	is_active,
-- 	--streak_indicator,
-- 	min(current_season) as start_season,
-- 	max(current_season) as end_season,
-- 	'2021'::int4 as current_season
-- from with_streaks
-- group by 1,2,3
-- order by 1,2;

-- create table players_scd
-- (
-- 	player_name text,
-- 	scoring_class scoring_class,
-- 	is_active boolean,
-- 	start_season integer,
-- 	end_date integer,
-- 	current_season INTEGER
-- );

-- CREATE TYPE scd_type AS (
--                     scoring_class scoring_class,
--                     is_active boolean,
--                     start_season INTEGER,
--                     end_season INTEGER
--                         );


WITH last_season_scd AS (
    SELECT * FROM players_scd
    WHERE current_season = 2021
    AND end_date = 2021
),
     historical_scd AS (
        SELECT
            player_name,
               scoring_class,
               is_active,
               start_season,
               end_date
        FROM players_scd
        WHERE current_season = 2021
        AND end_date < 2021
     ),
     this_season_data AS (
         SELECT * FROM players
         WHERE current_season = 2022
     ),
     unchanged_records AS (
         SELECT
                ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ls.start_season,
                ts.current_season as end_date
        FROM this_season_data ts
        JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
         WHERE ts.scoring_class = ls.scoring_class
         AND ts.is_active = ls.is_active
     ),
     changed_records AS (
        SELECT
                ts.player_name,
                UNNEST(ARRAY[
                    ROW(
                        ls.scoring_class,
                        ls.is_active,
                        ls.start_season,
                        ls.end_date

                        )::scd_type,
                    ROW(
                        ts.scoring_class,
                        ts.is_active,
                        ts.current_season,
                        ts.current_season
                        )::scd_type
                ]) as records
        FROM this_season_data ts
        LEFT JOIN last_season_scd ls
        ON ls.player_name = ts.player_name
         WHERE (ts.scoring_class <> ls.scoring_class
          OR ts.is_active <> ls.is_active)
     ),
     unnested_changed_records AS (

         SELECT player_name,
                (records::scd_type).scoring_class,
                (records::scd_type).is_active,
                (records::scd_type).start_season,
                (records::scd_type).end_season
                FROM changed_records
         ),
     new_records AS (

         SELECT
            ts.player_name,
                ts.scoring_class,
                ts.is_active,
                ts.current_season AS start_season,
                ts.current_season AS end_date
         FROM this_season_data ts
         LEFT JOIN last_season_scd ls
             ON ts.player_name = ls.player_name
         WHERE ls.player_name IS NULL

     )


SELECT *, 2022 AS current_season FROM (
                  SELECT *
                  FROM historical_scd

                  UNION ALL

                  SELECT *
                  FROM unchanged_records

                  UNION ALL

                  SELECT *
                  FROM unnested_changed_records

                  UNION ALL

                  SELECT *
                  FROM new_records
              ) a







