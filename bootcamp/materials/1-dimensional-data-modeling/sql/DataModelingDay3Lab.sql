
-- create type vertex_type as enum
-- ('player', 'team', 'game');

-- create table vertices (
-- 	identifier text,
-- 	type vertex_type,
-- 	properties json,
-- 	primary key (identifier, type)
-- );

-- create type edge_type as enum
-- ('plays_against', 'shares_team', 'plays_in', 'plays_on');

-- CREATE TABLE edges (
--     subject_identifier TEXT,
--     subject_type vertex_type,
--     object_identifier TEXT,
--     object_type vertex_type,
--     edge_type edge_type,
--     properties JSON,
--     PRIMARY KEY (subject_identifier,
--                 subject_type,
--                 object_identifier,
--                 object_type,
--                 edge_type)
-- )

-- WITH teams_deduped AS (
--     SELECT *, ROW_NUMBER() OVER(PARTITION BY team_id) as row_num
--     FROM teams
-- )
-- SELECT
--        team_id AS identifier,
--     'team'::vertex_type AS type,
--     json_build_object(
--         'abbreviation', abbreviation,
--         'nickname', nickname,
--         'city', city,
--         'arena', arena,
--         'year_founded', yearfounded
--         )
-- FROM teams_deduped
-- WHERE row_num = 1;

insert into edges
WITH deduped AS (
    SELECT *, row_number() over (PARTITION BY player_id, game_id) AS row_num
    FROM game_details
),
     filtered AS (
         SELECT * FROM deduped
         WHERE row_num = 1
     ),
	 aggregated AS (
          SELECT
           f1.player_id AS subject_player_id,
           max(f1.player_name),
           f2.player_id AS object_player_id,
           max(f2.player_name),
           CASE WHEN f1.team_abbreviation =         f2.team_abbreviation
                THEN 'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
            END as edge_type,
            COUNT(1) AS num_games,
            SUM(coalesce(f1.pts,0)) AS subject_points,
            SUM(coalesce(f2.pts,0)) as object_points
        FROM filtered f1
            JOIN filtered f2
            ON f1.game_id = f2.game_id
            AND f1.player_name <> f2.player_name
        WHERE f1.player_id > f2.player_id --não replica a mesma informação em ordens diferentes
        GROUP BY
                f1.player_id,
           f2.player_id,
           CASE WHEN f1.team_abbreviation = f2.team_abbreviation
                THEN  'shares_team'::edge_type
            ELSE 'plays_against'::edge_type
            END
	)
	select
		subject_player_id as subject_player_identifier,
		'player'::vertex_type,
		object_player_id,
		'player'::vertex_type,
		edge_type as edge_type,
		json_build_object(
	        'num_games', num_games,
	        'subject_points', subject_points,
	        'object_points', object_points
		)
	from aggregated;


	select * from edges
