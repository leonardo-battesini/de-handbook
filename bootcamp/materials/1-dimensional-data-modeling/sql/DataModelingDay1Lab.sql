-- select * from player_seasons limit 100;

-- create type season_status as (
-- 	season INTEGER,
-- 	gp INTEGER,
-- 	pts REAL,
-- 	reb REAL,
-- 	ast REAL
-- );

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
-- 	PRIMARY KEY(player_name, current_season)
-- );

-- insert into players
-- with yesterday as (
-- 	select * from players
-- 	where current_season = 2000
-- ),
-- 	today as (
-- 		select * from player_seasons
-- 	where season = 2001
-- 	)

-- 	select
-- 	coalesce(t.player_name, y.player_name) as player_name,
-- 	coalesce(t.height, y.height) as height,
-- 	coalesce(t.college, y.college) as college,
-- 	coalesce(t.country, y.country) as country,
-- 	coalesce(t.draft_year, y.draft_year) as draft_year,
-- 	coalesce(t.draft_round, y.draft_round) as draft_round,
-- 	coalesce(t.draft_number, y.draft_number) as draft_number,
-- 	case when y.season_status is null
-- 		then array[row(
-- 		t.season,
-- 		t.gp,
-- 		t.pts,
-- 		t.reb,
-- 		t.ast)::season_status]
-- 		when t.season is not null then y.season_status || array[row(
-- 		t.season,
-- 		t.gp,
-- 		t.pts,
-- 		t.reb,
-- 		t.ast)::season_status]
-- 		else y.season_status
-- 	end as season_status,
-- 	CASE
-- 		WHEN t.season IS NOT NULL THEN
-- 		(CASE WHEN t.pts > 20 THEN 'star'
-- 		WHEN t.pts > 15 THEN 'good'
-- 		WHEN t.pts > 10 THEN 'average'
-- 		ELSE 'bad' END)::scoring_class
-- 		ELSE y.scoring_class
-- 	END as scoring_class,
-- 	case when t.season is not null then 0
-- 		else y.years_since_last_active + 1
-- 	end as years_since_last_active,
-- 	coalesce(t.season, y.current_season + 1) as current_season
-- 	from today t FULL OUTER JOIN yesterday y
-- 		on t.player_name = y.player_name;


with unnested as (
select *,
	unnest(season_status) as season_status
from players 
where current_season = 2001
and player_name = 'Michael Jordan'
)
select player_name,
	(season_status::season_status).*
from unnested;

-- CREATE TYPE scoring_class AS
--     ENUM ('bad', 'average', 'good', 'star');