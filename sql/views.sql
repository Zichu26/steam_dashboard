-- SQL Views for Steam Dashboard
-- Run these in Snowflake after tables are populated

-- ============================================================
-- GAMES_BY_TAG View
-- Extracts tags from GAME_INFO and creates a flattened view
-- for the "Popular games all-time by tag" dashboard component
-- ============================================================

CREATE OR REPLACE VIEW STEAM_ANALYTICS.RAW.GAMES_BY_TAG AS
WITH tag_data AS (
    SELECT
        appid,
        name,
        owners,
        price,
        ccu,
        positive,
        negative,
        genre,
        PARSE_JSON(tags) as tags_json
    FROM STEAM_ANALYTICS.RAW.GAME_INFO
    WHERE tags IS NOT NULL
),
flattened_tags AS (
    SELECT
        t.appid,
        t.name,
        t.owners,
        t.price,
        t.ccu,
        t.positive,
        t.negative,
        t.genre,
        f.key AS tag_name,
        f.value::INTEGER AS tag_votes
    FROM tag_data t,
    LATERAL FLATTEN(input => t.tags_json) f
)
SELECT
    tag_name,
    appid,
    name AS game_name,
    owners,
    CASE
        WHEN price IS NULL OR price = '' THEN 0
        ELSE TRY_CAST(price AS INTEGER)
    END AS price_cents,
    ccu AS current_players,
    positive AS positive_reviews,
    negative AS negative_reviews,
    genre,
    tag_votes
FROM flattened_tags
ORDER BY tag_name, tag_votes DESC;

-- ============================================================
-- LIVE_LEADERBOARD View
-- Real-time game leaderboard sorted by concurrent users
-- ============================================================

CREATE OR REPLACE VIEW STEAM_ANALYTICS.RAW.LIVE_LEADERBOARD AS
SELECT
    appid AS game_id,
    name AS game_name,
    ccu AS player_count,
    CASE
        WHEN price IS NULL OR price = '' OR price = '0' THEN 'Free'
        ELSE CONCAT('$', ROUND(TRY_CAST(price AS FLOAT) / 100, 2))
    END AS current_price,
    owners AS total_downloads,
    positive AS positive_reviews,
    negative AS negative_reviews,
    ROUND(positive * 100.0 / NULLIF(positive + negative, 0), 1) AS review_score_pct,
    genre,
    developer,
    publisher,
    fetched_at,
    updated_at
FROM STEAM_ANALYTICS.RAW.GAME_INFO
WHERE name IS NOT NULL
ORDER BY ccu DESC NULLS LAST;

-- ============================================================
-- TOP_GAMES_BY_TAG View
-- Aggregated view showing top games per tag
-- ============================================================

CREATE OR REPLACE VIEW STEAM_ANALYTICS.RAW.TOP_GAMES_BY_TAG AS
WITH ranked_games AS (
    SELECT
        tag_name,
        game_name,
        appid,
        owners,
        price_cents,
        current_players,
        ROW_NUMBER() OVER (PARTITION BY tag_name ORDER BY current_players DESC NULLS LAST) as rank_in_tag
    FROM STEAM_ANALYTICS.RAW.GAMES_BY_TAG
)
SELECT
    tag_name,
    game_name,
    appid,
    owners,
    price_cents,
    current_players,
    rank_in_tag
FROM ranked_games
WHERE rank_in_tag <= 100;  -- Top 100 games per tag

-- ============================================================
-- USER_GAME_SUMMARY View
-- Summarizes player's game history from PLAYER_STATS
-- ============================================================

CREATE OR REPLACE VIEW STEAM_ANALYTICS.RAW.USER_GAME_SUMMARY AS
SELECT
    ps.steamid,
    p.personaname,
    ps.appid,
    ps.name AS game_name,
    gi.genre AS game_tag,
    ps.playtime_forever AS hrs_played,
    ps.playtime_2weeks AS recent_hrs,
    ps.rtime_last_played AS last_played,
    gi.positive,
    gi.negative
FROM STEAM_ANALYTICS.RAW.PLAYER_STATS ps
LEFT JOIN STEAM_ANALYTICS.RAW.PLAYERS p ON ps.steamid = p.steamid
LEFT JOIN STEAM_ANALYTICS.RAW.GAME_INFO gi ON ps.appid = gi.appid
WHERE ps.name IS NOT NULL;

-- ============================================================
-- GAME_SENTIMENT_SUMMARY View
-- Aggregates review sentiment for games
-- ============================================================

CREATE OR REPLACE VIEW STEAM_ANALYTICS.RAW.GAME_SENTIMENT_SUMMARY AS
SELECT
    appid,
    game_name,
    COUNT(*) as total_reviews,
    SUM(CASE WHEN voted_up THEN 1 ELSE 0 END) as positive_count,
    SUM(CASE WHEN NOT voted_up THEN 1 ELSE 0 END) as negative_count,
    ROUND(SUM(CASE WHEN voted_up THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 1) as positive_pct,
    AVG(playtime_at_review) as avg_playtime_at_review,
    MAX(timestamp_created) as latest_review_date
FROM STEAM_ANALYTICS.RAW.GAME_REVIEWS
GROUP BY appid, game_name;

-- ============================================================
-- GAME_PLAYER_COUNTS_LIVE View
-- Shows latest (live) player count + average for each game
-- THIS IS THE MAIN REAL-TIME DASHBOARD VIEW
-- ============================================================

CREATE OR REPLACE VIEW STEAM_ANALYTICS.RAW.GAME_PLAYER_COUNTS_LIVE AS
WITH latest_counts AS (
    -- Get the most recent player count for each game
    SELECT
        appid,
        game_name,
        live_player_count,
        fetched_at,
        ROW_NUMBER() OVER (PARTITION BY appid ORDER BY fetched_at DESC) as rn
    FROM STEAM_ANALYTICS.RAW.GAME_PLAYER_COUNTS
),
avg_counts AS (
    -- Calculate average player count over all time
    SELECT
        appid,
        AVG(live_player_count) as avg_player_count_alltime,
        COUNT(*) as sample_count
    FROM STEAM_ANALYTICS.RAW.GAME_PLAYER_COUNTS
    GROUP BY appid
),
avg_24h AS (
    -- Calculate average player count over last 24 hours
    SELECT
        appid,
        AVG(live_player_count) as avg_player_count_24h
    FROM STEAM_ANALYTICS.RAW.GAME_PLAYER_COUNTS
    WHERE fetched_at >= DATEADD(hour, -24, CURRENT_TIMESTAMP())
    GROUP BY appid
),
peak_counts AS (
    -- Get peak player count
    SELECT
        appid,
        MAX(live_player_count) as peak_player_count
    FROM STEAM_ANALYTICS.RAW.GAME_PLAYER_COUNTS
    GROUP BY appid
)
SELECT
    lc.appid AS game_id,
    lc.game_name,
    lc.live_player_count AS current_players,
    ROUND(ac.avg_player_count_alltime, 0) AS avg_players_alltime,
    ROUND(a24.avg_player_count_24h, 0) AS avg_players_24h,
    pc.peak_player_count AS peak_players,
    lc.fetched_at AS last_updated,
    ac.sample_count AS data_points,
    -- Calculate trend (current vs 24h avg)
    CASE
        WHEN a24.avg_player_count_24h IS NULL THEN 'N/A'
        WHEN lc.live_player_count > a24.avg_player_count_24h * 1.1 THEN 'UP'
        WHEN lc.live_player_count < a24.avg_player_count_24h * 0.9 THEN 'DOWN'
        ELSE 'STABLE'
    END AS trend
FROM latest_counts lc
LEFT JOIN avg_counts ac ON lc.appid = ac.appid
LEFT JOIN avg_24h a24 ON lc.appid = a24.appid
LEFT JOIN peak_counts pc ON lc.appid = pc.appid
WHERE lc.rn = 1
ORDER BY lc.live_player_count DESC;

-- ============================================================
-- GAME_PLAYER_COUNTS_TIMESERIES View
-- Time series data for charting player counts over time
-- ============================================================

CREATE OR REPLACE VIEW STEAM_ANALYTICS.RAW.GAME_PLAYER_COUNTS_TIMESERIES AS
SELECT
    appid AS game_id,
    game_name,
    live_player_count AS player_count,
    fetched_at AS timestamp,
    DATE_TRUNC('hour', fetched_at) AS hour_bucket
FROM STEAM_ANALYTICS.RAW.GAME_PLAYER_COUNTS
ORDER BY appid, fetched_at DESC;
