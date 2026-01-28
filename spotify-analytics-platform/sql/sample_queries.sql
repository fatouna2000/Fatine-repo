-- Spotify Analytics - Sample Queries
-- Useful for testing and demonstration

-- 1. Basic Counts
SELECT 'Total Users' as metric, COUNT(*) as value FROM users
UNION ALL
SELECT 'Total Artists', COUNT(*) FROM artists
UNION ALL
SELECT 'Total Tracks', COUNT(*) FROM tracks
UNION ALL
SELECT 'Total Events', COUNT(*) FROM events;

-- 2. User Demographics
SELECT 
    country,
    COUNT(*) as user_count,
    ROUND(AVG(age), 1) as avg_age,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
FROM users
GROUP BY country
ORDER BY user_count DESC;

-- 3. Subscription Analysis
SELECT 
    subscription,
    COUNT(*) as user_count,
    ROUND(AVG(age), 1) as avg_age,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as market_share
FROM users
GROUP BY subscription
ORDER BY user_count DESC;

-- 4. Daily Activity
SELECT 
    DATE(timestamp) as date,
    COUNT(*) as total_plays,
    COUNT(DISTINCT user_id) as unique_users,
    ROUND(AVG(duration_played), 1) as avg_duration
FROM events
GROUP BY DATE(timestamp)
ORDER BY date DESC
LIMIT 7;

-- 5. Top Tracks
SELECT 
    t.title,
    a.name as artist,
    COUNT(e.event_id) as play_count,
    COUNT(DISTINCT e.user_id) as unique_listeners,
    ROUND(AVG(e.duration_played), 1) as avg_duration
FROM tracks t
JOIN artists a ON t.artist_id = a.artist_id
JOIN events e ON t.track_id = e.track_id
GROUP BY t.track_id, t.title, a.name
ORDER BY play_count DESC
LIMIT 10;

-- 6. User Engagement
SELECT 
    u.user_id,
    u.subscription,
    COUNT(e.event_id) as total_plays,
    SUM(e.duration_played) as total_duration,
    COUNT(DISTINCT e.track_id) as unique_tracks,
    SUM(CASE WHEN e.event_type IN ('like', 'share') THEN 1 ELSE 0 END) as engagements
FROM users u
LEFT JOIN events e ON u.user_id = e.user_id
GROUP BY u.user_id, u.subscription
ORDER BY total_plays DESC
LIMIT 10;