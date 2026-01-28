-- Analytical Views for Spotify Analytics

-- View: User Analytics
CREATE OR REPLACE VIEW user_analytics AS
SELECT 
    u.user_id,
    u.username,
    u.country,
    u.age,
    u.subscription,
    COUNT(e.event_id) as total_plays,
    SUM(e.duration_played) as total_duration,
    COUNT(DISTINCT e.track_id) as unique_tracks,
    COUNT(DISTINCT DATE(e.timestamp)) as active_days,
    SUM(CASE WHEN e.event_type IN ('like', 'share', 'add_to_playlist') THEN 1 ELSE 0 END) as engagements,
    MAX(e.timestamp) as last_played,
    ROUND(100.0 * 
        SUM(CASE WHEN e.event_type IN ('like', 'share', 'add_to_playlist') THEN 1 ELSE 0 END) 
        / NULLIF(COUNT(e.event_id), 0), 2) as engagement_rate
FROM users u
LEFT JOIN events e ON u.user_id = e.user_id
GROUP BY u.user_id, u.username, u.country, u.age, u.subscription;

COMMENT ON VIEW user_analytics IS 'Comprehensive user engagement metrics';

-- View: Daily Metrics
CREATE OR REPLACE VIEW daily_metrics AS
SELECT 
    DATE(timestamp) as date,
    COUNT(*) as total_plays,
    COUNT(DISTINCT user_id) as unique_users,
    SUM(duration_played) as total_duration,
    ROUND(AVG(duration_played), 1) as avg_duration,
    COUNT(DISTINCT track_id) as unique_tracks,
    SUM(CASE WHEN event_type IN ('like', 'share', 'add_to_playlist') THEN 1 ELSE 0 END) as engagements,
    ROUND(100.0 * 
        SUM(CASE WHEN event_type IN ('like', 'share', 'add_to_playlist') THEN 1 ELSE 0 END) 
        / NULLIF(COUNT(*), 0), 2) as engagement_rate
FROM events
GROUP BY DATE(timestamp)
ORDER BY date;

COMMENT ON VIEW daily_metrics IS 'Daily aggregated activity metrics';

-- View: Top Artists
CREATE OR REPLACE VIEW top_artists AS
SELECT 
    a.artist_id,
    a.name,
    a.genre,
    a.popularity,
    COUNT(e.event_id) as total_plays,
    COUNT(DISTINCT e.user_id) as unique_listeners,
    ROUND(AVG(e.duration_played), 1) as avg_listen_duration,
    COUNT(DISTINCT t.track_id) as track_count
FROM artists a
JOIN tracks t ON a.artist_id = t.artist_id
JOIN events e ON t.track_id = e.track_id
GROUP BY a.artist_id, a.name, a.genre, a.popularity
ORDER BY total_plays DESC;

COMMENT ON VIEW top_artists IS 'Artist performance and popularity metrics';

SELECT '✅ Analytical views created successfully!' as message;