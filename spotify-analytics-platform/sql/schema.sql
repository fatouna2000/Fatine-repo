-- Spotify Analytics Database Schema
-- PostgreSQL 14+

-- Database creation
CREATE DATABASE spotify_analytics;

-- Connect to the database
-- \c spotify_analytics;

-- Table: users
CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(50) PRIMARY KEY,
    username VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    country CHAR(2),
    age INTEGER CHECK (age >= 13 AND age <= 100),
    subscription VARCHAR(20) CHECK (subscription IN ('free', 'premium', 'family', 'student')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_active TIMESTAMP
);

COMMENT ON TABLE users IS 'Spotify users with demographic information';
COMMENT ON COLUMN users.country IS 'ISO 3166-1 alpha-2 country code';

-- Table: artists
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    genre VARCHAR(100),
    followers INTEGER DEFAULT 0,
    popularity INTEGER CHECK (popularity >= 0 AND popularity <= 100),
    monthly_listeners INTEGER DEFAULT 0
);

-- Table: tracks
CREATE TABLE IF NOT EXISTS tracks (
    track_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    artist_id VARCHAR(50) REFERENCES artists(artist_id),
    album VARCHAR(255),
    duration_ms INTEGER CHECK (duration_ms > 0),
    explicit BOOLEAN DEFAULT FALSE,
    popularity INTEGER CHECK (popularity >= 0 AND popularity <= 100),
    danceability FLOAT CHECK (danceability >= 0 AND danceability <= 1),
    energy FLOAT CHECK (energy >= 0 AND energy <= 1),
    valence FLOAT CHECK (valence >= 0 AND valence <= 1),
    tempo FLOAT CHECK (tempo > 0),
    release_date DATE
);

-- Table: events
CREATE TABLE IF NOT EXISTS events (
    event_id VARCHAR(50) PRIMARY KEY,
    user_id VARCHAR(50) REFERENCES users(user_id),
    track_id VARCHAR(50) REFERENCES tracks(track_id),
    event_type VARCHAR(20) CHECK (event_type IN ('play', 'pause', 'skip', 'like', 'share', 'add_to_playlist')),
    timestamp TIMESTAMP NOT NULL,
    device VARCHAR(50),
    duration_played INTEGER CHECK (duration_played >= 0),
    session_id VARCHAR(100),
    source VARCHAR(50),
    shuffle BOOLEAN DEFAULT FALSE,
    offline BOOLEAN DEFAULT FALSE
);

-- Indexes for performance
CREATE INDEX idx_events_user_id ON events(user_id);
CREATE INDEX idx_events_timestamp ON events(timestamp);
CREATE INDEX idx_events_track_id ON events(track_id);
CREATE INDEX idx_users_country ON users(country);
CREATE INDEX idx_users_subscription ON users(subscription);
CREATE INDEX idx_tracks_artist_id ON tracks(artist_id);
CREATE INDEX idx_tracks_popularity ON tracks(popularity);

-- Sample data insertion (optional)
INSERT INTO artists (artist_id, name, genre, followers, popularity, monthly_listeners) VALUES
('artist_001', 'The Data Engineers', 'Electronic', 1500000, 85, 5000000),
('artist_002', 'SQL Band', 'Rock', 800000, 72, 3000000),
('artist_003', 'Python Singers', 'Pop', 1200000, 90, 7000000)
ON CONFLICT (artist_id) DO NOTHING;

SELECT '✅ Database schema created successfully!' as message;