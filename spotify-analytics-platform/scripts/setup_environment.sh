#!/bin/bash

# Spotify Analytics Platform - Setup Script
echo "🎵 Spotify Analytics Platform - Setup"
echo "====================================="

# 1. Install Python dependencies
echo "📦 Installing Python dependencies..."
pip3 install -r requirements.txt

# 2. Start PostgreSQL
echo "🗄️ Starting PostgreSQL..."
sudo service postgresql start 2>/dev/null || echo "PostgreSQL already running"

# 3. Create database
echo "📊 Creating database..."
psql -h localhost -U postgres -c "CREATE DATABASE IF NOT EXISTS spotify_analytics;" 2>/dev/null || echo "Database already exists"

# 4. Test connections
echo "🔌 Testing connections..."
python3 scripts/test_connections.py

echo "✅ Setup completed!"