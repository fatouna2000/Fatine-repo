# /home/project/airflow/dags/spotify_postgres_pro.py
"""
Pipeline Spotify PostgreSQL PROFESSIONNEL
Utilise PostgreSQL maintenant qu'il est disponible
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import pandas as pd
import numpy as np
import os
import json

# Configuration
DATA_PATH = "/tmp/spotify-postgres-pro"
os.makedirs(DATA_PATH, exist_ok=True)

def setup_postgresql():
    """Configure PostgreSQL pour le projet"""
    print("🗄️ Configuration PostgreSQL...")
    
    try:
        import psycopg2
        from sqlalchemy import create_engine
        
        # Connexion à PostgreSQL (sans mot de passe par défaut dans Skills Network)
        conn_str = 'postgresql://postgres:@localhost:5432/postgres'
        engine = create_engine(conn_str)
        
        # Créer la base de données si elle n'existe pas
        with engine.connect() as conn:
            conn.execute("COMMIT")  # Sortir de la transaction
            try:
                conn.execute("CREATE DATABASE spotify_analytics")
                print("✅ Base de données 'spotify_analytics' créée")
            except Exception as e:
                print(f"ℹ️ Base existe déjà: {e}")
        
        # Maintenant connecter à la nouvelle base
        conn_str_spotify = 'postgresql://postgres:@localhost:5432/spotify_analytics'
        engine_spotify = create_engine(conn_str_spotify)
        
        # Créer les tables
        with engine_spotify.connect() as conn:
            # Table utilisateurs
            conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id VARCHAR(50) PRIMARY KEY,
                username VARCHAR(100),
                email VARCHAR(255),
                country CHAR(2),
                age INTEGER,
                subscription VARCHAR(20),
                created_at TIMESTAMP,
                last_active TIMESTAMP
            )
            """)
            
            # Table artistes
            conn.execute("""
            CREATE TABLE IF NOT EXISTS artists (
                artist_id VARCHAR(50) PRIMARY KEY,
                name VARCHAR(255),
                genre VARCHAR(100),
                followers INTEGER,
                popularity INTEGER,
                monthly_listeners INTEGER
            )
            """)
            
            # Table morceaux
            conn.execute("""
            CREATE TABLE IF NOT EXISTS tracks (
                track_id VARCHAR(50) PRIMARY KEY,
                title VARCHAR(255),
                artist_id VARCHAR(50),
                album VARCHAR(255),
                duration_ms INTEGER,
                explicit BOOLEAN,
                popularity INTEGER,
                danceability FLOAT,
                energy FLOAT,
                valence FLOAT,
                tempo FLOAT,
                release_date TIMESTAMP,
                FOREIGN KEY (artist_id) REFERENCES artists(artist_id)
            )
            """)
            
            # Table événements
            conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                event_id VARCHAR(50) PRIMARY KEY,
                user_id VARCHAR(50),
                track_id VARCHAR(50),
                event_type VARCHAR(20),
                timestamp TIMESTAMP,
                device VARCHAR(50),
                duration_played INTEGER,
                session_id VARCHAR(100),
                source VARCHAR(50),
                shuffle BOOLEAN,
                offline BOOLEAN,
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                FOREIGN KEY (track_id) REFERENCES tracks(track_id)
            )
            """)
            
            # Créer des index pour la performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_events_user_id ON events(user_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_events_track_id ON events(track_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_users_country ON users(country)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_users_subscription ON users(subscription)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_tracks_artist_id ON tracks(artist_id)")
            
            print("✅ Tables et index créés dans PostgreSQL")
        
        return {
            "status": "success",
            "database": "spotify_analytics",
            "connection": conn_str_spotify
        }
        
    except Exception as e:
        print(f"❌ Erreur configuration PostgreSQL: {e}")
        return {"status": "error", "error": str(e)}

def generate_production_data():
    """Génère des données de production réalistes"""
    print("🏭 Génération de données de production...")
    
    np.random.seed(42)
    
    # 1. Utilisateurs (500 utilisateurs)
    users = []
    countries = ['US', 'GB', 'FR', 'DE', 'JP', 'BR', 'MX', 'ES', 'IT', 'CA', 'AU', 'IN']
    
    for i in range(1, 501):
        users.append({
            'user_id': f'user_{i:05d}',
            'username': f'user_{i}',
            'email': f'user{i}@musicmail.com',
            'country': np.random.choice(countries, p=[0.3, 0.15, 0.1, 0.1, 0.08, 0.07, 0.06, 0.05, 0.04, 0.03, 0.01, 0.01]),
            'age': np.random.randint(13, 75),
            'subscription': np.random.choice(['free', 'premium', 'family', 'student'], p=[0.35, 0.45, 0.1, 0.1]),
            'created_at': datetime.now() - timedelta(days=np.random.randint(1, 1825)),  # Jusqu'à 5 ans
            'last_active': datetime.now() - timedelta(days=np.random.randint(0, 90))
        })
    
    # 2. Artistes (200 artistes)
    artists = []
    genres = ['Pop', 'Rock', 'Hip Hop', 'Electronic', 'R&B', 'Country', 'Jazz', 'Classical', 'Reggaeton', 'K-Pop']
    
    for i in range(1, 201):
        genre = np.random.choice(genres)
        artist_popularity = np.random.randint(1, 100)
        artists.append({
            'artist_id': f'artist_{i:04d}',
            'name': f'Artist {i}',
            'genre': genre,
            'followers': int(np.random.exponential(100000)) + 1000,
            'popularity': artist_popularity,
            'monthly_listeners': int(np.random.exponential(500000)) + 10000
        })
    
    # 3. Morceaux (2000 morceaux)
    tracks = []
    for i in range(1, 2001):
        artist_idx = np.random.randint(1, 201)
        track_duration = np.random.randint(90000, 420000)  # 1.5-7 minutes
        
        # Corréler la popularité avec celle de l'artiste
        base_popularity = artists[artist_idx-1]['popularity']
        track_popularity = min(100, max(1, int(np.random.normal(base_popularity, 15))))
        
        tracks.append({
            'track_id': f'track_{i:06d}',
            'title': f'Song {i}',
            'artist_id': f'artist_{artist_idx:04d}',
            'album': f'Album {np.random.randint(1, 101)}',
            'duration_ms': track_duration,
            'explicit': np.random.random() > 0.7,
            'popularity': track_popularity,
            'danceability': np.random.beta(2, 2),  # Distribution en cloche
            'energy': np.random.beta(1.5, 1),
            'valence': np.random.beta(1, 1),  # Émotion (positivité)
            'tempo': np.random.uniform(60, 200),
            'release_date': datetime.now() - timedelta(days=np.random.randint(0, 365*3))
        })
    
    # 4. Événements d'écoute (50,000 événements)
    events = []
    event_types = ['play', 'pause', 'skip', 'like', 'share', 'add_to_playlist', 'radio_start']
    event_probs = [0.60, 0.08, 0.15, 0.06, 0.02, 0.05, 0.04]
    
    devices = ['mobile_ios', 'mobile_android', 'desktop_web', 'desktop_app', 'smart_speaker']
    device_probs = [0.35, 0.35, 0.15, 0.10, 0.05]
    
    sources = ['search', 'playlist', 'radio', 'artist_page', 'charts', 'discover_weekly', 'release_radar']
    source_probs = [0.20, 0.30, 0.15, 0.10, 0.05, 0.10, 0.10]
    
    print("📊 Génération des événements...")
    for i in range(1, 50001):
        if i % 10000 == 0:
            print(f"  ⏳ {i}/50000 événements générés...")
        
        user_idx = np.random.randint(1, 501)
        track_idx = np.random.randint(1, 2001)
        track_duration = tracks[track_idx-1]['duration_ms'] // 1000  # Convertir en secondes
        
        # Probabilité d'écoute complète basée sur la popularité
        popularity_factor = tracks[track_idx-1]['popularity'] / 100
        listen_complete_prob = 0.3 + (popularity_factor * 0.5)
        
        duration_played = 0
        if np.random.random() < listen_complete_prob:
            # Écoute complète ou partielle
            duration_played = min(track_duration, int(np.random.normal(track_duration * 0.8, track_duration * 0.2)))
        else:
            # Skip rapide
            duration_played = np.random.randint(5, 30)
        
        events.append({
            'event_id': f'event_{i:08d}',
            'user_id': f'user_{user_idx:05d}',
            'track_id': f'track_{track_idx:06d}',
            'event_type': np.random.choice(event_types, p=event_probs),
            'timestamp': datetime.now() - timedelta(
                hours=np.random.exponential(720)  # Plus récent = plus probable
            ),
            'device': np.random.choice(devices, p=device_probs),
            'duration_played': max(1, duration_played),
            'session_id': f'session_{np.random.randint(1, 10000):06d}',
            'source': np.random.choice(sources, p=source_probs),
            'shuffle': np.random.random() > 0.6,
            'offline': np.random.random() > 0.9  # 10% d'écoute hors ligne
        })
    
    # Sauvegarder en CSV (backup)
    print("💾 Sauvegarde des fichiers CSV...")
    pd.DataFrame(users).to_csv(f"{DATA_PATH}/users.csv", index=False)
    pd.DataFrame(artists).to_csv(f"{DATA_PATH}/artists.csv", index=False)
    pd.DataFrame(tracks).to_csv(f"{DATA_PATH}/tracks.csv", index=False)
    pd.DataFrame(events).to_csv(f"{DATA_PATH}/events.csv", index=False)
    
    # Statistiques
    stats = {
        "users": len(users),
        "artists": len(artists),
        "tracks": len(tracks),
        "events": len(events),
        "data_size_mb": sum([
            os.path.getsize(f"{DATA_PATH}/users.csv"),
            os.path.getsize(f"{DATA_PATH}/artists.csv"),
            os.path.getsize(f"{DATA_PATH}/tracks.csv"),
            os.path.getsize(f"{DATA_PATH}/events.csv")
        ]) / (1024 * 1024)
    }
    
    print("✅ DONNÉES DE PRODUCTION GÉNÉRÉES:")
    print(f"   👥 {stats['users']:,} utilisateurs")
    print(f"   🎤 {stats['artists']:,} artistes")
    print(f"   🎵 {stats['tracks']:,} morceaux")
    print(f"   📊 {stats['events']:,} événements")
    print(f"   💾 Taille totale: {stats['data_size_mb']:.2f} MB")
    
    return stats

def load_to_postgresql():
    """Charge les données dans PostgreSQL"""
    print("🚀 Chargement dans PostgreSQL...")
    
    try:
        from sqlalchemy import create_engine
        
        # Connexion à PostgreSQL
        conn_str = 'postgresql://postgres:@localhost:5432/spotify_analytics'
        engine = create_engine(conn_str)
        
        # Vérifier la connexion
        with engine.connect() as conn:
            conn.execute("SELECT 1")
        print("✅ Connecté à PostgreSQL: spotify_analytics")
        
        # Charger les données
        tables_to_load = [
            ('users', f'{DATA_PATH}/users.csv'),
            ('artists', f'{DATA_PATH}/artists.csv'),
            ('tracks', f'{DATA_PATH}/tracks.csv'),
            ('events', f'{DATA_PATH}/events.csv')
        ]
        
        loaded_counts = {}
        
        for table_name, file_path in tables_to_load:
            if os.path.exists(file_path):
                print(f"📥 Chargement {table_name}...")
                df = pd.read_csv(file_path)
                
                # Convertir les dates
                date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower()]
                for col in date_columns:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # Charger dans PostgreSQL
                df.to_sql(table_name, engine, if_exists='replace', index=False, method='multi', chunksize=1000)
                loaded_counts[table_name] = len(df)
                print(f"   ✅ {len(df):,} lignes chargées")
            else:
                print(f"⚠️ Fichier manquant: {file_path}")
        
        # Créer des vues analytiques
        print("🔧 Création des vues analytiques...")
        with engine.connect() as conn:
            # Vue: Métriques utilisateur
            conn.execute("""
            CREATE OR REPLACE VIEW user_analytics AS
            SELECT 
                u.user_id,
                u.country,
                u.subscription,
                u.age,
                COUNT(e.event_id) as total_plays,
                SUM(e.duration_played) as total_duration,
                COUNT(DISTINCT e.track_id) as unique_tracks,
                COUNT(DISTINCT DATE(e.timestamp)) as active_days,
                SUM(CASE WHEN e.event_type IN ('like', 'share', 'add_to_playlist') THEN 1 ELSE 0 END) as engagements,
                MAX(e.timestamp) as last_played
            FROM users u
            LEFT JOIN events e ON u.user_id = e.user_id
            GROUP BY u.user_id, u.country, u.subscription, u.age
            """)
            
            # Vue: Métriques quotidiennes
            conn.execute("""
            CREATE OR REPLACE VIEW daily_metrics AS
            SELECT 
                DATE(timestamp) as date,
                COUNT(*) as total_plays,
                COUNT(DISTINCT user_id) as unique_users,
                SUM(duration_played) as total_duration,
                AVG(duration_played) as avg_duration,
                COUNT(DISTINCT track_id) as unique_tracks,
                SUM(CASE WHEN event_type IN ('like', 'share', 'add_to_playlist') THEN 1 ELSE 0 END) as engagements,
                ROUND(100.0 * 
                    SUM(CASE WHEN event_type IN ('like', 'share', 'add_to_playlist') THEN 1 ELSE 0 END) 
                    / COUNT(*), 2) as engagement_rate
            FROM events
            GROUP BY DATE(timestamp)
            ORDER BY date
            """)
            
            # Vue: Top artistes
            conn.execute("""
            CREATE OR REPLACE VIEW top_artists AS
            SELECT 
                a.artist_id,
                a.name,
                a.genre,
                a.popularity,
                COUNT(e.event_id) as total_plays,
                COUNT(DISTINCT e.user_id) as unique_listeners,
                AVG(e.duration_played) as avg_listen_duration
            FROM artists a
            JOIN tracks t ON a.artist_id = t.artist_id
            JOIN events e ON t.track_id = e.track_id
            GROUP BY a.artist_id, a.name, a.genre, a.popularity
            ORDER BY total_plays DESC
            """)
            
            conn.commit()
        
        print("✅ Vues analytiques créées")
        
        # Vérifier les données chargées
        verification = {}
        with engine.connect() as conn:
            for table in ['users', 'artists', 'tracks', 'events']:
                result = conn.execute(f"SELECT COUNT(*) FROM {table}")
                count = result.fetchone()[0]
                verification[table] = count
                print(f"🔍 {table}: {count:,} lignes")
        
        return {
            "status": "success",
            "database": "postgresql",
            "connection": conn_str,
            "loaded_counts": loaded_counts,
            "verification": verification
        }
        
    except Exception as e:
        print(f"❌ Erreur PostgreSQL: {e}")
        return {"status": "error", "error": str(e)}

def analyze_with_postgresql():
    """Analyse avancée avec PostgreSQL"""
    print("🧠 Analyse avancée avec PostgreSQL...")
    
    try:
        from sqlalchemy import create_engine
        import json
        
        # Connexion
        conn_str = 'postgresql://postgres:@localhost:5432/spotify_analytics'
        engine = create_engine(conn_str)
        
        analyses = {}
        
        with engine.connect() as conn:
            # 1. Statistiques globales
            print("📊 Calcul des statistiques globales...")
            result = conn.execute("""
            SELECT 
                'Global Stats' as metric,
                COUNT(DISTINCT u.user_id) as total_users,
                COUNT(DISTINCT a.artist_id) as total_artists,
                COUNT(DISTINCT t.track_id) as total_tracks,
                COUNT(e.event_id) as total_events,
                ROUND(AVG(e.duration_played), 1) as avg_listen_duration,
                ROUND(100.0 * 
                    SUM(CASE WHEN e.event_type IN ('like', 'share', 'add_to_playlist') THEN 1 ELSE 0 END) 
                    / COUNT(*), 2) as engagement_rate
            FROM users u
            CROSS JOIN artists a
            CROSS JOIN tracks t
            CROSS JOIN events e
            GROUP BY metric
            """)
            analyses['global_stats'] = dict(zip(['total_users', 'total_artists', 'total_tracks', 'total_events', 
                                                'avg_listen_duration', 'engagement_rate'], result.fetchone()[1:]))
            
            # 2. Distribution géographique
            print("🌍 Analyse géographique...")
            result = conn.execute("""
            SELECT 
                country,
                COUNT(*) as user_count,
                ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage,
                ROUND(AVG(age), 1) as avg_age,
                ROUND(100.0 * SUM(CASE WHEN subscription = 'premium' THEN 1 ELSE 0 END) / COUNT(*), 2) as premium_rate
            FROM users
            GROUP BY country
            ORDER BY user_count DESC
            LIMIT 15
            """)
            analyses['geo_distribution'] = [
                {"country": row[0], "users": row[1], "percentage": row[2], "avg_age": row[3], "premium_rate": row[4]}
                for row in result.fetchall()
            ]
            
            # 3. Analyse temporelle
            print("⏰ Analyse temporelle...")
            result = conn.execute("""
            SELECT 
                EXTRACT(HOUR FROM timestamp) as hour,
                COUNT(*) as event_count,
                COUNT(DISTINCT user_id) as unique_users,
                ROUND(AVG(duration_played), 1) as avg_duration,
                ROUND(100.0 * 
                    SUM(CASE WHEN event_type IN ('like', 'share', 'add_to_playlist') THEN 1 ELSE 0 END) 
                    / COUNT(*), 2) as engagement_rate
            FROM events
            GROUP BY EXTRACT(HOUR FROM timestamp)
            ORDER BY hour
            """)
            analyses['hourly_analysis'] = [
                {"hour": int(row[0]), "events": row[1], "users": row[2], "avg_duration": row[3], "engagement": row[4]}
                for row in result.fetchall()
            ]
            
            # 4. Top contenu
            print("🏆 Analyse du top contenu...")
            result = conn.execute("""
            SELECT 
                t.track_id,
                t.title,
                a.name as artist,
                COUNT(e.event_id) as play_count,
                COUNT(DISTINCT e.user_id) as unique_listeners,
                ROUND(AVG(e.duration_played), 1) as avg_duration,
                ROUND(100.0 * 
                    SUM(CASE WHEN e.event_type IN ('like', 'share', 'add_to_playlist') THEN 1 ELSE 0 END) 
                    / COUNT(*), 2) as engagement_rate
            FROM tracks t
            JOIN artists a ON t.artist_id = a.artist_id
            JOIN events e ON t.track_id = e.track_id
            GROUP BY t.track_id, t.title, a.name
            HAVING COUNT(e.event_id) >= 10
            ORDER BY play_count DESC
            LIMIT 20
            """)
            analyses['top_tracks'] = [
                {"track_id": row[0], "title": row[1], "artist": row[2], "plays": row[3], 
                 "listeners": row[4], "avg_duration": row[5], "engagement": row[6]}
                for row in result.fetchall()
            ]
            
            # 5. Analyse des utilisateurs premium vs free
            print("💎 Analyse premium vs free...")
            result = conn.execute("""
            SELECT 
                subscription,
                COUNT(*) as user_count,
                ROUND(AVG(age), 1) as avg_age,
                ROUND(AVG(EXTRACT(EPOCH FROM (NOW() - last_active))/86400), 1) as days_since_last_active,
                (SELECT COUNT(DISTINCT e.user_id) 
                 FROM events e 
                 WHERE e.user_id IN (SELECT user_id FROM users WHERE subscription = u.subscription)
                ) as active_users_count
            FROM users u
            GROUP BY subscription
            ORDER BY user_count DESC
            """)
            analyses['subscription_analysis'] = [
                {"subscription": row[0], "users": row[1], "avg_age": row[2], 
                 "days_inactive": row[3], "active_users": row[4]}
                for row in result.fetchall()
            ]
        
        # Sauvegarder les analyses
        os.makedirs(f"{DATA_PATH}/analyses", exist_ok=True)
        
        # Rapport JSON complet
        report_file = f"{DATA_PATH}/analyses/postgresql_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_file, 'w') as f:
            json.dump(analyses, f, indent=2, default=str)
        
        # Rapport texte résumé
        summary_file = f"{DATA_PATH}/analyses/executive_summary.txt"
        with open(summary_file, 'w') as f:
            f.write("=" * 80 + "\n")
            f.write("SPOTIFY ANALYTICS - RAPPORT EXÉCUTIF (PostgreSQL)\n")
            f.write(f"Généré le: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("=" * 80 + "\n\n")
            
            global_stats = analyses.get('global_stats', {})
            f.write("📊 STATISTIQUES GLOBALES:\n")
            f.write("-" * 60 + "\n")
            f.write(f"👥 Utilisateurs totaux: {global_stats.get('total_users', 0):,}\n")
            f.write(f"🎤 Artistes totaux: {global_stats.get('total_artists', 0):,}\n")
            f.write(f"🎵 Morceaux totaux: {global_stats.get('total_tracks', 0):,}\n")
            f.write(f"📈 Événements totaux: {global_stats.get('total_events', 0):,}\n")
            f.write(f"⏱️  Durée moyenne d'écoute: {global_stats.get('avg_listen_duration', 0):.1f}s\n")
            f.write(f"💝 Taux d'engagement: {global_stats.get('engagement_rate', 0):.2f}%\n\n")
            
            f.write("🌍 TOP 5 PAYS:\n")
            f.write("-" * 60 + "\n")
            for i, country_data in enumerate(analyses.get('geo_distribution', [])[:5], 1):
                f.write(f"{i}. {country_data['country']}: {country_data['users']:,} utilisateurs ")
                f.write(f"({country_data['percentage']}%), Âge moyen: {country_data['avg_age']} ans, ")
                f.write(f"Premium: {country_data['premium_rate']}%\n")
            
            f.write("\n🏆 TOP 3 MORCEAUX:\n")
            f.write("-" * 60 + "\n")
            for i, track in enumerate(analyses.get('top_tracks', [])[:3], 1):
                f.write(f"{i}. {track['title']} - {track['artist']}\n")
                f.write(f"   🔊 {track['plays']:,} plays | 👥 {track['listeners']:,} auditeurs uniques\n")
                f.write(f"   ⏱️  {track['avg_duration']}s en moyenne | 💝 Engagement: {track['engagement']}%\n")
            
            f.write("\n💎 COMPARAISON ABONNEMENTS:\n")
            f.write("-" * 60 + "\n")
            for sub in analyses.get('subscription_analysis', []):
                f.write(f"📱 {sub['subscription'].upper()}:\n")
                f.write(f"   👥 {sub['users']:,} utilisateurs ({sub['users']/global_stats.get('total_users', 1)*100:.1f}%)\n")
                f.write(f"   📅 Dernière activité: {sub['days_inactive']:.1f} jours en moyenne\n")
                f.write(f"   🎵 Utilisateurs actifs: {sub['active_users']:,}\n")
        
        print(f"✅ Analyses PostgreSQL générées:")
        print(f"   📊 Rapport complet: {report_file}")
        print(f"   📝 Résumé exécutif: {summary_file}")
        
        # Afficher un aperçu
        with open(summary_file, 'r') as f:
            lines = f.readlines()[:40]
            print("\n📄 APERÇU DU RAPPORT:")
            for line in lines:
                print(line.rstrip())
        
        return {
            "status": "analysis_complete",
            "reports": {
                "detailed": report_file,
                "summary": summary_file
            },
            "analyses_generated": len(analyses)
        }
        
    except Exception as e:
        print(f"❌ Erreur analyse PostgreSQL: {e}")
        return {"status": "analysis_error", "error": str(e)}

def verify_postgresql():
    """Vérification finale PostgreSQL"""
    print("🔍 Vérification finale PostgreSQL...")
    
    try:
        from sqlalchemy import create_engine
        
        conn_str = 'postgresql://postgres:@localhost:5432/spotify_analytics'
        engine = create_engine(conn_str)
        
        with engine.connect() as conn:
            # Vérifier toutes les tables
            tables = ['users', 'artists', 'tracks', 'events', 'user_analytics', 'daily_metrics', 'top_artists']
            
            verification = {}
            for table in tables:
                try:
                    result = conn.execute(f"SELECT COUNT(*) FROM {table}")
                    count = result.fetchone()[0]
                    verification[table] = count
                    print(f"✅ {table}: {count:,} lignes")
                except:
                    print(f"⚠️ {table}: Table non trouvée ou erreur")
                    verification[table] = "error"
            
            # Vérifier la taille de la base
            result = conn.execute("""
            SELECT 
                pg_database_size('spotify_analytics') as size_bytes,
                pg_size_pretty(pg_database_size('spotify_analytics')) as size_pretty
            """)
            db_size = result.fetchone()
            
            verification['database_size'] = {
                'bytes': db_size[0],
                'human_readable': db_size[1]
            }
            
            print(f"💾 Taille base de données: {db_size[1]}")
        
        return {
            "status": "verification_complete",
            "verification": verification,
            "database": "spotify_analytics",
            "connection": conn_str
        }
        
    except Exception as e:
        print(f"❌ Erreur vérification: {e}")
        return {"status": "verification_error", "error": str(e)}

# ========== DAG PROFESSIONNEL ==========

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    'spotify_postgres_production',
    default_args=default_args,
    description='Pipeline Spotify de production avec PostgreSQL',
    schedule_interval='0 2 * * *',  # Tous les jours à 2h du matin
    catchup=False,
    tags=['spotify', 'postgresql', 'production', 'analytics', 'data-engineering'],
    max_active_runs=1,
) as dag:
    
    start = BashOperator(
        task_id='start',
        bash_command=f'echo "🚀 DÉMARRAGE Pipeline Production PostgreSQL" && mkdir -p {DATA_PATH}/{{analyses,backups}}',
    )
    
    setup = PythonOperator(
        task_id='setup_postgresql',
        python_callable=setup_postgresql,
    )
    
    generate = PythonOperator(
        task_id='generate_production_data',
        python_callable=generate_production_data,
    )
    
    load = PythonOperator(
        task_id='load_to_postgresql',
        python_callable=load_to_postgresql,
    )
    
    analyze = PythonOperator(
        task_id='analyze_with_postgresql',
        python_callable=analyze_with_postgresql,
    )
    
    verify = PythonOperator(
        task_id='verify_postgresql',
        python_callable=verify_postgresql,
    )
    
    end = BashOperator(
        task_id='end',
        bash_command=f'''
        echo "✅ PIPELINE PRODUCTION POSTGRESQL TERMINÉ ! 🎉" &&
        echo "📊 Données chargées dans: spotify_analytics" &&
        echo "📁 Rapports générés dans: {DATA_PATH}/analyses/" &&
        echo "🎵 Projet Spotify Analytics - COMPLET"
        ''',
    )
    
    # Dépendances
    start >> setup >> generate >> load >> analyze >> verify >> end