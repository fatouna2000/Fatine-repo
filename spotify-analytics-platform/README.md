# 🎵 Spotify Analytics Platform

![Airflow](https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-316192?style=for-the-badge&logo=postgresql&logoColor=white)
![Python](https://img.shields.io/badge/Python-FFD43B?style=for-the-badge&logo=python&logoColor=blue)
![Docker](https://img.shields.io/badge/Docker-2CA5E0?style=for-the-badge&logo=docker&logoColor=white)

Une plateforme complète de Data Engineering pour l'analyse de données musicales, inspirée par Spotify.

## 🏗️ Architecture





## ✨ Fonctionnalités

✅ **Orchestration avec Apache Airflow** - Pipeline ETL automatisé  
✅ **Data Warehouse avec PostgreSQL** - Stockage professionnel des données  
✅ **Génération de données réalistes** - 500 utilisateurs, 200 artistes, 50,000 événements  
✅ **Vues analytiques** - Métriques prêtes à l'emploi  
✅ **Monitoring** - Interface Airflow pour le suivi des exécutions  

## 🚀 Démarrage Rapide

### Prérequis
- Python 3.8+
- PostgreSQL 14+
- Apache Airflow 2.7+

### Installation
```bash
# 1. Clone le projet
git clone https://github.com/ton-username/spotify-analytics-platform.git
cd spotify-analytics-platform

# 2. Installe les dépendances
pip install -r requirements.txt

# 3. Configure PostgreSQL
sudo service postgresql start
psql -c "CREATE DATABASE spotify_analytics;"

# 4. Configure Airflow
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

# 5. Copie les DAGs
cp dags/*.py $AIRFLOW_HOME/dags/

# 6. Démarre les services
airflow webserver --port 8080 --daemon
airflow scheduler --daemon
