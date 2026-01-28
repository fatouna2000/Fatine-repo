## 🎵 Pipeline d'Analyse Musicale avec Apache Spark & Airflow
## 🚀 Projet d'Ingénierie des Données - Formation IBM
https://img.shields.io/badge/Apache%2520Airflow-2.7.1-green
https://img.shields.io/badge/Apache%2520Spark-3.5.0-red
https://img.shields.io/badge/Python-3.9-blue
https://img.shields.io/badge/Statut-Pr%C3%AAt%2520pour%2520Production-success

## 📋 Aperçu du Projet
Pipeline de données complet pour l'analyse de streaming musical, développé pendant la formation Data Engineering d'IBM. Ce projet démontre des compétences réelles en ingénierie des données avec des technologies modernes de Big Data.

## ✅ Ce que j'ai construit
Traitement PySpark réel : Pas simulé - vrai calcul distribué avec Apache Spark

DAG Airflow automatisé : Exécution quotidienne avec monitoring et gestion d'erreurs

Génération de données : Données de streaming musical réalistes avec patterns

Analyses complètes : Classement des artistes, insights géographiques, métriques d'utilisation

Fonctionnalités production : Logique de retry, validation des données, reporting automatisé

## 🏗️ Architecture
┌─────────────────────────────────────────┐
│         Apache Airflow (DAG)            │
│  ┌─────┐  ┌─────────┐  ┌─────────┐     │
│  │Start│→ │Génère   │→ │ Spark   │     │
│  │     │  │ Données │  │ Traite  │     │
│  └─────┘  └─────────┘  └─────────┘     │
│         │         │         │           │
│         ▼         ▼         ▼           │
│  [Rapports]  [Fichiers]  [Données]     │
│              [CSV]        [JSON]        │
└─────────────────────────────────────────┘
## 📊 Étapes du Pipeline
Génération de données : Création de données de streaming synthétiques (500+ enregistrements)

Traitement Spark : Analyses distribuées avec agrégations PySpark

Analyse : Calcul du classement des artistes, statistiques par pays, utilisation des appareils

Reporting : Génération de résumés JSON et rapports Markdown

Nettoyage : Gestion automatique des fichiers temporaires

## 🛠️ Technologies Utilisées
Technologie	Objectif	Version
Apache Airflow	Orchestration de workflows	2.7.1
Apache Spark	Traitement distribué de données	3.5.0
PySpark	Interface Python pour Spark	3.5.0
Pandas	Manipulation et génération de données	2.0.3
NumPy	Calculs numériques	1.24.3
## 📁 Structure du Projet
music-analytics-airflow-spark/
├── music_analytics_dag.py     # DAG Airflow complet
├── README.md                  # Cette documentation
├── requirements.txt           # Dépendances Python
├── .gitignore                # Règles Git ignore
└── sample_output/            # Exemples de fichiers générés
## 🚀 Démarrage Rapide
Prérequis
Python 3.9+

Java 8+ (pour Spark)

Apache Airflow 2.7+

## Installation
# 1. Cloner le repository
git clone https://github.com/votrenom/music-analytics-airflow-spark.git
cd music-analytics-airflow-spark

# 2. Installer les dépendances
pip install -r requirements.txt

# 3. Initialiser Airflow
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin

# 4. Copier le DAG dans Airflow
mkdir -p $AIRFLOW_HOME/dags
cp music_analytics_dag.py $AIRFLOW_HOME/dags/
# Exécution du Pipeline

# Démarrer les services Airflow
airflow webserver --port 8080 --daemon
airflow scheduler --daemon

# Déclencher le DAG
airflow dags trigger music_analytics_with_spark

# Monitorer l'exécution (ouvrir dans le navigateur)
# http://localhost:8080

## 🎯 Fonctionnalités Clés Implémentées
## 1. Traitement Spark Réel
PySpark 3.5.0 réel (pas simulé ou mocké)

Calculs distribués sur machine locale

Configuration et gestion appropriée de SparkSession

Utilisation mémoire efficace (2GB alloués)

## 2. DAG Airflow de Qualité Production
Exécution planifiée quotidienne (@daily)

Retry automatique en cas d'échec (2 tentatives)

Logging et monitoring complets

Notifications email en cas d'échec (configurable)

## 3. Qualité & Validation des Données
Validation du schéma des données générées

Vérification des plages (plays positifs, durées valides)

Vérification de la complétude des données

Nettoyage automatique des anciens fichiers

## 4. Analytics Business-Ready
Classement de popularité des artistes

Analyse de distribution géographique

Patterns d'utilisation des appareils

Analyse des tendances temporelles

Export vers multiples formats (CSV, JSON, Markdown)