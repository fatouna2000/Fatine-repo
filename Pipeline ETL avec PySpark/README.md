## 🛠️ Projet de Formation : Pipeline ETL avec PySpark
## 📋 Contexte
Ce projet a été réalisé dans le cadre de la formation Data Engineering d'IBM pour démontrer les compétences acquises en traitement de données à grande échelle avec PySpark.

## 🎯 Objectifs du Projet
Chargement : Téléchargement automatique de datasets depuis le cloud

Transformation : Nettoyage, enrichissement et jointure de données

Agrégation : Calculs de totaux et moyennes par différentes dimensions

Stockage : Persistance dans Hive et HDFS pour analyse future

## 🏗️ Technologies Utilisées
PySpark 3.4.4 - Traitement distribué

Python 3.7 - Langage principal

Hive - Entrepôt de données

HDFS - Système de fichiers distribué

Jupyter Notebook - Environnement de développement

## 📊 Fonctionnalités Clés
ETL Complet : De l'extraction au chargement

Jointures Intelligentes : Sur la colonne customer_id

Enrichissement Temporel : Ajout de colonnes year et quarter

Filtrage Business : Transactions > 1000

Agrégations Multi-niveaux : Par client, trimestre et année

Stockage Optimisé : Tables Hive + fichiers Parquet HDFS

## 🚀 Résultats Concrets
2 tables Hive créées : customer_totals et quarterly_averages

1 dataset Parquet dans HDFS : filtered_data.parquet

12 transformations appliquées automatiquement

Pipeline reproductible pour traitement à grande échelle

## 🧠 Compétences IBM Développées
Ce projet valide les compétences clés de la formation Data Engineering IBM :

Big Data Processing avec PySpark

Data Warehousing avec Hive

Distributed Storage avec HDFS

ETL Pipeline Development

Data Quality Management



Formation : IBM Data Engineering
Objectif : Validation pratique des compétences PySpark/ETL