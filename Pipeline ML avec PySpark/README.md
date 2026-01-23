# 🚀 Pipeline ML avec PySpark : Prédiction du bruit des aérofoils NASA

## 📋 Aperçu rapide
Pipeline complet de Machine Learning distribué avec PySpark pour prédire le niveau sonore des aérofoils NASA.

**Features** : ETL → Feature Engineering → Modélisation → Évaluation → Production

## 🎯 Scénario
Data Engineer en aéronautique devant fournir un pipeline ML robuste pour optimiser les designs d'aérofoils.

## 🛠️ Technologies
![PySpark](https://img.shields.io/badge/PySpark-3.1.2-orange)
![Python](https://img.shields.io/badge/Python-3.7%2B-blue)
![ML](https://img.shields.io/badge/ML-Pipeline-green)

**Stack** : PySpark | Spark MLlib | Parquet | Linear Regression

## 📊 Résultats
| Métrique | Valeur |
|----------|---------|
| **R²** | 0.54 |
| **MSE** | 22.59 |
| **MAE** | 3.73 |

## 📁 Contenu du notebook
✅ ETL complet avec nettoyage des données

✅ Feature Engineering (VectorAssembler + StandardScaler)

✅ Modélisation avec régression linéaire

✅ Évaluation détaillée (MSE, MAE, R²)

✅ Persistance du modèle pour production

✅ Visualisations et explications

## 🎯 Compétences démontrées
PySpark ML : Feature engineering avec VectorAssembler/StandardScaler

Pipeline ML : Construction et évaluation complète

ETL avancé : Nettoyage + format Parquet

MLOps : Persistance et chargement de modèles

## 📊 Performance détaillée
Modèle final : Régression linéaire sur features normalisées

R² = 0.54 : 54% de variance expliquée

MAE = 3.73 dB : Erreur moyenne de ±3.73 décibels

Impact features : Fréquence et ChordLength prédominants