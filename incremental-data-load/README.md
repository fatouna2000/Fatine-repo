# 🔄 Chargement Incrémental MySQL → PostgreSQL

## 📋 Contexte
Script Python pour synchroniser incrémentalement les données d'une base MySQL (staging) vers PostgreSQL (production).

## 🛠️ Technologies
- **Python 3** + **MySQL Connector** + **Psycopg2**
- **MySQL** (base de staging)
- **PostgreSQL** (data warehouse de production)

## 🎯 Fonctionnement
1. **Identifier** le dernier RowID en production
2. **Extraire** les nouvelles lignes depuis le staging
3. **Insérer** avec gestion des conflits (idempotent)

## 🔧 Caractéristiques
- ✅ **Chargement incrémental** (delta)
- ✅ **Idempotent** (ré-exécutable)
- ✅ **Gestion des transactions** (commit/rollback)
- ✅ **Logging détaillé**

## 🎓 Formation
**Formation** : IBM Data Engineering  
**Compétences** : ETL, SQL, Python, Chargement incrémental, Multi-SGBD