from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'etl_admin',
    'start_date': datetime(2026, 1, 1),
    'email': ['admin@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='Pipeline ETL pour analyser les logs web',
    schedule_interval='@daily',
    catchup=False
)

# Tâche d'extraction - VERSION GARANTIE
extract_data = BashOperator(
    task_id='extract_data',
    bash_command="""
       
        # 1. Utiliser un répertoire avec garantie de permissions
        TMP_DIR="/tmp/airflow_etl_$$"
        mkdir -p "$TMP_DIR"
        cd "$TMP_DIR"
        
        # 2. Copier le fichier source
        cp /home/project/airflow/dags/capstone/accesslog.txt .
        
        # 3. Exécuter l'extraction
        cut -d" " -f1 accesslog.txt > extracted_data.txt
        
        # 4. Copier le résultat vers le répertoire final
        cp extracted_data.txt /home/project/airflow/dags/capstone/
        
        # 5. Nettoyer
        cd /
        rm -rf "$TMP_DIR"
        
        # 6. Vérification
        if [ -f "/home/project/airflow/dags/capstone/extracted_data.txt" ]; then
            echo "✅ EXTRACTION RÉUSSIE"
            echo "📊 Lignes extraites: $(wc -l < /home/project/airflow/dags/capstone/extracted_data.txt)"
            echo "📝 Extrait:"
            head -3 /home/project/airflow/dags/capstone/extracted_data.txt
        else
            echo "❌ ÉCHEC"
            exit 1
        fi
    """,
    dag=dag
)

# Tâche de transformation - VERSION GARANTIE
transform_data = BashOperator(
    task_id='transform_data',
    bash_command="""
        # === UTILISATION DE /tmp POUR LES PERMISSIONS ===
        TMP_DIR="/tmp/airflow_transform_$$"
        mkdir -p "$TMP_DIR"
        cd "$TMP_DIR"
        
        # Copier le fichier source
        cp /home/project/airflow/dags/capstone/extracted_data.txt .
        
        # Filtrer
        grep -v "198.46.149.143" extracted_data.txt > transformed_data.txt
        
        # Copier le résultat
        cp transformed_data.txt /home/project/airflow/dags/capstone/
        
        # Nettoyer et vérifier
        cd /
        rm -rf "$TMP_DIR"
        
        echo "✅ TRANSFORMATION RÉUSSIE"
        echo "📊 Avant: $(wc -l < /home/project/airflow/dags/capstone/extracted_data.txt)"
        echo "📊 Après: $(wc -l < /home/project/airflow/dags/capstone/transformed_data.txt)"
    """,
    dag=dag
)

# Tâche de chargement - VERSION GARANTIE
load_data = BashOperator(
    task_id='load_data',
    bash_command="""
        # === CRÉATION DIRECTE DANS LE RÉPERTOIRE ===
        cd /home/project/airflow/dags/capstone
        
        # Vérifier que le fichier existe
        if [ ! -f "transformed_data.txt" ]; then
            echo "❌ Fichier transformed_data.txt manquant"
            ls -la
            exit 1
        fi
        
        # Créer l'archive
        tar -cvf weblog.tar transformed_data.txt
        
        echo "✅ CHARGEMENT RÉUSSI"
        echo "📦 Archive créée: weblog.tar"
        echo "📏 Taille: $(ls -lh weblog.tar | awk '{print $5}')"
        echo "✅ TÂCHES TERMINÉES AVEC SUCCÈS"
    """,
    dag=dag
)

# Définir l'ordre d'exécution
extract_data >> transform_data >> load_data