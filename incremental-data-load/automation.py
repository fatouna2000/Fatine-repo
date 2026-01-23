# automation.py - Exercice 1: Automatisation du chargement incrémentiel
# Import libraries required for connecting to mysql
import mysql.connector
import sys

# Import libraries required for connecting to DB2 or PostgreSql
import psycopg2

# ==================== CONFIGURATION ====================
# À COMPLÉTER avec vos valeurs
MYSQL_CONFIG = {
    'user': 'root',
    'password': 'K6k4FpnyaXR2ge2b6xeDZBnu',  # Votre mot de passe MySQL
    'host': '172.21.59.4',                 # Votre hôte MySQL
    'database': 'sales',
    'port': '3306'
}

POSTGRES_CONFIG = {
    'host': '172.21.192.236',                     # Localhost pour PostgreSQL
    'database': 'postgres',
    'user': 'postgres',
    'password': '34KqX4Lhnxx6K1QKivbU036B',                  # Mot de passe PostgreSQL
    'port': '5432'
}
# =======================================================

# ==================== CONNEXIONS ====================
# Connect to MySQL
try:
    mysql_conn = mysql.connector.connect(**MYSQL_CONFIG)
    mysql_cursor = mysql_conn.cursor(dictionary=True)  # Pour avoir les résultats en dictionnaire
    print("✅ Connexion MySQL établie")
except mysql.connector.Error as err:
    print(f"❌ Erreur de connexion MySQL: {err}")
    sys.exit(1)

# Connect to DB2 or PostgreSql
try:
    postgres_conn = psycopg2.connect(**POSTGRES_CONFIG)
    postgres_cursor = postgres_conn.cursor()
    print("✅ Connexion PostgreSQL établie")
except psycopg2.Error as err:
    print(f"❌ Erreur de connexion PostgreSQL: {err}")
    sys.exit(1)

# ==================== TÂCHE 1 ====================
# Find out the last rowid from DB2 data warehouse or PostgreSql data warehouse
# The function get_last_rowid must return the last rowid of the table sales_data on the IBM DB2 database or PostgreSql.
def get_last_rowid():
    """
    À COMPLÉTER:
    1. Exécuter une requête SQL pour obtenir le MAX(rowid) de la table sales_data
    2. Retourner cette valeur, ou 0 si la table est vide
    """
    try:
        # À COMPLÉTER: Exécuter la requête SQL
        postgres_cursor.execute("SELECT MAX(rowid) FROM sales_data")
        
        # À COMPLÉTER: Récupérer le résultat
        result = postgres_cursor.fetchone()
        
        # À COMPLÉTER: Retourner la valeur ou 0 si None
        if result and result[0] is not None:
            return result[0]
        else:
            return 0
    except Exception as e:
        print(f"Erreur dans get_last_rowid: {e}")
        return 0

last_row_id = get_last_rowid()
print("Last row id on production datawarehouse = ", last_row_id)

# ==================== TÂCHE 2 ====================
# List out all records in MySQL database with rowid greater than the one on the Data warehouse
# The function get_latest_records must return a list of all records that have a rowid greater than the last_row_id in the sales_data table in the sales database on the MySQL staging data warehouse.
def get_latest_records(rowid):
    """
    À COMPLÉTER:
    1. Exécuter une requête SQL pour obtenir les lignes avec rowid > rowid
    2. Retourner toutes les lignes trouvées
    """
    try:
        # À COMPLÉTER: Créer la requête SQL
        query = """
        SELECT rowid, product_id, customer_id, quantity
        FROM sales_data 
        WHERE rowid > %s
        ORDER BY rowid
        """
        
        # À COMPLÉTER: Exécuter la requête avec le paramètre
        mysql_cursor.execute(query, (rowid,))
        
        # À COMPLÉTER: Récupérer tous les résultats
        records = mysql_cursor.fetchall()
        return records
    except Exception as e:
        print(f"Erreur dans get_latest_records: {e}")
        return []

new_records = get_latest_records(last_row_id)
print("New rows on staging datawarehouse = ", len(new_records))

# Optionnel: Afficher un aperçu
if new_records and len(new_records) > 0:
    print("\nAperçu des nouvelles lignes (3 premières):")
    for i, record in enumerate(new_records[:3]):
        print(f"  Ligne {i+1}: ID={record['rowid']}, Produit={record['product_id']}, "
              f"Client={record['customer_id']}, Quantité={record['quantity']}")
    if len(new_records) > 3:
        print(f"  ... et {len(new_records)-3} autres")

# ==================== TÂCHE 3 ====================
# Insert the additional records from MySQL into DB2 or PostgreSql data warehouse.
# The function insert_records must insert all the records passed to it into the sales_data table in IBM DB2 database or PostgreSql.
def insert_records(records):
    """
    À COMPLÉTER:
    1. Pour chaque record dans records, l'insérer dans PostgreSQL
    2. Utiliser ON CONFLICT pour éviter les doublons
    3. N'insérer que les 4 colonnes disponibles, PostgreSQL ajoutera les autres via DEFAULT
    """
    if not records:
        print("Aucun enregistrement à insérer")
        return
    
    try:
        # À COMPLÉTER: Créer la requête d'insertion
        # IMPORTANT: Insérer seulement 4 colonnes, PostgreSQL utilisera les DEFAULT
        insert_query = """
        INSERT INTO sales_data (rowid, product_id, customer_id, quantity)
        VALUES (%s, %s, %s, %s)
        ON CONFLICT (rowid) DO NOTHING
        """
        
        inserted_count = 0
        
        # À COMPLÉTER: Pour chaque record, l'insérer
        for record in records:
            # À COMPLÉTER: Préparer les valeurs (4 valeurs seulement)
            values = (
                record['rowid'],
                record['product_id'],
                record['customer_id'],
                record['quantity']
                # price et timeestamp seront ajoutés AUTOMATIQUEMENT par PostgreSQL
                # grâce aux DEFAULT dans la table
            )
            
            # À COMPLÉTER: Exécuter l'insertion
            postgres_cursor.execute(insert_query, values)
            inserted_count += 1
        
        # À COMPLÉTER: Valider les changements
        postgres_conn.commit()
        print(f"✅ {inserted_count} lignes insérées avec succès")
        
    except Exception as e:
        print(f"❌ Erreur dans insert_records: {e}")
        # À COMPLÉTER: En cas d'erreur, annuler les changements
        postgres_conn.rollback()

# Exécuter l'insertion
insert_records(new_records)
print("New rows inserted into production datawarehouse = ", len(new_records))

# ==================== VÉRIFICATION ====================
# Vérification supplémentaire
if new_records:
    postgres_cursor.execute("SELECT MAX(rowid) FROM sales_data")
    new_max = postgres_cursor.fetchone()[0]
    print(f"New last row id in production = {new_max}")
    
    postgres_cursor.execute("SELECT COUNT(*) FROM sales_data")
    total = postgres_cursor.fetchone()[0]
    print(f"Total rows in production = {total}")

# ==================== DÉCONNEXION ====================
# disconnect from mysql warehouse
mysql_cursor.close()
mysql_conn.close()
print("✅ Déconnexion MySQL")

# disconnect from DB2 or PostgreSql data warehouse 
postgres_cursor.close()
postgres_conn.close()
print("✅ Déconnexion PostgreSQL")

# End of program
print("\n" + "="*60)
print("PROGRAMME TERMINÉ AVEC SUCCÈS")
print("="*60)