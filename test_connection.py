# test_connection.py
import psycopg2
import os

def test_postgres_connection():
    try:
        # Paramètres de connexion
        dbname = os.getenv("ELT_DATABASE_NAME", "postgres")
        user = os.getenv("ELT_DATABASE_USERNAME", "postgres")
        password = os.getenv("ELT_DATABASE_PASSWORD", "postgres")
        host = os.getenv("POSTGRES_CONN_HOST", "localhost")
        port = os.getenv("POSTGRES_CONN_PORT", "5432")
        
        print(f"Tentative de connexion à {host}:{port}")
        print(f"Base de données : {dbname}")
        print(f"Utilisateur : {user}")
        
        # Connexion
        conn = psycopg2.connect(
            dbname=dbname, 
            user=user, 
            password=password, 
            host=host, 
            port=port
        )
        
        cursor = conn.cursor()
        cursor.execute("SELECT version();")
        version = cursor.fetchone()
        
        print("Connexion réussie !")
        print(f"Version PostgreSQL : {version[0]}")
        
        cursor.close()
        conn.close()
        return True
        
    except Exception as e:
        print(f"Erreur de connexion : {e}")
        return False

if __name__ == "__main__":
    test_postgres_connection()
