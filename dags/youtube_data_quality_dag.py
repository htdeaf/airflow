from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta
import os
import subprocess
import psycopg2

# =======================
# Configuration
# =======================
SODA_PROJECT_DIR = "/usr/local/airflow/soda"
SODA_EXEC = "soda"
DATA_SOURCE_NAME = "postgres"

# Database connection variables (adaptées à Postgres de ton Docker Airflow)
DB_HOST = os.getenv("PGHOST", "postgres")
DB_PORT = os.getenv("PGPORT", "5432")
DB_NAME = os.getenv("PGDATABASE", "airflow")
DB_USER = os.getenv("PGUSER", "postgres")
DB_PASS = os.getenv("PGPASSWORD", "postgres")

# =======================
# DAG Arguments
# =======================
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="youtube_data_quality_dag",
    default_args=default_args,
    description="Validation de la qualité des données YouTube avec Soda Core",
    schedule=None,  # pas planifié, il s’exécute après youtube_to_postgres_dag
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["youtube", "quality", "soda"],
)

# =======================
# Fonctions Helpers
# =======================
def check_database_connection():
    """
    Vérifie la connectivité à la base de données PostgreSQL
    """
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        cur = conn.cursor()
        cur.execute("SELECT 1")
        result = cur.fetchone()
        cur.close()
        conn.close()
        if result:
            return "Database connection successful"
        else:
            raise Exception("Database connection failed")
    except Exception as e:
        raise Exception(f"Database connection failed: {str(e)}")

def run_soda_scan():
    """
    Exécute le scan Soda Core pour valider la qualité des données
    """
    try:
        env = os.environ.copy()
        env.update({
            "PGHOST": DB_HOST,
            "PGPORT": DB_PORT,
            "PGDATABASE": DB_NAME,
            "PGUSER": DB_USER,
            "PGPASSWORD": DB_PASS,
        })

        cmd = [
            SODA_EXEC,
            "scan",
            "-d", DATA_SOURCE_NAME,
            "-c", f"{SODA_PROJECT_DIR}/soda_config.yml",
            f"{SODA_PROJECT_DIR}/youtube_videos.yml",
        ]

        print(f"Running command: {' '.join(cmd)}")

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            cwd=SODA_PROJECT_DIR,
            timeout=300
        )

        print(f"Soda scan stdout: {result.stdout}")
        print(f"Soda scan stderr: {result.stderr}")

        if result.returncode == 0:
            return "Soda scan completed successfully"
        else:
            return f"Soda scan completed with issues: {result.stdout}"

    except subprocess.TimeoutExpired:
        raise Exception("Soda scan timed out after 5 minutes")
    except Exception as e:
        raise Exception(f"Soda scan failed: {str(e)}")

# =======================
# Tasks
# =======================

# Attendre que le DAG 2 ait fini
wait_for_postgres_load = ExternalTaskSensor(
    task_id="wait_for_postgres_load",
    external_dag_id="youtube_to_postgres_dag",
    external_task_id=None,  # attendre la fin de tout le DAG
    poke_interval=60,
    timeout=600,
    dag=dag,
)

# Vérifier la connexion à la DB
check_db_task = PythonOperator(
    task_id="check_database_connection",
    python_callable=check_database_connection,
    dag=dag,
)

# Vérifier que Soda est bien installé
check_soda_task = BashOperator(
    task_id="check_soda_installation",
    bash_command=f"which {SODA_EXEC} || echo 'Soda not found in PATH'",
    dag=dag,
)

# Lancer le scan Soda
soda_scan_task = PythonOperator(
    task_id="run_soda_scan",
    python_callable=run_soda_scan,
    dag=dag,
)

# =======================
# Dépendances
# =======================
wait_for_postgres_load >> check_db_task >> check_soda_task >> soda_scan_task
