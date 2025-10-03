
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timezone
import os
import json


# ------------------ TASKS ------------------
@task()
def load_from_json():
    """Lire le dernier fichier JSON généré par le DAG 1."""
    # AMÉLIORATION : Il est préférable d'utiliser un dossier comme 'include/data'
    # mais nous gardons votre chemin pour l'instant.
    folder_path = "/usr/local/airflow/dags/data"
    file_name = f"YT_data_{datetime.now(timezone.utc).date()}.json"
    file_path = os.path.join(folder_path, file_name)

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"Fichier introuvable: {file_path}. Assurez-vous que le DAG d'extraction a bien tourné aujourd'hui.")

    with open(file_path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return data


@task()
def load_to_staging(data: list):
    """Charger les données brutes dans la table staging de manière idempotente."""
    pg_hook = PostgresHook(postgres_conn_id="pg_conn")

    # AMÉLIORATION : S'assurer que le schéma existe avant de créer la table.
    pg_hook.run("CREATE SCHEMA IF NOT EXISTS staging;")

    # AMÉLIORATION : Ajout d'une clé primaire sur video_id pour l'idempotence.
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS staging.youtube_raw (
        video_id TEXT PRIMARY KEY,
        title TEXT,
        published_at TIMESTAMP WITH TIME ZONE,
        duration TEXT,
        viewCount BIGINT,
        likeCount BIGINT,
        commentCount BIGINT,
        load_date TIMESTAMP DEFAULT NOW()
    );
    """
    pg_hook.run(create_table_sql)

    # AMÉLIORATION : Utilisation de ON CONFLICT pour rendre l'opération idempotente.
    # Si la vidéo existe, on met à jour ses statistiques.
    insert_sql = """
    INSERT INTO staging.youtube_raw (video_id, title, published_at, duration, viewCount, likeCount, commentCount)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (video_id) DO UPDATE SET
        title = EXCLUDED.title,
        published_at = EXCLUDED.published_at,
        duration = EXCLUDED.duration,
        viewCount = EXCLUDED.viewCount,
        likeCount = EXCLUDED.likeCount,
        commentCount = EXCLUDED.commentCount,
        load_date = NOW();
    """
    
    # AMÉLIORATION : On itère pour insérer les lignes une par une avec la gestion des conflits
    # et on gère les valeurs None qui peuvent venir de l'API.
    for item in data:
        row = (
            item.get("video_id"),
            item.get("title"),
            item.get("published_at"),
            item.get("duration"),
            item.get("viewCount"),
            item.get("likeCount"),
            item.get("commentCount"),
        )
        pg_hook.run(insert_sql, parameters=row)


@task()
def transform_to_core():
    """Créer une table 'core' nettoyée à partir des données 'staging'."""
    pg_hook = PostgresHook(postgres_conn_id="pg_conn")

    # AMÉLIORATION : S'assurer que le schéma existe.
    pg_hook.run("CREATE SCHEMA IF NOT EXISTS core;")

    # Le pattern "DROP TABLE / CREATE TABLE AS" est simple et efficace pour ce volume de données.
    transform_sql = """
    DROP TABLE IF EXISTS core.youtube_videos;
    CREATE TABLE core.youtube_videos AS
    SELECT
        video_id,
        title,
        published_at,
        duration, -- Pour aller plus loin, on pourrait convertir la durée ISO 8601 en secondes ici.
        COALESCE(viewCount, 0) AS view_count,
        COALESCE(likeCount, 0) AS like_count,
        COALESCE(commentCount, 0) AS comment_count
    FROM staging.youtube_raw
    WHERE published_at IS NOT NULL;
    """
    pg_hook.run(transform_sql)


# ------------------ DAG DEFINITION ------------------
with DAG(
    dag_id="youtube_to_postgres",
    schedule="@daily",
    start_date=datetime(2025, 9, 29),
    catchup=False,
    tags=["youtube", "etl", "postgres"],
) as dag:
    
    # AMÉLIORATION : Définition des dépendances claire et moderne.
    json_data = load_from_json()
    staging_task = load_to_staging(json_data)
    core_task = transform_to_core()

    trigger_quality = TriggerDagRunOperator(
        task_id="trigger_quality_checks",
        trigger_dag_id="youtube_data_quality",
        reset_dag_run=True,
        wait_for_completion=False
    )

    json_data >> staging_task >> core_task >> trigger_quality
    # json_data >> staging_task >> core_task 