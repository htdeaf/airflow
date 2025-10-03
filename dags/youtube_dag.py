from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import os
from datetime import datetime, timezone
import requests
import json

# Variables Airflow
API_KEY = Variable.get("API_KEY", default_var="TA_CLE_API")
CHANNEL_HANDLE = Variable.get("CHANNEL_HANDLE", default_var="MrBeast")
MAX_RESULTS = 50

# ------------------ TASKS ------------------
@task()
def get_playlist_id():
    url = f"https://youtube.googleapis.com/youtube/v3/channels?part=contentDetails&forHandle={CHANNEL_HANDLE}&key={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    playlist_id = data["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    return playlist_id

@task()
def get_video_ids(playlistId: str):
    video_ids = []
    pageToken = None
    base_url = f"https://youtube.googleapis.com/youtube/v3/playlistItems?part=contentDetails&maxResults={MAX_RESULTS}&playlistId={playlistId}&key={API_KEY}"

    while True:
        url = base_url
        if pageToken:
            url += f"&pageToken={pageToken}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        for item in data.get("items", []):
            video_ids.append(item["contentDetails"]["videoId"])

        pageToken = data.get("nextPageToken")
        if not pageToken:
            break

    return video_ids

@task()
def extract_video_data(video_ids: list):
    extracted_data = []

    def batch_list(video_id_lst, batch_size):
        for i in range(0, len(video_id_lst), batch_size):
            yield video_id_lst[i : i + batch_size]

    for batch in batch_list(video_ids, MAX_RESULTS):
        video_ids_str = ",".join(batch)
        url = f"https://youtube.googleapis.com/youtube/v3/videos?part=contentDetails&part=snippet&part=statistics&id={video_ids_str}&key={API_KEY}"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        for item in data.get("items", []):
            video_data = {
                "video_id": item["id"],
                "title": item["snippet"]["title"],
                "published_at": item["snippet"]["publishedAt"],
                "duration": item["contentDetails"]["duration"],
                "viewCount": item["statistics"].get("viewCount"),
                "likeCount": item["statistics"].get("likeCount"),
                "commentCount": item["statistics"].get("commentCount"),
            }
            extracted_data.append(video_data)

    return extracted_data

@task()
def save_to_json(extracted_data: list):
    folder_path = "/usr/local/airflow/dags/data"
    os.makedirs(folder_path, exist_ok=True)

    file_path = f"{folder_path}/YT_data_{datetime.now(timezone.utc).date()}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(extracted_data, f, indent=4, ensure_ascii=False)
    return file_path

# ------------------ DAG DEFINITION ------------------
with DAG(
    dag_id="youtube_data_pipeline",
    schedule="@daily",  # lance chaque jour
    start_date=datetime(2025, 9, 28),
    catchup=False,
    tags=["youtube", "etl"],
) as dag:

    playlist_id = get_playlist_id()
    video_ids = get_video_ids(playlist_id)
    video_data = extract_video_data(video_ids)
    json_file = save_to_json(video_data)

    # 🔥 Déclencher le DAG youtube_to_postgres une fois que l'export JSON est fini
    trigger_postgres = TriggerDagRunOperator(
        task_id="trigger_youtube_to_postgres",
        trigger_dag_id="youtube_to_postgres",  # doit correspondre au dag_id dans youtube_to_postgres_dag.py
        wait_for_completion=False,
    )

    json_file >> trigger_postgres
