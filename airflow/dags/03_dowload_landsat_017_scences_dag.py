### Download new TIF image from Landsat Satellite

from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


args = {
    'owner': 'Arturo Bringas',
    'email': 'arcturius@hotmail.com',
    'retries': 10,
    'retry_delay': timedelta(minutes = 15),
    'depends_on_past': True
}


dag = DAG(
    dag_id = '03_search_and_download_017_landsat8_scenes',
    default_args = args,
    schedule_interval=timedelta(days = 1),
    start_date=datetime(2023, 9, 22, 16),
    tags = ["landsat_8-9", "sargassum", "path_17", "bash", "python"]
)


#### 017 Scenes
task1 = BashOperator(
    dag = dag,
    task_id = 'download_017047_scene',
    bash_command = """
        python /data/landsat/input/airflow/dags/download_data.py \
        --date={{ ds }} \
        --filetype='band' \
        --sceneId='017047' \
        --credPath='/data/landsat/input/airflow/auth/api_key.yaml' \
        --savePath='/data/landsat/output/data' \
        """
)

task1


if __name__ == "__main__":
    dag.cli()