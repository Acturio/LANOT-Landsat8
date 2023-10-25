from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime, timedelta


args = {
    'owner': 'Arturo Bringas',
    'email': 'arcturius@hotmail.com',
    'retries': 96,
    'retry_delay': timedelta(minutes = 15),
    'depends_on_past': False
}


dag = DAG(
    dag_id = '03_search_and_download_017_landsat8_scenes',
    default_args = args,
    schedule_interval=timedelta(days = 8),
    start_date=datetime(2023, 9, 30, 23, 55),
    catchup=True,
    tags = ["landsat_8-9", "sargassum", "path_17", "bash", "python"]
)

start_task = DummyOperator(
    task_id = 'start',
    dag = dag,
)

#### 017047 Process
# Download Scenes
task1_017047 = BashOperator(
    dag = dag,
    task_id = 'download_017047_scene',
    bash_command = """
        python /data/landsat/input/airflow/dags/src/download_data.py \
        --date={{ data_interval_end }} \
        --filetype='band' \
        --sceneId='017047' \
        --credPath='/data/landsat/input/airflow/auth/api_key.yaml' \
        --savePath='/data/landsat/output/data' \
        """
)
#### Compute FAI
task2_017047 = BashOperator(
    dag = dag,
    task_id = 'compute_017047_fai',
    bash_command = """
        python /data/landsat/input/airflow/dags/src/compute_fai.py \
        --geoid='017047' \
        --date={{ data_interval_end }} \
        --dataPath='/data/landsat/output/data'
        """,
    trigger_rule = 'one_success'
)


start_task >> task1_017047 >> task2_017047


if __name__ == "__main__":
    dag.cli()