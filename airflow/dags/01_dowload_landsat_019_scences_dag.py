### Download new TIF image from Landsat Satellite

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import pendulum


args = {
    'owner': 'Arturo Bringas',
    'email': 'arcturius@hotmail.com',
    'retries': 96,
    'retry_delay': timedelta(minutes = 15),
    'depends_on_past': False
}


dag = DAG(
    dag_id = '01_search_and_download_019_landsat8_scenes',
    default_args = args,
    schedule_interval=timedelta(days = 8),
    start_date=datetime(2023, 9, 28, 23, 55),
    catchup = True,
    tags = ["landsat_8-9", "sargassum", "path_19", "bash", "python"]
)


start_task = DummyOperator(
    task_id = 'start',
    dag = dag,
)


#### 019045 Process ####
#### Download Scenes
task1_019045 = BashOperator(
    dag = dag,
    task_id = 'download_019045_scene',
    bash_command = """
        python /data/landsat/input/airflow/dags/src/download_data.py \
        --date={{ data_interval_end }} \
        --filetype='band' \
        --sceneId='019045' \
        --credPath='/data/landsat/input/airflow/auth/api_key.yaml' \
        --savePath='/data/landsat/output/data' \
        """
)
#### Compute FAI
task2_019045 = BashOperator(
    dag = dag,
    task_id = 'create_019045_landmask',
    bash_command = """
        python /data/landsat/input/airflow/dags/src/landmask.py \
        --geoid='019045' \
        --date={{ data_interval_end }} \
        --dataPath='/data/landsat/output/data'
        """,
    trigger_rule = 'one_success'
)
#### Create Landmask
task3_019045 = BashOperator(
    dag = dag,
    task_id = 'compute_019045_fai',
    bash_command = """
        python /data/landsat/input/airflow/dags/src/compute_fai.py \
        --geoid='019045' \
        --date={{ data_interval_end }} \
        --dataPath='/data/landsat/output/data'
        """,
    trigger_rule = 'one_success'
)


#### 019046 Process
# Download Scenes
task1_019046 = BashOperator(
    dag = dag,
    task_id = 'download_019046_scene',
    bash_command = """
        python /data/landsat/input/airflow/dags/src/download_data.py \
        --date={{ data_interval_end }} \
        --filetype='band' \
        --sceneId='019046' \
        --credPath='/data/landsat/input/airflow/auth/api_key.yaml' \
        --savePath='/data/landsat/output/data' \
        """
)
#### Create Landmask
task2_019046 = BashOperator(
    dag = dag,
    task_id = 'create_019046_landmask',
    bash_command = """
        python /data/landsat/input/airflow/dags/src/landmask.py \
        --geoid='019046' \
        --date={{ data_interval_end }} \
        --dataPath='/data/landsat/output/data'
        """,
    trigger_rule = 'one_success'
)
#### Compute FAI
task3_019046 = BashOperator(
    dag = dag,
    task_id = 'compute_019046_fai',
    bash_command = """
        python /data/landsat/input/airflow/dags/src/compute_fai.py \
        --geoid='019046' \
        --date={{ data_interval_end }} \
        --dataPath='/data/landsat/output/data'
        """,
    trigger_rule = 'one_success'
)


start_task >> task1_019045 >> task2_019045 >> task3_019045
start_task >> task1_019046 >> task2_019046 >> task3_019046

if __name__ == "__main__":
    dag.cli()