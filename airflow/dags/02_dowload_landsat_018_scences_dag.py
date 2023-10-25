### Download new TIF image from Landsat Satellite

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.timetables.trigger import CronTriggerTimetable
from airflow.timetables.interval import CronDataIntervalTimetable
from datetime import datetime, timedelta
import pendulum


args = {
    'owner': 'Arturo Bringas',
    'email': 'arcturius@hotmail.com',
    'retries': 96,
    'retry_delay': timedelta(minutes = 15),
    'depends_on_past': False
}

def cron_timetable(*args, **kwargs):
    return CronDataIntervalTimetable(kwargs['cron_expression'])


dag = DAG(
    dag_id = '02_search_and_download_018_landsat8_scenes',
    default_args = args,
    schedule_interval=timedelta(days = 8),
    start_date=datetime(2023, 9, 29, 23, 55),
    catchup = True,
    tags = ["landsat_8-9", "sargassum", "path_18", "bash", "python"]
)


start_task = DummyOperator(
    task_id = 'start',
    dag = dag,
)

#### 018045 Process ####
#### Download Scenes
task1_018045 = BashOperator(
    dag = dag,
    task_id = 'download_018045_scene',
    bash_command = """
        python /data/landsat/input/airflow/dags/src/download_data.py \
        --date={{ data_interval_end }} \
        --filetype='band' \
        --sceneId='018045' \
        --credPath='/data/landsat/input/airflow/auth/api_key.yaml' \
        --savePath='/data/landsat/output/data' \
        """
)
#### Compute FAI
task2_018045 = BashOperator(
    dag = dag,
    task_id = 'create_018045_landmask',
    bash_command = """
        python /data/landsat/input/airflow/dags/src/landmask.py \
        --geoid='018045' \
        --date={{ data_interval_end }} \
        --dataPath='/data/landsat/output/data'
        """,
    trigger_rule = 'one_success'
)
#### Create Landmask
task3_018045 = BashOperator(
    dag = dag,
    task_id = 'compute_018045_fai',
    bash_command = """
        python /data/landsat/input/airflow/dags/src/compute_fai.py \
        --geoid='018045' \
        --date={{ data_interval_end }} \
        --dataPath='/data/landsat/output/data'
        """,
    trigger_rule = 'one_success'
)


#### 018046 Process
# Download Scenes
task1_018046 = BashOperator(
    dag = dag,
    task_id = 'download_018046_scene',
    bash_command = """
        python /data/landsat/input/airflow/dags/src/download_data.py \
        --date={{ data_interval_end }} \
        --filetype='band' \
        --sceneId='018046' \
        --credPath='/data/landsat/input/airflow/auth/api_key.yaml' \
        --savePath='/data/landsat/output/data' \
        """
)
#### Create Landmask
task2_018046 = BashOperator(
    dag = dag,
    task_id = 'create_018046_landmask',
    bash_command = """
        python /data/landsat/input/airflow/dags/src/landmask.py \
        --geoid='018046' \
        --date={{ data_interval_end }} \
        --dataPath='/data/landsat/output/data'
        """,
    trigger_rule = 'one_success'
)
#### Compute FAI
task3_018046 = BashOperator(
    dag = dag,
    task_id = 'compute_018046_fai',
    bash_command = """
        python /data/landsat/input/airflow/dags/src/compute_fai.py \
        --geoid='018046' \
        --date={{ data_interval_end }} \
        --dataPath='/data/landsat/output/data'
        """,
    trigger_rule = 'one_success'
)



#### 018047 Process
# Download Scenes
task1_018047 = BashOperator(
    dag = dag,
    task_id = 'download_018047_scene',
    bash_command = """
        python /data/landsat/input/airflow/dags/src/download_data.py \
        --date={{ data_interval_end }} \
        --filetype='band' \
        --sceneId='018047' \
        --credPath='/data/landsat/input/airflow/auth/api_key.yaml' \
        --savePath='/data/landsat/output/data' \
        """
)
#### Create Landmask
task2_018047 = BashOperator(
    dag = dag,
    task_id = 'create_018047_landmask',
    bash_command = """
        python /data/landsat/input/airflow/dags/src/landmask.py \
        --geoid='018047' \
        --date={{ data_interval_end }} \
        --dataPath='/data/landsat/output/data'
        """,
    trigger_rule = 'one_success'
)
#### Compute FAI
task3_018047 = BashOperator(
    dag = dag,
    task_id = 'compute_018047_fai',
    bash_command = """
        python /data/landsat/input/airflow/dags/src/compute_fai.py \
        --geoid='018047' \
        --date={{ data_interval_end }} \
        --dataPath='/data/landsat/output/data'
        """,
    trigger_rule = 'one_success'
)


start_task >> task1_018045 >> task2_018045 >> task3_018045
start_task >> task1_018046 >> task2_018046 >> task3_018046
start_task >> task1_018047 >> task2_018047 >> task3_018047

if __name__ == "__main__":
    dag.cli()