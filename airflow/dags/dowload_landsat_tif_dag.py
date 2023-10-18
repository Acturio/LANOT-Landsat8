### Download new TIF image from Landsat Satellite

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta


args = {
    'owner': 'Arturo Bringas',
    'email': 'arcturius@hotmail.com',
    'retries': 5,
    'retry_delay': timedelta(minutes = 45),
    'depends_on_past': True
}

start_date_task1 = datetime(2023, 9, 28)
start_date_task2 = datetime(2023, 9, 29)
start_date_task3 = datetime(2023, 9, 30)


dag = DAG(
    dag_id = '01_search_and_download_new_landsat_scenes',
    default_args = args,
    schedule_interval=timedelta(days = 8),
    start_date=datetime(2023, 9, 28, 18), 
    tags = ["landsat_8-9", "sargassum", "bash", "python"],
    catchup = False
)


#### 019 Scenes
task1 = BashOperator(
    dag = dag,
    task_id = 'get_019045_scene',
    bash_command = """
        python /data/landsat/input/airflow/dags/download_data.py \
        --date={{ ds }} \
        --filetype='band' \
        --sceneId='019045' \
        --credPath='/data/landsat/input/airflow/auth/api_key.yaml' \
        --savePath='/data/landsat/output/data' \
        """
)

task2 = BashOperator(
    dag = dag,
    task_id = 'get_019046_scene',
    bash_command = """
        python /data/landsat/input/airflow/dags/download_data.py \
        --date={{ ds }} \
        --filetype='band' \
        --sceneId='019046' \
        --credPath='/data/landsat/input/airflow/auth/api_key.yaml' \
        --savePath='/data/landsat/output/data' \
        """
)

#### 018 Scenes
task3 = BashOperator(
    dag = dag,
    task_id = 'get_018045_scene',
    bash_command = """
        python /data/landsat/input/airflow/dags/download_data.py \
        --date={{ ds }} \
        --filetype='band' \
        --sceneId='018045' \
        --credPath='/data/landsat/input/airflow/auth/api_key.yaml' \
        --savePath='/data/landsat/output/data' \
        """
)

task4 = BashOperator(
    dag = dag,
    task_id = 'get_018046_scene',
    bash_command = """
        python /data/landsat/input/airflow/dags/download_data.py \
        --date={{ ds }} \
        --filetype='band' \
        --sceneId='018046' \
        --credPath='/data/landsat/input/airflow/auth/api_key.yaml' \
        --savePath='/data/landsat/output/data' \
        """
)

task5 = BashOperator(
    dag = dag,
    task_id = 'get_018047_scene',
    bash_command = """
        python /data/landsat/input/airflow/dags/download_data.py \
        --date={{ ds }} \
        --filetype='band' \
        --sceneId='018047' \
        --credPath='/data/landsat/input/airflow/auth/api_key.yaml' \
        --savePath='/data/landsat/output/data' \
        """
)

#### 017 Scenes
task6 = BashOperator(
    dag = dag,
    task_id = 'get_017047_scene',
    bash_command = """
        python /data/landsat/input/airflow/dags/download_data.py \
        --date={{ ds }} \
        --filetype='band' \
        --sceneId='017047' \
        --credPath='/data/landsat/input/airflow/auth/api_key.yaml' \
        --savePath='/data/landsat/output/data' \
        """
        )


trigger_task1 = TriggerDagRunOperator(
    task_id='download_scenes_019',
    trigger_dag_id='01_search_and_download_new_landsat_scenes',  
    dag=dag,
    execution_date=start_date_task1,
    conf={"message": "download_scenes_019"},
)

trigger_task2 = TriggerDagRunOperator(
    task_id='download_scenes_018',
    trigger_dag_id='01_search_and_download_new_landsat_scenes',
    dag=dag,
    execution_date=start_date_task2,
    conf={"message": "download_scenes_018"},
)

trigger_task3 = TriggerDagRunOperator(
    task_id='download_scenes_017',
    trigger_dag_id='01_search_and_download_new_landsat_scenes', 
    dag=dag,
    execution_date=start_date_task3,
    conf={"message": "download_scenes_017"},
)


trigger_task1 >> task1
trigger_task1 >> task2
trigger_task2 >> task3
trigger_task2 >> task4
trigger_task2 >> task5
trigger_task3 >> task6

if __name__ == "__main__":
    dag.cli()