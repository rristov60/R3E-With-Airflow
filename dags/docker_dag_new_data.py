import base64
import codecs
import pickle
import docker
import shutil
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.docker.operators.docker import DockerOperator


default_args = {
    'owner': 'riste',
    'retries': 0,
    'start_date': datetime.utcnow(),
    'retry_delay': timedelta(minutes=5)
}


def get_model(**kwargs):
    s3_hook = S3Hook(aws_conn_id='minio')
    key=f'models/{kwargs["ds"]}_train_out_new_data.pkl',
    
    serialized_model = s3_hook.read_key(
        key=f'models/{kwargs["ds"]}_train_out_new_data.pkl',
        bucket_name='airflow'
    )
    
    print(serialized_model)
    
    decoded_bytes = base64.b64decode(serialized_model)
    
    deserialized_model = pickle.loads(bytes(decoded_bytes))
    
    local_file_path="/home/riste/Documents/Aalto/Advanced_Topic_CS/ApacheAirflow/Docker/new_data/model.pkl"
    
    
    with open(local_file_path, 'wb') as local_file:
        pickle.dump(deserialized_model, local_file)
    

def build_image(**kwargs):
    client = docker.from_env()
    image, build_logs = client.images.build(
        path="/home/riste/Documents/Aalto/Advanced_Topic_CS/ApacheAirflow/Docker/new_data",
        tag=f'train_out_new_data:{kwargs["ds"]}',
        buildargs={'PICKLE_FILE_PATH': '/home/riste/Documents/Aalto/Advanced_Topic_CS/ApacheAirflow/Docker/new_data/model.pkl', 'PORT': '8081'}
    )
    
def stop_and_delete_container(**kwargs):
    client = docker.from_env()
    container_name = 'train_out_new_data'
    
    try:
        container = client.containers.get(container_name)
        container.stop()
        container.remove()
    except docker.errors.NotFound:
        return
    

def start_container(**kwargs):
    client = docker.from_env()
    container_name = 'train_out_new_data'

    client.containers.run(
        image=f'train_out_new_data:{kwargs["ds"]}',
        name=container_name,
        detach=True
    )
    

with DAG(
    'docker_dag_new_data',
    default_args=default_args,
    description='Docker DAG to build and deploy docker',
    schedule_interval=timedelta(days=1),
    dagrun_timeout=None,
    concurrency=5
) as dag:
    
    get_model_task = PythonOperator(
        task_id='get_model',
        python_callable=get_model
    )
    
    build_image_task = PythonOperator(
        task_id='build_image',
        python_callable=build_image
    )
    
    stop_and_delete_container_task = PythonOperator(
        task_id='stop_and_delete_container',
        python_callable=stop_and_delete_container
    )
    
    start_container_task = PythonOperator(
        task_id='start_container',
        python_callable=start_container
    ) 
    
    get_model_task >> build_image_task >> stop_and_delete_container_task >> start_container_task