from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

default_args = {
    'owner': 'riste',
    'retries': 0,
    'start_date': datetime.utcnow(),
    'retry_delay': timedelta(minutes=5)
}


def justPrint():
    print("Hello there! General obi wan kenobi")
    
def taskToEnd():
    print("Goodbye Yoda!")
    
    
with DAG(
    'central_dag',
    default_args=default_args,
    description='Central DAG to trigger downstream DAG',
    schedule_interval=timedelta(days=1),
    dagrun_timeout=None,
    concurrency=5
) as dag:
    
    #region IOT-temp.csv
    # Define the S3KeySensor task to sense the change in the S3 file
    s3_sensor_task = S3KeySensor(
        task_id='data_1',
        bucket_name='airflow',
        bucket_key='IOT-temp.csv',  # Path to the file in the bucket
        # wildcard_match=True,  # Set to True if the key is a wildcard
        timeout=86000,  # Set the timeout as needed
        poke_interval=60,  # Set the poke_interval as needed
        mode='poke',  # Use 'poke' mode for S3KeySensor
        aws_conn_id='minio',  # Specify the Airflow connection for AWS credentials
        dag=dag,
    )
    
    trigger_iotTemp = TriggerDagRunOperator(
        task_id='pipeline_1',
        trigger_dag_id='Temperature_Forecast_S3_v3',
        dag=dag
    )
    #endregion
    
    
    #region data.csv
    # Define the S3KeySensor task to sense the change in the S3 file
    s3_sensor_task_data = S3KeySensor(
        task_id='data_2',
        bucket_name='airflow',
        bucket_key='data.csv',  # Path to the file in the bucket
        # wildcard_match=True,  # Set to True if the key is a wildcard
        timeout=86000,  # Set the timeout as needed
        poke_interval=60,  # Set the poke_interval as needed
        mode='poke',  # Use 'poke' mode for S3KeySensor
        aws_conn_id='minio',  # Specify the Airflow connection for AWS credentials
        dag=dag,
    )
    
    trigger_data = TriggerDagRunOperator(
        task_id='pipeline_2',
        trigger_dag_id='Temperature_Forecast_S3_Data',
        dag=dag
    )
    #endregion
    
    
    #region new_data.csv
    # Define the S3KeySensor task to sense the change in the S3 file
    s3_sensor_task_new_data = S3KeySensor(
        task_id='data_3',
        bucket_name='airflow',
        bucket_key='new_data.csv',  # Path to the file in the bucket
        # wildcard_match=True,  # Set to True if the key is a wildcard
        timeout=86000,  # Set the timeout as needed
        poke_interval=60,  # Set the poke_interval as needed
        mode='poke',  # Use 'poke' mode for S3KeySensor
        aws_conn_id='minio',  # Specify the Airflow connection for AWS credentials
        dag=dag,
    )
    
    trigger_new_data = TriggerDagRunOperator(
        task_id='pipeline_3',
        trigger_dag_id='Temperature_Forecast_S3_New_Data',
        dag=dag
    )
    #endregion



    s3_sensor_task >> trigger_iotTemp
    s3_sensor_task_data >> trigger_data
    s3_sensor_task_new_data >> trigger_new_data
    # start_task = PythonOperator(
    #     task_id='start_task',
    #     python_callable=justPrint
    # )
    
    # trigger_another_dag = TriggerDagRunOperator(
    #     task_id='trigger_another_dag',
    #     trigger_dag_id='Temperature_Forecast',
    #     dag=dag
    # )
    
    # end_task = PythonOperator(
    #     task_id='end_task',
    #     python_callable=taskToEnd
    # )
    
    # start_task >> trigger_another_dag >> end_task
