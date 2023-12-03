from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    'owner': 'riste',
    'retries': 5,
    'start_date': datetime.utcnow(),
    'retry_delay': timedelta(minutes=5)
}


def justPrint():
    print("Hello there! General obi wan kenobi")
    
def taskToEnd():
    print("Goodbye Yoda!")
    
    
with DAG(
    'central_dag_await',
    default_args=default_args,
    description='Central DAG to trigger downstream DAG',
    schedule_interval=timedelta(days=1),
) as dag:

    start_task = PythonOperator(
        task_id='start_task',
        python_callable=justPrint
    )
    
    trigger_another_dag = TriggerDagRunOperator(
        task_id='trigger_another_dag',
        trigger_dag_id='Temperature_Forecast',
        dag=dag
    )
    
    # ExternalTaskSensor to wait for completion of a task in downstream DAG
    wait_for_task_sensor = ExternalTaskSensor(
        task_id='wait_for_task_sensor',
        external_dag_id='Temperature_Forecast',
        external_task_id='clean_temporary',
        timeout=3600,
        poke_interval=60,
        mode='poke',
        dag=dag,
    )
    
    end_task = PythonOperator(
        task_id='end_task',
        python_callable=taskToEnd
    )
    
    start_task >> trigger_another_dag >> wait_for_task_sensor >> end_task
