# Robustness, Resilience, Reliability, and Elasticity (R3E) for Big Data ML pipelines with Apache Airflow

## Startup instructions
Prerequisites:
- Apache Airflow
- PostgreSQL 
- MinIO. 

After the installation of these prerequisites, the following commands can be run:

```bash
$ bash init.sh # For initializing Apache Airflow DB and creating an initial admin user 
$ bash start.sh # Running MinIO, Airflow Webserver, and Airflow Scheduler 
```

### Content
1. Introduction
2. Related Technology
3. Apache Airflow for R3E
4. Conclusion


## 1. Introduction
Artificial Intelligence (AI) projects and(or) products are thriving more than ever. The best example of this is OpenAI's Chat-GPT with how it changed the world. The key component that enables engineers to develop such projects is called Machine Learning (ML). ML is just a collection of algorithms that use various amounts of data to train an ML model which is later used to perform a certain task. Due to the nature of this process and the fact that for a good model, huge amounts of data are required, the model training process needs to be broken into smaller steps, such as data formatting, preprocessing the data, training the model, testing the model, deploying the model, and distributing it. Furthermore, these steps are usually organized in pipelines and each step is a certain stage in the pipeline, for the pipeline execution to be successful, each task or rather the whole pipeline needs to have certain properties such as robustness, resilience, reliability, and elasticity (R3E). The vast amount of data used in this kind of pipeline is known as Big Data. This research explores whether the aforementioned properties can be achieved using Apache Airflow for Big Data ML pipelines. 

## 2. Related Technology
What is Apache Airflow? According to their [website](https://airflow.apache.org/docs/apache-airflow/stable/index.html), "Apache Airflow is an open-source platform for developing, scheduling, and monitoring batch-oriented workflows". Airflow is based on Python, which means that almost all Python modules can be utilized. This makes it an excellent tool for creating ML pipelines. Airflow uses a principle called "workflows as code", which means that all of the workflows are defined as code. The approach "workflow as code", according to Airflow's website, "serves several purposes":

- **Dynamic** - All pipelines are configured as Python code, which allows dynamic pipeline generation
- **Extensible** - "The Airflow™ framework contains operators to connect with numerous technologies. All Airflow components are extensible to easily adjust to your environment."
- **Flexible** - "Workflow parameterization is leveraging the built-in [Jinja](https://jinja.palletsprojects.com/) templating engine.".

### Apache Airflow architecture
A workflow in Airflow is represented as a **DAG**(Directed Acycling Graph) which is comprised of tasks. These tasks are "arranged with dependencies and data flows taken into account". Below is a small diagram from an example DAG taken from Airflow's website:

#### **Figure 1.** Example DAG diagram
![Figure 1. Example DAG diagram](./figures/Figure1.png) 

The DAG describes the order in which tasks are executed (also known as tasks flow), task run retries, and the dependencies between the tasks within the DAG. The tasks on the other hand describe the actual operation, i.e. code execution. Each task can be an instance of a certain operator. Operator in Airflow defines what kind of script or code is being executed. For example, there is __BashOperator__ which executes bash scripts, and __PythonOperator__ which executes Python code. Tasks' capabilities can be further extended with so-called providers. The providers are Python packages that extend the core of Apache Airflow, thus extending the tasks' capabilities. Following is an example of DAG:
```python
# Imports
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator # Bash operator
from airflow.operators.python import PythonOperator # Python operator

# Default DAG arguments
default_args = {
    'owner': 'riste',
    'retries': 0,
    'start_date': datetime(2022, 1, 1),
    'retry_delay': timedelta(minutes=5)
}

# Python function
def print_message(**kwargs):
    print("Greetings!")

# Airflow DAG
with DAG(
    default_args=default_args,
    dag_id='example_dag',
    description='Example DAG helping familiarize with Airflow terms',
    schedule_interval='@daily'
) as dag:
    # Task 1
    python_task = PythonOperator(
        task_id='greet',
        python_callable=greet,
    )
    # Task 2
    bash_task = BashOperator(
        task_id='introduce',
        bash_command="echo My name is Riste! What is your name?"
    )

    # Task flow
    python_task.set_downstream(bash_task) # or python_task >> bash_task
```

Airflow as a whole, consists of multiple components:
- **Scheduler** - handles the workflows scheduling and triggering, and submitting the tasks to the executor. Multiple types of schedulers differ for example whether the task execution is sequential or there can be parallel task execution 
- **Executor** - runs tasks. In a default configuration, all of the tasks are run "inside" the scheduler, but in a real production environment, the executor pushes task execution to external workers
- **Webserver** - Airflow's UI used to inspect, trigger, and debug DAGs and tasks
- **Folder of DAG files**
- **Metadata Database** - used for storing states by the scheduler, executor, and the web server

#### **Figure 2.** Airflow Infrastructure
![Figure 2. Airflow Infratrcutre](./figures/Figure2.png)

When running airflow, there is an automatically generated file called `airflow.cfg`, which is the configuration file for Airflow and all of its components. For further reference on how Airflow is installed please see [the official documentation](https://airflow.apache.org/docs/). For how a database is initialized, and how the components are run, please see [`start.sh`](https://github.com/rristov60/R3E-With-Airflow/blob/main/start.sh) located in the root folder of this project or [the official documentation](https://airflow.apache.org/docs/).

Before moving forward, a quick note should be made that in the context of this research the terms DAG and ML pipeline are interchangeable and are used with the same context.

### MinIO
According to the MinIO's documentation, MinIO "is a high-performance, S3 compatible object store. It is built for large-scale AI/ML, data lake, and database workloads. It is software-defined and runs on any cloud or on-premises infrastructure". MinIO is used as S3 storage in this experiment. MinIO is AWS provider compatible, which means that Airflow does not require any additional configuration to be used with MinIO other than the default AWS-S3 one.

## 3. Apache Airflow for R3E
As mentioned in the introduction, the main motivation for this research is to explore whether Apache Airflow can be used to create a Big Data ML pipeline that is robust, resilient, reliable, and elastic. To research this, an example ML project was used which was transformed into a pipeline. The utilized project is  [IoT Temperature Forecasting](https://www.kaggle.com/code/koheimuramatsu/iot-temperature-forecasting) by [_kohei-mu_](https://www.kaggle.com/koheimuramatsu). The data used in this project is not typically considered big data. However, dummy data was added to the initial dataset so the data reached 5GBs. Again, this is not considered typical big data, but it suffices to prove a point. Figure 3. represents the structure of the ML pipeline. 

#### **Figure 3.** Airflow ML pipeline
![Figure 3. Airflow ML pipeline](./figures/Figure3.png)

Apache Airflow has **no** direct way for passing data bigger than 48KB from one task to another. This requires some "out of the box" thinking on how the pipeline should be structured. The most common way to solve this challenge is to utilize an S3 instance with Airflow's [AWS provider](https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/index.html). 

This challenges R3E because it introduces more dependencies which require additional managing and more potential failure points, hence making the R3E implementation more challenging.

The pipeline shown in Figure 3 consisted of several tasks, i.e. `start_task` - which is used as a trigger by another DAG just to start the pipeline, `preprocess_data` - which fetches raw data from an S3 instance, preprocesses it, and puts the result back into the same instance, `data_prep` - which fetches the result from the previous task from S3, prepares it for the training of the ML model and saves the prepared data back to S3, `train` - fetches the result from the previous task from S3, trains the model and saves it as `.pkl` in an S3 instance, `marked_current_data_as_processed` - which marks the current data as processed in S3 and deletes some obsolete files created during the whole process and finally `deploy_model` - which triggers deployment pipeline ([docker_dag.py](https://github.com/rristov60/R3E-With-Airflow/blob/main/dags/docker_dag.py)). The complete ML pipeline can be found in the following [file](https://github.com/rristov60/R3E-With-Airflow/blob/main/dags/dag_temperature_forecast_S3.py) in the dags folder in this repository.

Now, let's take a look at achieving R3E using Airflow. 

First is **robustness**. Airflow allows this to be easily achieved by the built-in `retries` and `retry_delay` properties for each DAG, which specify the number of retries for a single failed task from the DAG and the delay between each retry, respectively:
```python
with DAG(
    owner='riste',
    retries=5, # Maximum allowed retries
    retry_delay=timedelta(minutes=5), # Delay between each retry
    dag_id='Temperature_Forecast_S3_v3',
    description='DAG to create temperature Forecast Model with Apache Airflow',
    start_date=datetime.utcnow(),
    schedule_interval='@daily',
    concurrency=5,
    max_active_runs=5, 
) as dag:
```
When a task fails, it is retried `retries` amount of times, without affecting the whole state of the DAG or the other tasks in the DAG itself. This means that a single task will not cause the whole pipeline to fail, and in terms of this research, the robustness is satisfied. A full example of this can be seen in the files that are in the [dags](https://github.com/rristov60/R3E-With-Airflow/tree/main/dags) folder in this repository. All of the DAGs have specified retries and retry_delay.

Next is **resilience**. To begin with, the resilience means that the pipeline will fail due to an error. The retries here are taken out of consideration since it is assumed that the error is peristent and no amount of retries under the same conditions will fix the issue. Unfortunately, there is no built-in mechanism that allows easy and automatic error handling, but since all of the pipelines are written in Python, the default Python error handling can be used. This is a challenge in itself, and in the pipelines used in this research, no error handling was done, which is out of the scope of this reserach.   

**Elasticity** is not something that can be achieved by default with Apache Airflow because Airflow does not offer automatic scaling. The best solution is to have a separate pipeline that creates DAGs dynamically by need. However, since Airflow is not event-driven, some sort of mechanism is required which is going to subside. The most common subside is a sensor(listener). A general demonstration of an S3 sensor(listener) can be found in the following [dag](https://github.com/rristov60/R3E-With-Airflow/blob/main/dags/s3_pipeline_trigerrer.py) in the `dags` folder of this repository. The dag creation can be done in multiple ways, but the most common one is to have a DAG template. This is not into any of the examples in this repository, but the following is an [example](https://docs.astronomer.io/learn/dynamically-generating-dags?tab=traditional#example-generate-dags-from-json-config-files) of how that can be achieved:
```python
# TEMPLATE DAG

from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime


@dag(
    dag_id=dag_id_to_replace,
    start_date=datetime(2023, 7, 1),
    schedule=schedule_to_replace,
    catchup=False,
)
def dag_from_config():
    BashOperator(
        task_id="say_hello",
        bash_command=bash_command_to_replace,
        env={"ENVVAR": env_var_to_replace},
    )


dag_from_config()
```

```python
# DAG config file
{
    "dag_id": "dag_file_1",
    "schedule": "'@daily'",
    "bash_command": "'echo $ENVVAR'",
    "env_var": "'Hello! :)'"
}
```

```python
# Dynamic DAG creation

import json
import os
import shutil
import fileinput


config_filepath = "include/dag-config/"
dag_template_filename = "include/dag-template.py"

for filename in os.listdir(config_filepath):
    f = open(config_filepath + filename)
    config = json.load(f)

    new_filename = "dags/" + config["dag_id"] + ".py"
    shutil.copyfile(dag_template_filename, new_filename)

    for line in fileinput.input(new_filename, inplace=True):
        line = line.replace("dag_id_to_replace", "'" + config["dag_id"] + "'")
        line = line.replace("schedule_to_replace", config["schedule"])
        line = line.replace("bash_command_to_replace", config["bash_command"])
        line = line.replace("env_var_to_replace", config["env_var"])
        print(line, end="")
```
This example can be modified so the creation of a DAG from a template is done by another DAG. 

Last is **reliability**. As demonstrated in the example DAGs, located in [dags](https://github.com/rristov60/R3E-With-Airflow/tree/main/dags), the reliability is achieved by combining all of the other aspects from R3E and smart pipeline design. Something like this cannot be achieved from a single aspect or a single feature. The reliability of the pipeline is increased when all of the R3E mechanisms of Airflow are combined.

Furthermore, even though that is out of the scope of this research, I took a look at the performance, since it is a crucial part of any ML pipeline. Because Big Data ML pipelines work with huge amounts of that, they consume a significant amount of resources. And most of the time those ML pipelines are run by different workers but managed from the same machine. The following practical experiment has a goal to find out whether is more performant to have multiple Big Data ML pipelines consolidated into one or have them as multiple pipelines. As a demonstration, I will use the pipeline shown in Figure 4. whose code can be found [here](https://github.com/rristov60/R3E-With-Airflow/blob/main/dags/s3_pipeline_trigerrer.py):

#### **Figure 4.** Data listeners and pipeline triggers
![Figure 4. Data listeners and pipeline ](./figures/Figure4.png)

This pipeline "listens" for a file change with a S3 sensor. The files it "listens" for are Big Data files that are used to train ML models. When a file is changed, the appropriate pipeline is run by a certain worker. The pipelines that are triggered are the same pipeline that is shown in Figure 3., but slightly modified to use different data to train the ML model. The ML pipelines or rather the Apache Airflow DAGs can be found as [dag_temperature_forecast_S3.py](https://github.com/rristov60/R3E-With-Airflow/blob/main/dags/dag_temperature_forecast_S3.py), [dag_temperature_forecast_S3_new_data.py](https://github.com/rristov60/R3E-With-Airflow/blob/main/dags/dag_temperature_forecast_S3_new_data.py), and [dag_temperature_forecast_S3_data.py](https://github.com/rristov60/R3E-With-Airflow/blob/main/dags/dag_temperature_forecast_S3_data.py) in the `dags` folder of this repository. When I analyzed the performance between the single pipeline/DAG for multiple ML models vs ML pipeline per ML model, I saw that it doesn't make sense to take the first approach simply because the load is too high on the worker. It is much more expensive to have a single very powerful worker than having multiple less powerful workers. Also, the R3E is much harder to achieve because there are more potential failing points when using the single pipeline approach. A conclusion from this is that it is much more performant and it makes much more sense financially to distribute the workload amongst multiple workers.  


## 4. Conclusion
To conclude, this research explores whether Apache Airflow is the proper tool to achieve R3E for Big Data ML pipelines. From the challenges presented throughout this document, it can be concluded that Apache Airflow **_can be_** used as a tool to achieve R3E for Big Data ML pipelines. However, when doing further research, other tools might make achieving this much easier and due to the less complexity of achieving some tasks, more performant as well. A good example of such tools is event-driven tools which would allow the elasticity to be achieved much easier. 

## Note
- This research is an assignment for [Aalto's CS-E4660 - Advanced Topics in Software Systems](https://github.com/rdsea/sys4bigml)