#!/bin/bash

# source py_env/bin/activate

export AIRFLOW_HOME=$(pwd)
airflow db init
airflow users create --username admin --firstname Admin --lastname Adminski --role Admin --email admin@domain.com