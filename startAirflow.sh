#!/bin/bash

source py_env/bin/activate
export AIRFLOW_HOME=$(pwd)
airflow webserver -p 8080
