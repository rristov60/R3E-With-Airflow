#!/bin/bash

source py_env/bin/activate
export AIRFLOW_HOME=$(pwd)
airflow scheduler
