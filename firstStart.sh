#!/bin/bash

source py_env/bin/activate

# Init DB and create
bash createUser.sh 
# Start minio instance and send it to bacckground
bash ./minio/startMinio.sh
# Start Apache airflow webserver and send it to background
bash startAirflow.sh > /dev/null & 
# Start Apache airflow scheduler and send it to background
bash startScheduler.sh > /dev/null &