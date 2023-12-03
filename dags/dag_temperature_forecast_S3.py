import os
import codecs
import random
import pickle
import numpy as np
import pandas as pd
from io import StringIO
from airflow import DAG
from prophet import Prophet
from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


default_args = {
    'owner': 'riste',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# Internal function used for reading the data
def read_data(path):
    df = pd.read_csv(path)
    return df
    
# Internal function used to convert month to a season
def month2seasons(x):
    if x in [12, 1, 2]:
        season = 'Winter'
    elif x in [3, 4, 5]:
        season = 'Summer'
    elif x in [6, 7, 8, 9]:
        season = 'Monsoon'
    elif x in [10, 11]:
        season = 'Post_Monsoon'
    return season

# Internal function used to convert hour to generic timing
def hours2timing(x):
    if x in [22,23,0,1,2,3]:
        timing = 'Night'
    elif x in range(4, 12):
        timing = 'Morning'
    elif x in range(12, 17):
        timing = 'Afternoon'
    elif x in range(17, 22):
        timing = 'Evening'
    else:
        timing = 'X'
    return timing

# Preprocessing data
def pre_process_data(**kwargs):
    s3_hook = S3Hook(aws_conn_id='minio')
    key = "IOT-temp.csv"
    s3_object = s3_hook.get_key(key=key, bucket_name="airflow")
    data = s3_object.get()['Body'].read().decode('utf-8')
    print(data)
    df = pd.read_csv(StringIO(data))
    df.drop('room_id/id', axis=1, inplace=True)
    df.rename(columns={'noted_date':'date', 'out/in':'place'}, inplace=True)
    
    # Extracting datetime information
    df['date'] = pd.to_datetime(df['date'], format='%d-%m-%Y %H:%M')
    df['year'] = df['date'].apply(lambda x : x.year)
    df['month'] = df['date'].apply(lambda x : x.month)
    df['day'] = df['date'].apply(lambda x : x.day)
    df['weekday'] = df['date'].apply(lambda x : x.day_name())
    df['weekofyear'] = df['date'].apply(lambda x : x.weekofyear)
    df['hour'] = df['date'].apply(lambda x : x.hour)
    df['minute'] = df['date'].apply(lambda x : x.minute)
    
    # Month to season
    df['season'] = df['month'].apply(month2seasons)
    
    # Timing information
    df['timing'] = df['hour'].apply(hours2timing)
    
    # Removing duplicate records
    df.drop_duplicates(inplace=True)
    df[df.duplicated()]
    
    # Unqiue IDs
    df['id'].apply(lambda x : x.split('_')[6]).nunique() == len(df)
    # Numeric parts in ID as new identifier
    df['id'] = df['id'].apply(lambda x : int(x.split('_')[6]))
    
    df.loc[df['id'].isin(range(4000, 4011))].sort_values(by='id')
    
    # with NamedTemporaryFile(mode='w', suffix='{{ run_id }}_{{ ti }}_{{ ds }}_{{ ts }}.csv') as f:
    with NamedTemporaryFile(mode='w', delete=False, suffix=f"_{kwargs['dag'].dag_id}.csv") as f:
        s3_key = f'{kwargs["dag"].dag_id}_{kwargs["ds"]}/{f.name.split("/tmp/")[-1]}'
        s3_hook.load_string(df.to_csv(), s3_key, "airflow")
        kwargs['ti'].xcom_push(key=f"{kwargs['dag'].dag_id}_{kwargs['ds']}", value=s3_key)

# Model train for 'In'
def train_in(**kwargs):
    
    key = kwargs['ti'].xcom_pull(task_ids='data_prep', key=f"{kwargs['dag'].dag_id}_{kwargs['ds']}_orgDF")
    print(key)
    s3_hook = S3Hook(aws_conn_id='minio')
    s3_object = s3_hook.get_key(key=key, bucket_name="airflow")
    data = s3_object.get()['Body'].read().decode('utf-8')
    org_df = pd.read_csv(StringIO(data))
    
    # make dataframe for training
    prophet_df = pd.DataFrame()
    prophet_df["ds"] = pd.date_range(start=org_df['daily'][0], end=org_df['daily'][133])
    prophet_df['y'] = org_df['In']
    # add seasonal information
    prophet_df['monsoon'] = org_df['season_Monsoon']
    prophet_df['post_monsoon'] = org_df['season_Post_Monsoon']
    prophet_df['winter'] = org_df['season_Winter']

    # train model by Prophet
    m = Prophet(changepoint_prior_scale=0.1, yearly_seasonality=2, weekly_seasonality=False)
    # include seasonal periodicity into the model
    m.add_seasonality(name='season_monsoon', period=124, fourier_order=5, prior_scale=0.1, condition_name='monsoon')
    m.add_seasonality(name='season_post_monsoon', period=62, fourier_order=5, prior_scale=0.1, condition_name='post_monsoon')
    m.add_seasonality(name='season_winter', period=93, fourier_order=5, prior_scale=0.1, condition_name='winter')
    m.fit(prophet_df)
    
    serialized_model = codecs.encode(pickle.dumps(m), "base64").decode()
    key_to_save_s3 = f'models/{kwargs["ds"]}_train_in.pkl'
    s3_hook.load_string(serialized_model, key_to_save_s3, "airflow", replace=True)

# Model train for 'Out'
def train_out(**kwargs):
    key = kwargs['ti'].xcom_pull(task_ids='data_prep', key=f"{kwargs['dag'].dag_id}_{kwargs['ds']}_orgDF")
    s3_hook = S3Hook(aws_conn_id='minio')
    s3_object = s3_hook.get_key(key=key, bucket_name="airflow")
    data = s3_object.get()['Body'].read().decode('utf-8')
    org_df = pd.read_csv(StringIO(data))
    
    # make dataframe for training
    prophet_df = pd.DataFrame()
    prophet_df["ds"] = pd.date_range(start=org_df['daily'][0], end=org_df['daily'][133])
    prophet_df['y'] = org_df['Out']
    # add seasonal information
    prophet_df['monsoon'] = org_df['season_Monsoon']
    prophet_df['post_monsoon'] = org_df['season_Post_Monsoon']
    prophet_df['winter'] = org_df['season_Winter']

    # train model by Prophet
    m = Prophet(changepoint_prior_scale=0.1, yearly_seasonality=2, weekly_seasonality=False)
    # include seasonal periodicity into the model
    m.add_seasonality(name='season_monsoon', period=124, fourier_order=5, prior_scale=0.1, condition_name='monsoon')
    m.add_seasonality(name='season_post_monsoon', period=62, fourier_order=5, prior_scale=0.1, condition_name='post_monsoon')
    m.add_seasonality(name='season_winter', period=93, fourier_order=5, prior_scale=0.1, condition_name='winter')
    m.fit(prophet_df)
    
    serialized_model = codecs.encode(pickle.dumps(m), "base64").decode()
    key_to_save_s3 = f'models/{kwargs["ds"]}_train_out.pkl'
    s3_hook.load_string(serialized_model, key_to_save_s3, "airflow", replace=True)

# Data preparation
def data_prep(**kwargs):
    key = kwargs['ti'].xcom_pull(task_ids='preprocess_data', key=f"{kwargs['dag'].dag_id}_{kwargs['ds']}")
    s3_hook = S3Hook(aws_conn_id='minio')
    s3_object = s3_hook.get_key(key=key, bucket_name="airflow")
    data = s3_object.get()['Body'].read().decode('utf-8')
    df = pd.read_csv(StringIO(data))
    
    month_rd = np.round(df['date'].apply(lambda x : datetime.strptime(x,"%Y-%m-%d %H:%M:%S").strftime("%Y-%m")).value_counts(normalize=True).sort_index() * 100,decimals=1)
    pl_cnt = np.round(df['place'].value_counts(normalize=True) * 100)
    season_cnt = np.round(df['season'].value_counts(normalize=True) * 100)
    timing_cnt = np.round(df['timing'].value_counts(normalize=True) * 100)
    
    in_month = np.round(df[df['place']=='In']['date'].apply(lambda x : datetime.strptime(x,"%Y-%m-%d %H:%M:%S").strftime("%Y-%m")).value_counts(normalize=True).sort_index() * 100, decimals=1)
    out_month = np.round(df[df['place']=='Out']['date'].apply(lambda x : datetime.strptime(x,"%Y-%m-%d %H:%M:%S").strftime("%Y-%m")).value_counts(normalize=True).sort_index() * 100, decimals=1)
    in_out_month = pd.merge(in_month,out_month,right_index=True,left_index=True).rename(columns={'date_x':'In', 'date_y':'Out'})
    in_out_month = in_out_month.reset_index()
    in_out_month = pd.melt(in_out_month.reset_index(), ['index']).rename(columns={'index':'Month', 'variable':'Place'})

    season_agg = df.groupby('season').agg({'temp': ['min', 'max']})
    season_maxmin = pd.merge(season_agg['temp']['max'],season_agg['temp']['min'],right_index=True,left_index=True)
    season_maxmin = pd.melt(season_maxmin.reset_index(), ['season']).rename(columns={'season':'Season', 'variable':'Max/Min'})

    timing_agg = df.groupby('timing').agg({'temp': ['min', 'max']})
    timing_maxmin = pd.merge(timing_agg['temp']['max'],timing_agg['temp']['min'],right_index=True,left_index=True)
    timing_maxmin = pd.melt(timing_maxmin.reset_index(), ['timing']).rename(columns={'timing':'Timing', 'variable':'Max/Min'})

    tsdf = df.drop_duplicates(subset=['date','place']).sort_values('date').reset_index(drop=True)
    tsdf['temp'] = df.groupby(['date','place'])['temp'].mean().values
    tsdf.drop('id', axis=1, inplace=True)
    
    in_month = tsdf[tsdf['place']=='In'].groupby('month').agg({'temp':['mean']})
    in_month.columns = [f"{i[0]}_{i[1]}" for i in in_month.columns]
    out_month = tsdf[tsdf['place']=='Out'].groupby('month').agg({'temp':['mean']})
    out_month.columns = [f"{i[0]}_{i[1]}" for i in out_month.columns]
    
    tsdf['daily'] = tsdf['date'].apply(lambda x : pd.to_datetime(datetime.strptime(x,"%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")))
    in_day = tsdf[tsdf['place']=='In'].groupby(['daily']).agg({'temp':['mean']})
    in_day.columns = [f"{i[0]}_{i[1]}" for i in in_day.columns]
    out_day = tsdf[tsdf['place']=='Out'].groupby(['daily']).agg({'temp':['mean']})
    out_day.columns = [f"{i[0]}_{i[1]}" for i in out_day.columns]
   
    in_wd = tsdf[tsdf['place']=='In'].groupby('weekday').agg({'temp':['mean']})
    in_wd.columns = [f"{i[0]}_{i[1]}" for i in in_wd.columns]
    in_wd['week_num'] = [['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'].index(i) for i in in_wd.index]
    in_wd.sort_values('week_num', inplace=True)
    in_wd.drop('week_num', axis=1, inplace=True)
    out_wd = tsdf[tsdf['place']=='Out'].groupby('weekday').agg({'temp':['mean']})
    out_wd.columns = [f"{i[0]}_{i[1]}" for i in out_wd.columns]
    out_wd['week_num'] = [['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'].index(i) for i in out_wd.index]
    out_wd.sort_values('week_num', inplace=True)
    out_wd.drop('week_num', axis=1, inplace=True)

    in_wof = tsdf[tsdf['place']=='In'].groupby('weekofyear').agg({'temp':['mean']})
    in_wof.columns = [f"{i[0]}_{i[1]}" for i in in_wof.columns]
    out_wof = tsdf[tsdf['place']=='Out'].groupby('weekofyear').agg({'temp':['mean']})
    out_wof.columns = [f"{i[0]}_{i[1]}" for i in out_wof.columns]
    
    in_tsdf = tsdf[tsdf['place']=='In'].reset_index(drop=True)
    in_tsdf.index = in_tsdf['date']

    out_tsdf = tsdf[tsdf['place']=='Out'].reset_index(drop=True)
    out_tsdf.index = out_tsdf['date']

    inp_df = pd.DataFrame()
    in_d_inp = in_day.resample('1D').interpolate('spline', order=5)
    out_d_inp = out_day.resample('1D').interpolate('spline', order=5)
    inp_df['In'] = in_d_inp.temp_mean
    inp_df['Out'] = out_d_inp.temp_mean


    org_df = inp_df.reset_index()
    org_df['season'] = org_df['daily'].apply(lambda x : month2seasons(x.month))
    org_df = pd.get_dummies(org_df, columns=['season'])
    
    with NamedTemporaryFile(mode='w', delete=False, suffix=f"_{kwargs['dag'].dag_id}_orgDF.csv") as f:
        org_df.to_csv(f.name, index=False)
        s3_key = f'{kwargs["dag"].dag_id}_{kwargs["ds"]}/{f.name.split("/tmp/")[-1]}'
        s3_hook.load_string(org_df.to_csv(), s3_key, "airflow")
        kwargs['ti'].xcom_push(key=f"{kwargs['dag'].dag_id}_{kwargs['ds']}_orgDF", value=s3_key)

# Deletes temporary files created by the previous functions
def clean_temporary(**kwargs):
    # Preprocessed data
    preprocessed_path = kwargs['ti'].xcom_pull(task_ids='preprocess_data', key=f"{kwargs['dag'].dag_id}_{kwargs['ds']}")
    os.remove(preprocessed_path)
    
    # Prepared data
    preparedData_path = kwargs['ti'].xcom_pull(task_ids='data_prep', key=f"{kwargs['dag'].dag_id}_{kwargs['ds']}_orgDF")
    os.remove(preparedData_path)

def decide_branch(**kwargs):
    rand_integer = random.randint(0, 10)
    if rand_integer > 5:
        return 'train_out'
    else:
        return 'train_out'

def marked_current_data_as_processed(**kwargs):
    s3_hook = S3Hook(aws_conn_id='minio')
    key = "IOT-temp.csv"
    s3_object = s3_hook.get_key(key=key, bucket_name="airflow")
    data = s3_object.get()['Body'].read().decode('utf-8') 
    df = pd.read_csv(StringIO(data))
    
    s3_key = f'processed/{kwargs["run_id"]}_{kwargs["ts"]}_data.csv'
    s3_hook.load_string(df.to_csv(), s3_key, "airflow")
    s3_hook.delete_objects('airflow', key)


with DAG(
    default_args=default_args,
    dag_id='Temperature_Forecast_S3_v3',
    description='DAG to create temperature Forecast Model with Apache Airflow',
    start_date=datetime.utcnow(), # Elasticity
    schedule_interval='@daily', # Elasticity
    concurrency=5, # For performance aspect
    max_active_runs=5, # Also for the performance aspect
    # Also example with distributed celery executor (for performance aspect)
    # trigger_rule="none_failed_min_one_success",
) as dag:
    
     
    start_task = BashOperator(
        task_id='start_task',
        bash_command="echo Data changed, triggering pipeline!"
    )
    
    preprocess = PythonOperator(
       task_id='preprocess_data',
       python_callable=pre_process_data
    )
    
    dataprep = PythonOperator(
        task_id='data_prep',
        python_callable=data_prep
    )
    
    
    train = PythonOperator(
        task_id='train',
        python_callable=train_out
    )
    
    markedCurrentDataAsProcessed = PythonOperator(
        task_id='marked_current_data_as_processed',
        python_callable=marked_current_data_as_processed
    )
    
    final_task = TriggerDagRunOperator(
        trigger_dag_id='docker_dag_iotTemp',
        task_id='deploy_model'
    )
    
    
    # Set DAG (Directional Acyclic Graph)
    start_task.set_downstream(preprocess)
    preprocess.set_downstream(dataprep)
    dataprep.set_downstream(train)
    train >> markedCurrentDataAsProcessed
    markedCurrentDataAsProcessed.set_downstream(final_task)