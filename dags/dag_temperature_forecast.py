import os
import numpy as np
import pandas as pd
from airflow import DAG
from prophet import Prophet
from tempfile import NamedTemporaryFile
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

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
    current_working_directory = os.getcwd()
    df = pd.read_csv(f"{current_working_directory}/datasets/IOT-temp.csv")
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
        df.to_csv(f.name, index=False)
        #f.flush()
        kwargs['ti'].xcom_push(key=f"{kwargs['dag'].dag_id}_{kwargs['ds']}", value=f.name)

# Model train for 'In'
def train_in(**kwargs):
    
    path = kwargs['ti'].xcom_pull(task_ids='data_prep', key=f"{kwargs['dag'].dag_id}_{kwargs['ds']}_orgDF")
    # Replace later by reading the data from model_prep tmp
    org_df = pd.read_csv(path)
    
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

# Model train for 'Out'
def train_out(**kwargs):
    path = kwargs['ti'].xcom_pull(task_ids='data_prep', key=f"{kwargs['dag'].dag_id}_{kwargs['ds']}_orgDF")
    # Replace later by reading the data from model_prep tmp
    org_df = pd.read_csv(path)
    
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

# Data preparation
def data_prep(**kwargs):
    path = kwargs['ti'].xcom_pull(task_ids='preprocess_data', key=f"{kwargs['dag'].dag_id}_{kwargs['ds']}")
    df = pd.read_csv(path)
    
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
        kwargs['ti'].xcom_push(key=f"{kwargs['dag'].dag_id}_{kwargs['ds']}_orgDF", value=f.name)

# Deletes temporary files created by the previous functions
def clean_temporary(**kwargs):
    # Preprocessed data
    preprocessed_path = kwargs['ti'].xcom_pull(task_ids='preprocess_data', key=f"{kwargs['dag'].dag_id}_{kwargs['ds']}")
    os.remove(preprocessed_path)
    
    # Prepared data
    preparedData_path = kwargs['ti'].xcom_pull(task_ids='data_prep', key=f"{kwargs['dag'].dag_id}_{kwargs['ds']}_orgDF")
    os.remove(preparedData_path)

    pass

def serve_models(**kwargs):
    # Fetch models path from temporary folder using kwargs['ti'].xcom_pull()
    print("Deploying and serving models")


with DAG(
    default_args=default_args,
    dag_id='Temperature_Forecast',
    description='DAG to create temperature Forecast Model with Apache Airflow',
    start_date=datetime(2023, 11, 6),
    schedule_interval='@daily'
) as dag:
    #pass
    preprocess = PythonOperator(
       task_id='preprocess_data',
       python_callable=pre_process_data
    )
    
    dataprep = PythonOperator(
        task_id='data_prep',
        python_callable=data_prep
    )
    
    trainIn = PythonOperator(
        task_id='train_in',
        python_callable=train_in
    )
    
    trainOut = PythonOperator(
        task_id='train_out',
        python_callable=train_out
    )
    
    serveModels = PythonOperator(
        task_id='serve_models',
        python_callable=serve_models
    )
    
    deleteTemporary = PythonOperator(
        task_id='clean_temporary',
        python_callable=clean_temporary
    )
    
    # Set DAG (Directional Acyclic Graph)
    # direction flows
    preprocess.set_downstream(dataprep)
    dataprep.set_downstream(trainIn)
    dataprep.set_downstream(trainOut)
    trainIn.set_downstream(serveModels)
    trainOut.set_downstream(serveModels)
    serveModels.set_downstream(deleteTemporary)