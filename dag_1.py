from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd 
import csv
import pymongo


def remove_duplicates():
    df = pd.read_csv('/opt/airflow/dags/data.csv')
    df.drop_duplicates(inplace=True)
    df.to_csv('/opt/airflow/dags/data.csv', index=False)

def replace_zeros():
    df = pd.read_csv('/opt/airflow/dags/data.csv')
    df = df.fillna("-")
    df.to_csv('/opt/airflow/dags/data.csv', index=False)

def sort_by_column():
    df = pd.read_csv('/opt/airflow/dags/data.csv')
    df.sort_values(by='at', inplace=True)
    df.to_csv('/opt/airflow/dags/data.csv', index=False)

def remove_stuff():
    df= pd.read_csv("/opt/airflow/dags/data.csv")
    df['content']=df['content'].str.replace('[^a-zA-Z\s\.,;!?\-]', '', regex=True)
    df.to_csv('/opt/airflow/dags/data.csv', index=False)

def load_dataframe_to_mongodb():

    client = pymongo.MongoClient('mongodb://192.168.100.160:27017/')
    db = client['airflow']
    collection = db['airflow']

    
    df= pd.read_csv("/opt/airflow/dags/data.csv")
    data = df.to_dict(orient='records')

    collection.insert_many(data)
    client.close()

with DAG("dag_1", start_date=datetime.now(), schedule_interval=None, catchup=False) as dag:

       file_sensor = FileSensor(
           task_id="file_sensor",
           filepath="/opt/airflow/dags/data.csv",
           fs_conn_id="fs_default",
           poke_interval=10
       )

       with TaskGroup('processing_tasks') as processing_tasks:
          
          task_1 = PythonOperator(
               task_id='remove_duplicates',
               python_callable=remove_duplicates
          )

          task_2 = PythonOperator(
               task_id='replace_zeros',
               python_callable=replace_zeros
          )

          task_3 = PythonOperator(
               task_id='sort_by_column',
               python_callable=sort_by_column
          )

          task_4 = PythonOperator(
               task_id='remove_stuff',
               python_callable=remove_stuff
          )
       
       task_5 = PythonOperator(
            task_id='load_dataframe_task',
            python_callable=load_dataframe_to_mongodb,
       )

       file_sensor>>task_1>>task_2>>task_3>>task_4>>task_5
          


       








