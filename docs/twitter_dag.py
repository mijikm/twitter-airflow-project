#!/usr/bin/env python
# coding: utf-8

# In[1]:


# Import packages
from datetime import timedelta
from airflow import DAG 
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
# Import the entire function from twitter_etl file
from twitter_etl import run_twitter_etl

# Define parameters required for DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 11, 8),
    'email': ['adminsb'],
    'email_on_fallure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

# Create a dag
dag = DAG(
    'twitter_dag', # Dag name shown on the UI
    default_args=default_args,
    description='My first etl code'
)

# Use Python operator to run the dag
run_etl = PythonOperator(
    task_id='complete_twitter_etl',
    python_callable=run_twitter_etl, # specify the function to be called
    dag=dag,
)

run_etl

