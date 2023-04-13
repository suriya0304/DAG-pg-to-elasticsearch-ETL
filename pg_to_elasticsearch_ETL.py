#!/usr/bin/env python
# coding: utf-8

# In[4]:


import datetime as dt
from datetime import timedelta 

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import pandas as pd

import psycopg2 as db
from elasticsearch import Elasticsearch


# In[7]:


default_args={
    "owner":'suriya',
    "start_date":dt.datetime(2023,4,12),
    'retries':1,
    'retry_delay':dt.timedelta(minutes=5)
}


# In[ ]:


with DAG('DAG_pg_es',default_args=default_args,schedule_interval=timedelta(minutes=5)) as dag:
    getData=PythonOperator(task_id='QueryPostgresSQL',python_callable=QueryPostgresSQL)
    insertData=PythonOperator(task_id='InsertElasticSearch',python_callable=InsertElasticSearch)
getData>>insertData


# In[21]:


def QueryPostgresSQL():
    conn_string="dbname='data_engineer' host='localhost' user='postgres' password='test'"
    conn=db.connect(conn_string)
    df=pd.read_sql('select name,city from people',conn)
    df.to_csv('getPgData.csv')
    print('----- Data saved as scv -----')


# In[ ]:


def InsertElasticSearch():
    es=Elasticsearch('http://127.0.0.1')
    df=pd.read_csv('getPgData.csv')
    for i,r in df.iterrows():
        doc=r.to_json
        es.index(index="frompostgresql",doc_type="doc",body=doc)
        print(res)
    


# In[22]:


QueryPostgresSQL()


# In[ ]:




