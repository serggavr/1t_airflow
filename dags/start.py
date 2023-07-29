import pandas as pd
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from datetime import datetime
from sqlalchemy import create_engine
import requests
import psycopg2

CSV_URL = 'https://s917sas.storage.yandex.net/rdisk/6c3bdf13145f6b3e59edc312d28122abc9c62d2cdadffaef5e540137668adea4/64c5629d/z33xiEBGQALbiKJO9cHRV3MMwM4P88DSYccVIw2rrZ554j8OI22ab7P0t0EDKVQprZPvweVTkpmAfiV51teiYQ==?uid=0&filename=Sample%20-%20Superstore.csv&disposition=attachment&hash=aPe/a92CElWpJKwaX6AhJGq6U/tNXaLkZFnfVrOa1/3gz98ryLhCh5m6SE2udkJiq/J6bpmRyOJonT3VoXnDag%3D%3D&limit=0&content_type=text%2Fplain&owner_uid=81729374&fsize=2287806&hid=374e66df151f17dfeec92a1c96629f33&media_type=spreadsheet&tknv=v2&rtoken=r701pNTsXvBg&force_default=no&ycrid=na-a70dc3be2f757e31dd8bbac0aa606a82-downloader17h&ts=601a4dbf82140&s=25365f9a7dba0e09eea1c42741e6566aa3de150aed16ab354388ae50aba83a0a&pb=U2FsdGVkX1-tvY1BIwWLRDdPaslHE9VTSWjZT1_UlHZHAIho6vQ7wp4AJxkPsLsGwEdl0JwRnmDcfXuGAWLfRHQUZFrf4twM8ZDLkytuSQQ'
connection_params = BaseHook.get_connection('postgres_connection')


def fn_load_data_to_folder():
    response = requests.get(CSV_URL)
    with open('/opt/airflow/raw_data/supermarket_1/resp.csv', 'wb') as f:
        f.write(response.content)


def fn_load_data_to_db(db_connection_params):
    dataframe = pd.read_csv('/opt/airflow/raw_data/supermarket_1/resp.csv', encoding="latin-1")

    try:
        engine = create_engine(f'postgresql+psycopg2://{db_connection_params.login}:{db_connection_params.password}@{db_connection_params.host}/{db_connection_params.schema}')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    dataframe.to_sql('supermarket_1', con=engine, if_exists='replace', index=False)


def fn_sum_per_year(db_connection_params):
    try:
        engine = create_engine(f'postgresql+psycopg2://{db_connection_params.login}:{db_connection_params.password}@{db_connection_params.host}/{db_connection_params.schema}')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    raw_dataframe = pd.read_sql_table('supermarket_1', con=engine)
    mart_dataframe = pd.DataFrame()
    mart_dataframe.insert(0, 'order_date', pd.to_datetime(raw_dataframe["Order Date"]).dt.year)
    mart_dataframe.insert(1, 'segment', raw_dataframe['Segment'])
    mart_dataframe.insert(2, 'sales', raw_dataframe['Sales'])
    mart_dataframe = mart_dataframe[mart_dataframe['segment'] == 'Corporate']
    mart_dataframe = mart_dataframe.groupby(['order_date', 'segment'], as_index=False)['sales'].sum().round(2)
    mart_dataframe.to_sql('supermarket_sales', con=engine, if_exists='replace', index=False)


my_dag = DAG(
    dag_id='new_dag',
    start_date=datetime(2022, 7, 29),
    schedule_interval='@once'
)

task_start = DummyOperator(
    task_id='start',
    dag=my_dag
)

task_create_folder = BashOperator(
    task_id='create_folder2',
    bash_command='mkdir -p /opt/airflow/raw_data;'
                 'mkdir -p /opt/airflow/raw_data/supermarket_1;',
    dag=my_dag
)

task_load_data_to_folder = PythonOperator(
    task_id='load_data_to_folder',
    python_callable=fn_load_data_to_folder,
    dag=my_dag
)


task_load_data_from_folder_to_db = PythonOperator(
    task_id='load_data_from_folder_to_db',
    python_callable=fn_load_data_to_db,
    op_kwargs={'db_connection_params': connection_params},
    dag=my_dag
)

task_count_sum_per_year = PythonOperator(
    task_id='count_sum_per_year',
    python_callable=fn_sum_per_year,
    op_kwargs={'db_connection_params': connection_params},
    dag=my_dag
)

task_end = DummyOperator(
    task_id='end',
    dag=my_dag
)

task_start >> task_create_folder >> task_load_data_to_folder >> task_load_data_from_folder_to_db >> task_count_sum_per_year >> task_end






