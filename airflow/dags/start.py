import pandas as pd
from airflow import DAG
# from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable
from datetime import datetime
from sqlalchemy import create_engine
import requests
import psycopg2
import datetime as dt

db_connection_params = BaseHook.get_connection('postgres_connection')
url_get_exchangerate = Variable.get('url_get_exchangerate_BTC_RUB')


def fn_get_exchangerate():
    url = url_get_exchangerate
    response = requests.get(url)
    data = response.json()
    return data['result']


def fn_load_current_exchangerate_to_db(**kwargs):

    try:
        engine = create_engine(f'postgresql+psycopg2://{db_connection_params.login}:{db_connection_params.password}@{db_connection_params.host}/{db_connection_params.schema}')
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

    ti = kwargs['ti']
    exchangerate = ti.xcom_pull(task_ids='get_exchangerate', key='return_value')
    exchangerate_dataframe = pd.DataFrame()
    exchangerate_dataframe.insert(0, 'date', [f'{dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}'])
    exchangerate_dataframe.insert(1, 'exchangerate', [float(exchangerate)])
    exchangerate_dataframe.to_sql('exchangerate', con=engine, if_exists='append', index=False)


my_dag = DAG(
    dag_id='new_dag',
    start_date=datetime(2023, 7, 29),
    schedule_interval="0-59/10 * * * *"
)

task_greeting = BashOperator(
    task_id='greeting',
    bash_command='echo "Good morning my diggers!"',
    dag=my_dag
)

task_get_exchangerate = PythonOperator(
    task_id='get_exchangerate',
    python_callable=fn_get_exchangerate,
    provide_context=True,
    dag=my_dag
)

task_load_current_exchangerate_to_db = PythonOperator(
    task_id='load_current_exchangerate_to_db',
    python_callable=fn_load_current_exchangerate_to_db,
    provide_context=True,
    dag=my_dag
)

task_greeting >> task_get_exchangerate >> task_load_current_exchangerate_to_db






