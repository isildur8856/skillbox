import os
import sys
import datetime

from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# импортируем функции загрузки данных из модулей
from modules.hits_pipeline import hits_main
from modules.sessions_pipeline import sessions_main

# путь к проекту
path = os.path.expanduser('~/airflow_diploma')
os.environ['PROJECT_PATH'] = path
sys.path.insert(0, path)

# аргументы для DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime.datetime(2024, 2, 28),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'depends_on_past': False,
}

# создание DAG
with DAG(
        'load_hits_and_sessions_data',
        default_args=default_args,
        description='DAG для загрузки данных в таблицы hits и sessions в базу данных sber_auto',
        schedule_interval=None,
) as dag:
    # приветственная задача
    hello_task = BashOperator(
        task_id='hello_task',
        bash_command='echo "Начинаем загрузку данных"',
        dag=dag,
    )
    # задача для загрузки данных из hits
    load_hits_task = PythonOperator(
        task_id='load_hits_data_task',
        python_callable=hits_main,
        dag=dag,
    )
    # задача для загрузки данных из sessions
    load_sessions_task = PythonOperator(
        task_id='load_sessions_data_task',
        python_callable=sessions_main,
        dag=dag
    )
    # задача вывода сообщения об успешном окончании загрузки данных
    goodbye_task = BashOperator(
        task_id='goodbye_task',
        bash_command='echo "Загрузка данных успешно завершена"',
        dag=dag,
    )
    # порядок выполнения задач
    hello_task >> load_hits_task >> load_sessions_task >> goodbye_task

