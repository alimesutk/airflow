from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import DAG
from datetime import datetime

args = {
    'owner': 'airflow',
}

dag = DAG(
    dag_id='ext_weather_temperature',
    default_args=args,
    start_date=datetime(2020, 2, 6),
    description='ETL - Extraction step.',
    schedule_interval=None,
    tags=['etl', 'extraction'],
)

run_this_first = DummyOperator(task_id='run_this_first',
                               dag=dag)

run_this = BashOperator(
    task_id='run_this',
    bash_command='python /opt/airflow/python_scripts/EXT_WEATHER_temperature.py',
    dag=dag)

run_this_first >> run_this
