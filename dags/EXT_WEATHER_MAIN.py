from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.models import DAG
from datetime import datetime, timedelta

"""
from os import listdir
from os.path import splitext, isfile, join

path = "/home/alimesut/DEV/airflow/python_scripts"
files_with_extension = [file for file in listdir(path) if isfile(join(path, file))]
files_with_not_extension = [splitext(filename)[0] for filename in listdir(path)]
file_list_with_extension = [word for word in files_with_extension if word.startswith("EXT_")]
file_list_with_not_extension = [word for word in files_with_not_extension if word.startswith("EXT_")]
"""

args = {
    'owner': 'airflow',
    'email': ['alimesut21@gmail.com'],  # to_email
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

dag = DAG(
    dag_id='ext_weather_main',
    default_args=args,
    start_date=datetime(2020, 2, 6),
    description='ETL - Extraction step.',
    schedule_interval=None,
    tags=['etl', 'extraction'],
)

start_task = DummyOperator(task_id='start_task',
                           dag=dag)

EXT_WEATHER_wind_speed = BashOperator(
    task_id="EXT_WEATHER_wind_speed",
    bash_command="python /opt/airflow/python_scripts/EXT_WEATHER_main.py 'wind_speed'",
    dag=dag,
    email_on_failure=True
)

EXT_WEATHER_pressure = BashOperator(
    task_id="EXT_WEATHER_pressure",
    bash_command="python /opt/airflow/python_scripts/EXT_WEATHER_main.py 'pressure'",
    dag=dag,
    email_on_failure=True
)

EXT_WEATHER_description = BashOperator(
    task_id="EXT_WEATHER_description",
    bash_command="python /opt/airflow/python_scripts/EXT_WEATHER_main.py 'description'",
    dag=dag,
    email_on_failure=True
)

EXT_WEATHER_humidity = BashOperator(
    task_id="EXT_WEATHER_humidity",
    bash_command="python /opt/airflow/python_scripts/EXT_WEATHER_main.py 'humidity'",
    dag=dag,
    email_on_failure=True
)

EXT_WEATHER_temperature = BashOperator(
    task_id="EXT_WEATHER_temperature",
    bash_command="python /opt/airflow/python_scripts/EXT_WEATHER_main.py 'temperature'",
    dag=dag,
    email_on_failure=True
)

EXT_WEATHER_wind_direction = BashOperator(
    task_id="EXT_WEATHER_wind_direction",
    bash_command="python /opt/airflow/python_scripts/EXT_WEATHER_main.py 'wind_direction'",
    dag=dag,
    email_on_failure=True
)

"""
fail_bash = BashOperator(
    task_id="fail_bash",
    bash_command="asdsafa",
    dag=dag,
    email_on_failure=True
)
"""

ext_end_email = EmailOperator(
    task_id='ext_end_email',
    to='alimesut21@gmail.com',
    subject='EXT_WEATHER RUN STATUS',
    html_content="Extraction tamamlandı. ",
    dag=dag)

start_task >> [EXT_WEATHER_pressure, EXT_WEATHER_wind_speed,
               EXT_WEATHER_description, EXT_WEATHER_humidity,
               EXT_WEATHER_temperature, EXT_WEATHER_wind_direction] >> ext_end_email

# , ps.EXT_WEATHER_pressure, ps.EXT_WEATHER_temperature, ps.EXT_WEATHER_wind_direction, ps.EXT_WEATHER_wind_speed
