from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# dag_args
default_arguments = {
    "owner": "John Rodriguez",
    "email": ["rodrijohn@examplemail.com"],
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

dag = DAG(
    'process_web_log',
    default_args=default_args,
    description='A DAG to analyze and process web server log files',
    schedule_interval='@daily',
)

extract_data_task = BashOperator(
    task_id='extract_data',
    bash_command='grep -oE "\b(?:[0-9]{1,3}\.){3}[0-9]{1,3}\b" /home/project/accesslog.txt > /home/project/extracted_data.txt',
    dag=dag,
)

transform_data_task = BashOperator(
    task_id='transform_data',
    bash_command='grep -v "198.46.149.143" /home/project/extracted_data.txt > /home/project/transformed_data.txt',
    dag=dag,
)

load_data_task = BashOperator(
    task_id='load_data',
    bash_command='tar -cf /home/project/weblog.tar /home/project/transformed_data.txt',
    dag=dag,
)

extract_data_task >> transform_data_task >> load_data_task