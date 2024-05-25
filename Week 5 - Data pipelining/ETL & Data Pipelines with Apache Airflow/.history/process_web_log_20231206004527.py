from datetime import timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# dag_args
default_args = {
    "owner": "Fawad Awan",
    "email": ["fawadawan@example.com"],
    "retries": 1,
    "retry_delay": timedelta(seconds=5),
}

dag = DAG(
    'process_web_log',
    start_date=days_ago(0),
    default_args=default_args,
    schedule=timedelta(days=1),
    description='A DAG to analyze and process web server log files',
)

extract_data_task = BashOperator(
    task_id='extract_data',
    bash_command='cut -d" " -f1 /home/project/accesslog.txt > /home/project/extracted_data.txt',
    dag=dag,
)

transform_data_task = BashOperator(
    task_id='transform_data',
    bash_command='cat /home/project/extracted_data.txt | grep "198.46.149.143" > /home/project/transformed_data.txt',
    dag=dag,
)

load_data_task = BashOperator(
    task_id='load_data',
    bash_command='tar -cvf /home/project/weblog.tar /home/project/transformed_data.txt',
    dag=dag,
)

extract_data_task >> transform_data_task >> load_data_task
