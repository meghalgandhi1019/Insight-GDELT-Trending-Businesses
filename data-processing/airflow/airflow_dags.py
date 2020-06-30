import datetime as dt
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime
import time
from airflow.operators.python_operator import PythonOperator


default_args = {
        'owner': 'airflow',
        'start_date': datetime(2020, 7, 1),
        'retries': 3,
        'retry_delay': dt.timedelta(minutes=5)
        }

dag = DAG(
    'upload_s3_spark_job_dag',
    default_args=default_args,
    schedule_interval='@monthly',  # Run once a month at midnight of the first day of the month
        )

# date in format YYYY-MM-DD. Get the previous execution date to get the gdelt files for that month
EXEC_DATE = '{{ prev_ds }}'

# Upload raw gdelt files to s3 bucket every month
upload_s3_task = BashOperator(
    task_id='upload_s3_task',
    bash_command='python /usr/local/Insight-GDELT-Trending-Businesses/ingestion/gdelt/upload_to_s3.py' + EXEC_DATE,
    dag=dag
    )

# Introduced delay of 30 min deliberately as uploading raw files to s3 might take some time
delay_python_task: PythonOperator = PythonOperator(task_id="delay_python_task",
                                                   dag=dag,
                                                   python_callable=lambda: time.sleep(1800))

# Task to run the spark job
spark_job_task = BashOperator(
    task_id='spark_job_task',
    bash_command='spark-submit /usr/local/Insight-GDELT-Trending-Businesses/data-processing/spark/spark_job_run_parquet.py' + EXEC_DATE,
    dag=dag
    )


upload_s3_task >> delay_python_task >> spark_job_task
