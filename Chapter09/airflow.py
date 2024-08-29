from airflow import DAG 
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator 
from datetime import datetime 
default_args = { 
    'owner': 'airflow', 
} 

dag = DAG('spark_batch_prediction', default_args=default_args, schedule_interval='0 12 * * *', start_date=datetime(2021, 1, 1))   
submit_job = SparkSubmitOperator( 
    task_id='submit_prediction_job', 
    application='/path/to/your-spark-job.jar', 
    conn_id='spark_default', 
    dag=dag) 
