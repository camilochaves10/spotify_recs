import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

import nytimes as ny
import penguin as pen
import spotify as spt

def download_nytimes():
    bucket_name = 
    blob_name = 
    service_account_key_file = 
    data = ny.get_data()
    json = ny.convert_data(data)
    ny.write_json_to_gcs(bucket_name, blob_name, service_account_key_file, json)

def download_penguin():
    bucket_name = 
    blob_name = 
    service_account_key_file = 
    data = pen.getdata(50)
    pen.write_json_to_gcs(bucket_name, blob_name, service_account_key_file, data)
    
def download_spotify():
    bucket_name = 
    blob_name = 
    service_account_key_file =
    
    
with DAG(
    dag_id="msds697-task2",
    schedule="@daily",
    start_date=datetime(2023, 2, 25),
    catchup=False
) as dag:

    create_insert_aggregate = SparkSubmitOperator(
        task_id="aggregate_creation",
        packages="com.google.cloud.bigdataoss:gcs-connector:hadoop2-1.9.17,org.mongodb.spark:mongo-spark-connector_2.12:3.0.1",
        exclude_packages="javax.jms:jms,com.sun.jdmk:jmxtools,com.sun.jmx:jmxri",
        conf={"spark.driver.userClassPathFirst":True,
             "spark.executor.userClassPathFirst":True,
            #  "spark.hadoop.fs.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
            #  "spark.hadoop.fs.AbstractFileSystem.gs.impl":"com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
            #  "spark.hadoop.fs.gs.auth.service.account.enable":True,
            #  "google.cloud.auth.service.account.json.keyfile":service_account_key_file,
             },
        verbose=True,
        application='aggregates_to_mongo.py'
    )
    
    download_nytimes_data = PythonOperator(task_id = "download_nytimes_data",
                                                  python_callable = download_nytimes,
                                                  dag=dag)
    
    download_penguin_data = PythonOperator(task_id = "download_penguin_data",
                                                  python_callable = download_penguin,
                                                  dag=dag)
    
    download_spotify_data = PythonOperator(task_id = "download_spotify_data",
                                                  python_callable = download_spotify,
                                                  dag=dag)
    
    
    download_nytimes_data
    download_penguin_data
    download_spotify_data