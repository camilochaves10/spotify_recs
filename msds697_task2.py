import airflow
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from user_definition import *

from spotify_calls *

# NOTE : In order to make sure it send configurations requests first, do not import your .py reading from gs.


def _download_spotify_data():
    blob_name = f'{yesterday}/{sf_data_sub_uri}.csv'
    df = retrieve_yesterday_data(sf_data_url, sf_data_sub_uri, sf_data_app_token, data_limit, str(yesterday))
    write_csv_to_gcs(bucket_name, blob_name, service_account_key_file, df)
    
def _download_sf_weather_data():
    data = filter_history_data(retreive_api_data(noaa_token, noaa_api_url),
                               three_days_ago)  
    write_json_to_gcs(bucket_name, f'{three_days_ago}/weather.json', service_account_key_file, data)
    

with DAG(
    dag_id="msds697-task2",
    schedule=None,
    start_date=datetime(2023, 2, 23),
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
    download_spotify_data = PythonOperator(task_id = "download_spotify_data",
                                                  python_callable = _download_spotify_data,
                                                  dag=dag)

    download_sf_weather_data = PythonOperator(task_id = "download_sf_weather_data",
                                                    python_callable = _download_sf_weather_data,
                                                    dag=dag)
    download_spotify_data
    download_sf_weather_data >> create_insert_aggregate