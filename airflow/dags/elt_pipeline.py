from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(
    dag_id='elt_pipeline',
    default_args=default_args,
    schedule_interval='@hourly',
    catchup=False,
) as dag:
    start = DummyOperator(task_id='start')

    kafka_to_spark = DockerOperator(
        task_id='kafka_to_spark',
        image='bitnami/spark',
        api_version='auto',
        auto_remove=True,
        command='spark-submit /opt/bitnami/spark/spark_streaming_job.py',
        docker_url='unix://var/run/docker.sock',
        network_mode='bridge'
    )

    spark_to_minio = DummyOperator(task_id='spark_to_minio')
    minio_to_postgres = DummyOperator(task_id='minio_to_postgres')

    end = DummyOperator(task_id='end')

    start >> kafka_to_spark >> spark_to_minio >> minio_to_postgres >> end
