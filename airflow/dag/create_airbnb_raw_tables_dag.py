from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator



default_args = {
    'owner': 'axel',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    dag_id='create_airbnb_raw_tables_dag',
    default_args=default_args,
    description='This is our first dag that we write',
    start_date=datetime(2023, 11, 9, 11),
    schedule_interval='@daily',
    template_searchpath="/opt/airflow/",

) as dag:


    create_raw_table_hosts = SnowflakeOperator(
        task_id="create_raw_table_hosts",
        snowflake_conn_id='snowflake_dev',
        sql="sql/demo1/raw_hosts.sql",
        params={},
    )

    create_raw_table_listings = SnowflakeOperator(
        task_id="create_raw_table_listings",
        snowflake_conn_id='snowflake_dev',
        sql="sql/demo1/raw_listings.sql",
        params={},
    )

    create_raw_table_reviews = SnowflakeOperator(
        task_id="create_raw_table_reviews",
        snowflake_conn_id='snowflake_dev',
        sql="sql/demo1/raw_reviews.sql",
        params={},
    )
   
    create_src_table_hosts = BashOperator(
        task_id='create_src_table_hosts',
        bash_command='cd dbt/demo1/models/src && dbt run --select "src_hosts.sql"'
    )

    create_src_table_listings = BashOperator(
        task_id='create_src_table_listings',
        bash_command='cd dbt/demo1/models/src && dbt run --select "src_listings.sql"'
    )

    create_src_table_reviews = BashOperator(
        task_id='create_src_table_reviews',
        bash_command='cd dbt/demo1/models/src && dbt run --select "src_reviews.sql"'
    )
    
    create_raw_table_hosts >>    create_raw_table_listings >>    create_raw_table_reviews 