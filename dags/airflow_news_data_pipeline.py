import logging
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from news_fetcher_etl import data_processor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

args = {"owner": "Airflow", "start_date": airflow.utils.dates.days_ago(2)}

dag = DAG(
    dag_id="news_processing_dag", 
	default_args=args, 
	schedule_interval=None
)

with dag:

	extract_news = PythonOperator(
		task_id='extract_news',
		python_callable=data_processor,
		dag=dag, 
	)
		
	upload_to_s3 = BashOperator(
		task_id="move_file_to_s3",
		bash_command='aws s3 mv {{ ti.xcom_pull("extract_news")}}  s3:// ',
	)
	
	create_snowflake_table = SnowflakeOperator(
		task_id="create_snowflake_table",
		sql="""create table if not exists NEWS_TABLE using template(select ARRAY_AGG(OBJECT_CONSTRUCT(*)) from TABLE(INFER_SCHEMA (LOCATION=>'@NEWS.PUBLIC.S3_STAGE',FILE_FORMAT=>'parquet_format')))
        """ ,
		snowflake_conn_id="snowflake_conn"
	)
	
	
	copy_to_snowflake = SnowflakeOperator(
		task_id="copy_to_snowflake",
		sql="""copy into NEWS.PUBLIC.NEWS_TABLE from @NEWS.PUBLIC.S3_STAGE MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE FILE_FORMAT=parquet_format
        """ ,
		snowflake_conn_id="snowflake_conn"
	)
	
extract_news >> upload_to_s3 >> create_snowflake_table >> copy_to_snowflake