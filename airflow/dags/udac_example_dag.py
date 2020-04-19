from datetime import datetime, timedelta
import os
from airflow import DAG

from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,PostgresOperator)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2020, 3, 26),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup':False,
    'email_on_retry': False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow'
          #schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
  
# The create_tables task takes the redshift connection id and
# sql file as input to create all staging, fact and dimension tables
create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="create_tables.sql"
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/11/2018-11-01-events.json",
    json_path="s3://udacity-dend/log_json_path.json",
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/A/A/",
    json_path="auto",
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    insert_sql = """
        INSERT INTO songplays(start_time, userid, level, songid, artistid, sessionid, location, user_agent)
        (SELECT DATEADD(ms,se.ts,'1970-01-01'), se.userid, se.level, ss.song_id, ss.artist_id, se.sessionid, se.location, se.useragent
        FROM staging_events se
        LEFT JOIN staging_songs ss
        ON se.song = ss.title
        AND se.artist = ss.artist_name
        AND se.length = ss.duration
        WHERE se.userid IS NOT NULL
        AND se.page = 'NextSong');        
    """
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    insert_sql = """
        INSERT INTO users(userid, first_name, last_name, gender, level)
        SELECT DISTINCT userid, firstname, lastname, gender, level FROM staging_events;
    """
    
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    insert_sql = """
        INSERT INTO songs(songid, title, artistid, year, duration)
        SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs;
    """
     
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    insert_sql = """
        INSERT INTO artists(artistid, name, location, lattitude, longitude)
        SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude FROM staging_songs;
    """
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    insert_sql = """
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    (SELECT
    DATEADD(ms,ts,'1970-01-01'),
    DATE_PART(h,DATEADD(ms,ts,'1970-01-01')),
    DATE_PART(d,DATEADD(ms,ts,'1970-01-01')),
    DATE_PART(w,DATEADD(ms,ts,'1970-01-01')),
    DATE_PART(mon,DATEADD(ms,ts,'1970-01-01')),
    DATE_PART(y,DATEADD(ms,ts,'1970-01-01')),
    DATE_PART(weekday,DATEADD(ms,ts,'1970-01-01'))
    FROM staging_events);
    """
)

# The run_quality_checks takes inputs as table names of all dimension tables, 
# and checks of any of the dimension tables are not loaded correctly. Checks for absence of records
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    tables = ['users','songs','artists','time']
   
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator