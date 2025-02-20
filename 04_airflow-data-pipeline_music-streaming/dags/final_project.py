from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
)
from helpers import SqlQueries

default_args = {
    "owner": "udacity",
    "start_date": pendulum.now(),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False,
}


@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval="0 * * * *",
)
def final_project():

    start_operator = DummyOperator(task_id="Begin_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        aws_conn_id="aws_credentials",
        redshift_conn_id="redshift",
        table="staging_events",
        s3_bucket=Variable.get("s3_udacity_bucket"),
        s3_key=Variable.get("s3_log_data_key"),
        json_path=Variable.get("s3_log_json_path"),
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
    )

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
    )

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
    )

    # 1. Extract data and load to fact table
    start_operator >> stage_events_to_redshift >> load_songplays_table
    start_operator >> stage_songs_to_redshift >> load_songplays_table

    # 2. Load to dimenstion tables with quality checks
    load_songplays_table >> load_user_dimension_table >> run_quality_checks
    load_songplays_table >> load_song_dimension_table >> run_quality_checks
    load_songplays_table >> load_artist_dimension_table >> run_quality_checks
    load_songplays_table >> load_time_dimension_table >> run_quality_checks

    # 3. Quality control checks and completion


final_project_dag = final_project()
