from airflow import DAG
from airflow.operators.bash import BashOperator  # ✅ Dùng import mới Airflow 2.x
from airflow.utils.dates import days_ago
from datetime import datetime

output_name = datetime.now().strftime("%Y%m%d")

schedule_interval = "@daily"
start_date = days_ago(1)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1
}

with DAG(
    dag_id="elt_reddit_pipeline",
    description="Reddit ELT - Load to Redshift & Postgres",
    schedule_interval=schedule_interval,
    default_args=default_args,
    start_date=start_date,
    catchup=True,
    max_active_runs=1,
    tags=["RedditETL"],
) as dag:

    extract_reddit_data = BashOperator(
        task_id="extract_reddit_data",
        bash_command=f"python /opt/airflow/extraction/extract_reddit_api.py {output_name}",
    )

    upload_to_s3 = BashOperator(
        task_id="upload_to_s3",
        bash_command=f"python /opt/airflow/extraction/upload_aws_s3_etl.py {output_name}",
    )

    copy_to_redshift = BashOperator(
        task_id="copy_to_redshift",
        bash_command=f"python /opt/airflow/extraction/upload_aws_redshift_etl.py {output_name}",
    )

    copy_to_postgres = BashOperator(
        task_id="copy_to_postgres",
        bash_command=f"python /opt/airflow/extraction/upload_postgres.py {output_name}",
    )

    slack_notification = BashOperator(
        task_id = "slack_notification",
        bash_command=f"python /opt/airflow/extraction/slack_notication.py",
    )
    # Task flow
    extract_reddit_data >> [upload_to_s3, copy_to_postgres]
    upload_to_s3 >> copy_to_redshift  >> slack_notification
