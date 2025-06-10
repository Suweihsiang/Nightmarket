from airflow.decorators import dag
from tasks.update_user_to_mysql import update_USER_csv, update_USER_db_Table
from tasks.task_notify import task_start_notify, task_finish_notify
from utils.send_tele_message import send_failure_message

from datetime import datetime, timedelta

default_args = {
    "owner": "airflow",
    "email": ["your_email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": send_failure_message,
}


@dag(
    dag_id="d_04_update_user_to_mysql",
    default_args=default_args,
    description="update user info to MySQL daily",
    schedule_interval="0 7 * * *",
    start_date=datetime(2025, 5, 3),
    catchup=False,
    tags=["Step 4 : update user info to MySQL"],
)
def d_04_update_user_to_mysql():
    start = task_start_notify()
    df_new_user = update_USER_csv()
    update_db = update_USER_db_Table(df_new_user)
    finish = task_finish_notify()

    start >> df_new_user >> update_db >> finish


d_04_update_user_to_mysql()
