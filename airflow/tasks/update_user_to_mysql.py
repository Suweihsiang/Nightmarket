import pandas as pd
from pandas import DataFrame
from sqlalchemy import text
from datetime import datetime, date
from utils.create_mysql_engine import start_mysql_engine

from airflow.decorators import task


@task
def update_USER_csv() -> DataFrame:
    """
    更新USER.csv檔案
    """
    df = pd.read_csv("./data/Clean/comments_Cleaned.csv")
    df_user = pd.read_csv("./data/USER.csv")
    df_today = df[df["create_date"] == datetime.strftime(date.today(), "%Y-%m-%d")]
    df_new_user = df_today[
        ["user_id", "user_name", "is_local_guide", "review_count", "photo_count"]
    ]
    df_user = pd.concat([df_user, df_new_user])
    df_user.drop_duplicates(subset=["user_id"], inplace=True)
    df_user.to_csv("./data/USER.csv", index=False, header=True, encoding="utf-8")
    return df_new_user


@task
def update_USER_db_Table(df_new_user: DataFrame) -> None:
    """
    更新MySQL的USER Table
    """
    engine = start_mysql_engine()
    data = df_new_user.to_dict(orient="records")
    sql = """
        INSERT INTO `USER`
        VALUES(:user_id,:user_name,:is_local_guide,:review_count,:photo_count)
        ON DUPLICATE KEY UPDATE 
            user_name = VALUES(user_name),
            is_local_guide = VALUES(is_local_guide),
            review_count = VALUES(review_count),
            photo_count = VALUES(photo_count);
    """

    update_record = {0: 0, 1: 0, 2: 0}
    with engine.begin() as conn:
        for d in data:
            result = conn.execute(text(sql), d)
            update_record[result.rowcount] += 1

    print(
        f"處理結果：\n未變動資料:{update_record[0]}筆\n新增資料:{update_record[1]}筆\n更新資料:{update_record[2]}筆"
    )
