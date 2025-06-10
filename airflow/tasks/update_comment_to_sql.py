import pandas as pd
from pandas import DataFrame
from datetime import date, datetime
from sqlalchemy import text
from utils.create_mysql_engine import start_mysql_engine

from airflow.decorators import task


@task
def get_new_comment() -> DataFrame:
    """
    取得要放入MySQL之新評論
    """
    df = pd.read_csv("./data/Clean/comments_Cleaned.csv")
    df_today = df[df["create_date"] == datetime.strftime(date.today(), "%Y-%m-%d")]
    df_store = pd.read_csv("./data/STORE.csv")
    df_new = df_today.merge(df_store, how="left", on="st_name")
    df_new = df_new[
        [
            "nm_id",
            "st_id",
            "user_id",
            "rating_star",
            "time_num",
            "time_unit",
            "months_ago",
            "content_clean",
            "create_date",
            "update_date",
        ]
    ]
    return df_new


@task
def update_COMMENT_db_Table(df_new: DataFrame):
    """
    將新評論存放至MySQL
    """
    engine = start_mysql_engine()
    df_new.to_sql("COMMENT", con=engine, if_exists="append", index=False)

    # 若資料庫中同一評論者在同一家店家有兩筆評論紀錄時，選擇留下最新的那筆
    query1 = """
            WITH tmp AS(
                SELECT comment_id, ROW_NUMBER() OVER(PARTITION BY st_id, user_id ORDER BY create_date DESC) AS rn FROM COMMENT 
            )
            DELETE FROM COMMENT
            WHERE comment_id IN 
            (SELECT comment_id FROM tmp WHERE rn > 1)
        """

    # 更新update_date為當日，並且更新months_ago
    query2 = """
            UPDATE NM_Project.COMMENT
            SET months_ago = months_ago + DATEDIFF(CURDATE(),update_date)/30,
                update_date = CURDATE()
        """

    query = [query1, query2]

    with engine.begin() as conn:
        for q in query:
            conn.execute(text(q))
