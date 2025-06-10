import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine


def start_engine(user: str, password: str, host: str, port: int, db: str) -> Engine:
    """
    建立連接至MySQL資料庫的引擎。
    """
    # 設定資料庫連線字串
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")
    return engine


def send_baseinfo(engine: Engine) -> None:
    """
    將清理後的夜市、店家、評論與使用者資料匯MySQL資料庫。
    此為建立後續分析查詢基礎的主要資料表。
    """
    # 匯入評論資料
    print("Send Comments to MySQL")
    df_comment = pd.read_csv("./airflow/data/Clean/comments_Cleaned.csv")
    df_comment.to_sql(
        name="COMMENT_GENERAL", con=engine, if_exists="append", index=False
    )

    # 匯入店家資料
    print("Send Store Info to MySQL")
    df_store = pd.read_csv("./airflow/data/Clean/restaurants_Cleaned.csv")
    df_store.to_sql(name="STORE_GENERAL", con=engine, if_exists="append", index=False)

    # 匯入夜市資料
    print("Send Nightmarket Info to MySQL")
    df_nm = pd.read_csv("./airflow/data/NIGHTMARKET.csv")
    df_nm.to_sql(name="NIGHTMARKET", con=engine, if_exists="append", index=False)

    # 從評論資料中擷取使用者資訊並匯入
    print("Send User Info to MySQL")
    df_user = df_comment[
        ["user_id", "user_name", "is_local_guide", "review_count", "photo_count"]
    ].copy()
    df_user.sort_values(by=["review_count", "photo_count"],inplace=True)
    df_user.drop_duplicates("user_id", keep="last", inplace=True)
    df_user.to_sql(name="USER", con=engine, if_exists="append", index=False)


def create_store_comment_table(engine: Engine) -> None:
    """
    依據SQL腳本建立店家與評論的資料表。
    """
    # 讀取建立表格之sql檔
    with open("./MySQL/Create_Store_Comment.sql", "r", encoding="utf-8") as f:
        store_comment_sql = f.read()

    # 執行sql指令
    with engine.begin() as conn:
        for statement in store_comment_sql.strip().split(";"):
            if statement.strip():
                print(statement)
                conn.execute(text(statement + ";"))
