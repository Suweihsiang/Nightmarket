import os
from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


def start_mysql_engine() -> Engine:
    """
    建立連到MySQL的引擎
    """
    load_dotenv()
    user = os.getenv("MYSQL_USER")
    password = os.getenv("MYSQL_PASSWORD")
    host = os.getenv("MYSQL_HOST")
    port = os.getenv("MYSQL_PORT")
    db = os.getenv("MYSQL_DB")
    engine = create_engine(f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}")
    return engine
