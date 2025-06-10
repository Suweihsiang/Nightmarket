from create_mysql import start_engine,send_baseinfo,create_store_comment_table

if __name__ == "__main__":
    engine = start_engine("root", "password", "localhost", 3307, "NM_Project")
    send_baseinfo(engine)
    create_store_comment_table(engine)
