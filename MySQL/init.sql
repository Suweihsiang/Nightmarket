CREATE DATABASE NM_Project;

CREATE TABLE NM_Project.COMMENT_GENERAL(
	comment_id INT NOT NULL AUTO_INCREMENT COMMENT "評論ID" PRIMARY KEY,
	nm_name varchar(50) NOT NULL COMMENT "夜市名稱",
	st_name varchar(200) NOT NULL COMMENT "店家名稱",
	nm_city varchar(10) NOT NULL COMMENT "店家所在縣市",
	st_address varchar(100) NOT NULL COMMENT "店家地址",
	user_name varchar(100) COMMENT "評論者名稱",
	user_id varchar(30) NOT NULL COMMENT "評論者ID",
	is_local_guide bool NOT NULL COMMENT "在地嚮導標記",
	review_count INT COMMENT "評論者總評論數",
	photo_count INT COMMENT "評論者發布照片數",
	rating_star INT NOT NULL COMMENT "評論星等",
	time_num INT NOT NULL COMMENT "評論時間值",
	time_unit varchar(8) NOT NULL COMMENT "評論時間單位",
	months_ago FLOAT NOT NULL COMMENT "以月為單位計算時間",
	content_clean TEXT COMMENT "評論內容",
	create_date DATE NOT NULL COMMENT "建立時間",
	update_date DATE NOT NULL COMMENT "更新時間"
)COMMENT = "評論總表";

CREATE TABLE NM_Project.STORE_GENERAL(
	st_id INT NOT NULL AUTO_INCREMENT COMMENT "店家ID" PRIMARY KEY,
	nm_name VARCHAR(50) NOT NULL COMMENT "夜市名稱",
	st_name VARCHAR(200) NOT NULL COMMENT "店家名稱",
	nm_city VARCHAR(10) NOT NULL COMMENT "店家所在縣市",
	st_address VARCHAR(100) COMMENT "店家地址",
	st_url TEXT NOT NULL COMMENT "店家網址",
	st_score FLOAT COMMENT "店家平均星等",
	st_price VARCHAR(50) COMMENT "店家均消",
	st_total_com INT COMMENT "店家評論總數",
	st_tag VARCHAR(200) COMMENT "店家熱門留言標籤"
)COMMENT = "店家總表";

CREATE TABLE NM_Project.NIGHTMARKET
(
	nm_id VARCHAR(3) NOT NULL COMMENT "夜市ID" PRIMARY KEY,
	nm_name VARCHAR(50) NOT NULL COMMENT "夜市名稱" UNIQUE,
	nm_city VARCHAR(10) NOT NULL COMMENT "夜市所在縣市"
)COMMENT = "夜市總表";

CREATE TABLE NM_Project.USER
(
	user_id VARCHAR(30) NOT NULL COMMENT "評論者ID" PRIMARY KEY,
	user_name VARCHAR(100) COMMENT "評論者名稱",
	is_local_guide BOOL NOT NULL COMMENT "在地嚮導標記",
	review_count INT COMMENT "評論者總評論數",
	photo_count INT COMMENT "評論者發布照片數"
);