CREATE TABLE NM_Project.STORE
SELECT 
	n.nm_id ,s.st_id ,s.st_name , s.st_address ,s.st_url ,s.st_score ,s.st_price ,s.st_total_com ,s.st_tag
FROM
	NM_Project.STORE_GENERAL s
LEFT JOIN
	NM_Project.NIGHTMARKET n
ON
	s.nm_name = n.nm_name;

ALTER TABLE NM_Project.STORE
ADD PRIMARY KEY(st_id);

ALTER TABLE NM_Project.STORE
MODIFY COLUMN st_id INT NOT NULL AUTO_INCREMENT COMMENT "店家ID",
MODIFY COLUMN nm_id VARCHAR(3) NOT NULL COMMENT "夜市ID";

ALTER TABLE NM_Project.STORE
ADD CONSTRAINT FK_NM_ID FOREIGN KEY(nm_id)
REFERENCES NM_Project.NIGHTMARKET(nm_id);

CREATE TABLE NM_Project.COMMENT
SELECT
	s.nm_id, s.st_id, u.user_id ,c.rating_star ,c.time_num ,c.time_unit ,c.months_ago ,c.content_clean ,c.create_date ,c.update_date
FROM
	NM_Project.COMMENT_GENERAL c
LEFT JOIN
	NM_Project.USER u
ON
	c.user_id = u.user_id
LEFT JOIN
	NM_Project.STORE s 
ON 
	c.st_name = s.st_name;

ALTER TABLE NM_Project.COMMENT
MODIFY COLUMN nm_id VARCHAR(3) NOT NULL COMMENT "夜市ID",
MODIFY COLUMN st_id INT NOT NULL COMMENT "店家ID",
MODIFY COLUMN user_id VARCHAR(30) NOT NULL COMMENT "評論者ID";

ALTER TABLE NM_Project.COMMENT 
ADD CONSTRAINT FK_CM_NM_ID FOREIGN KEY(nm_id) REFERENCES NM_Project.NIGHTMARKET(nm_id),
ADD CONSTRAINT FK_CM_ST_ID FOREIGN KEY(st_id) REFERENCES NM_Project.STORE(st_id),
ADD CONSTRAINT FK_CM_USER_ID FOREIGN KEY(user_id) REFERENCES NM_Project.USER(user_id);

ALTER TABLE NM_Project.COMMENT
ADD COLUMN comment_id INT NOT NULL AUTO_INCREMENT COMMENT "評論ID" PRIMARY KEY FIRST;

UPDATE NM_Project.COMMENT
SET months_ago = months_ago + DATEDIFF(CURDATE(),update_date)/30,
    update_date = CURDATE();

DROP TABLE NM_Project.COMMENT_GENERAL;

DROP TABLE NM_Project.STORE_GENERAL;