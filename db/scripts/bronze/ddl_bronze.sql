CREATE TABLE IF NOT EXISTS job_db_sm4x.bronze.topcv_data_job (
    id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    company VARCHAR(150) NOT NULL,
    logo TEXT, 
    url TEXT UNIQUE, 
    location VARCHAR(100), 
    salary VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS job_db_sm4x.bronze.itviec_data_job (
    id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    company VARCHAR(150) NOT NULL,
    logo TEXT, 
    url TEXT UNIQUE, 
    location VARCHAR(100), 
    mode VARCHAR(50),
    tags VARCHAR(200),
    descriptions TEXT, 
    requirements TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Add new columns to bronze.itviec_data_job
ALTER TABLE job_db_sm4x.bronze.itviec_data_job
ADD COLUMN posted_to_discord BOOLEAN DEFAULT FALSE;


ALTER TABLE job_db_sm4x.bronze.topcv_data_job
ADD COLUMN descriptions TEXT,
ADD COLUMN requirements TEXT,
ADD COLUMN experience VARCHAR(100),
ADD COLUMN education VARCHAR(100),
ADD COLUMN type_of_work VARCHAR(50),
ADD COLUMN posted_to_discord BOOLEAN DEFAULT FALSE;

-- FIXING SEQUENCE ID COLUMN
/* TRUNCATE TABLE discord_job_db.bronze.itviec_data_job;
TRUNCATE TABLE discord_job_db.bronze.topcv_data_job;
ALTER SEQUENCE discord_job_db.bronze.topcv_data_job_id_seq RESTART WITH 1;
ALTER SEQUENCE discord_job_db.bronze.itviec_data_job_id_seq RESTART WITH 1; */

-- HANDLING DUPLICATE DATA DUE TO UNCLEANSE URLS
/* create table temp_top_cv_data as
with cleanse_url as(
	select *, trim(SPLIT_PART(url, '?ta_source', 1)) new_url
	from bronze.topcv_data_job
), rn_url as(
	select *,
		row_number() over (partition by new_url) rn
	from cleanse_url
), dedup as(
	select
		id,
		title,
		company,
		logo,
		new_url url,
		location,
		salary,
		created_at,
		descriptions,
		requirements,
		experience,
		education,
		type_of_work,
		posted_to_discord
	from rn_url
	where rn = 1
)
select * from dedup;
truncate table bronze.topcv_data_job;
insert into bronze.topcv_data_job
select * from temp_top_cv_data;
drop table temp_top_cv_data; */