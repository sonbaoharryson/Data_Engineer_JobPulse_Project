\connect job_db_sm4x

CREATE SCHEMA IF NOT EXISTS staging;

-- =========================
-- TopCV staging table
-- =========================
CREATE TABLE IF NOT EXISTS job_db_sm4x.staging.topcv_data_job (
    id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    company VARCHAR(150) NOT NULL,
    logo TEXT,
    url TEXT UNIQUE,
    location VARCHAR(100),
    salary VARCHAR(50),
    descriptions TEXT,
    requirements TEXT,
    experience VARCHAR(100),
    education VARCHAR(100),
    type_of_work VARCHAR(50),
    posted_to_discord BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =========================
-- ITviec staging table
-- =========================
CREATE TABLE IF NOT EXISTS job_db_sm4x.staging.itviec_data_job (
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
    posted_to_discord BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- FIXING SEQUENCE ID COLUMN
/* TRUNCATE TABLE discord_job_db.staging.itviec_data_job;
TRUNCATE TABLE discord_job_db.staging.topcv_data_job;
ALTER SEQUENCE discord_job_db.staging.topcv_data_job_id_seq RESTART WITH 1;
ALTER SEQUENCE discord_job_db.staging.itviec_data_job_id_seq RESTART WITH 1; */

-- HANDLING DUPLICATE DATA DUE TO UNCLEANSE URLS
/* create table temp_top_cv_data as
with cleanse_url as(
	select *, trim(SPLIT_PART(url, '?ta_source', 1)) new_url
	from staging.topcv_data_job
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
truncate table staging.topcv_data_job;
insert into staging.topcv_data_job
select * from temp_top_cv_data;
drop table temp_top_cv_data; */