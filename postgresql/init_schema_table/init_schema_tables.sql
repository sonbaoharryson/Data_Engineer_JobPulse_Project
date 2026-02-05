\connect job_db_sm4x

CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS audit;

-- TopCV staging table
CREATE TABLE IF NOT EXISTS job_db_sm4x.staging.topcv_data_job (
    id SERIAL PRIMARY KEY,
    title VARCHAR(256),
    company TEXT,
    logo_url TEXT,
    url TEXT UNIQUE NOT NULL,
    job_category VARCHAR(256),
    working_location VARCHAR(256),
    salary VARCHAR(256),
    descriptions TEXT,
    requirements TEXT,
    experiences TEXT,
    level_of_education VARCHAR(256),
    work_model VARCHAR(256),
    posted_to_discord BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ITviec staging table
CREATE TABLE IF NOT EXISTS job_db_sm4x.staging.itviec_data_job (
    id SERIAL PRIMARY KEY,
    title VARCHAR(256),
    company TEXT,
    logo_url TEXT,
    url TEXT UNIQUE NOT NULL,
    job_category VARCHAR(256),
    working_location VARCHAR(256),
    work_model VARCHAR(256),
    tags TEXT,
    descriptions TEXT,
    requirements_and_experiences TEXT,
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




-- Audit table
CREATE TABLE IF NOT EXISTS audit.master_job_elt_audit (
    -- Primary Key
    audit_id SERIAL PRIMARY KEY,
    dag_run_id VARCHAR(250) NOT NULL,
    dag_id VARCHAR(250) NOT NULL DEFAULT 'master_job_elt',
    execution_date TIMESTAMP NOT NULL,
    logical_date TIMESTAMP,
    run_type VARCHAR(50),
    dag_status VARCHAR(50) NOT NULL,
    start_date TIMESTAMP,
    end_date TIMESTAMP,
    duration_seconds INTEGER,
    task_id VARCHAR(250),
    task_name VARCHAR(250),
    task_status VARCHAR(50),
    task_start_date TIMESTAMP,
    task_end_date TIMESTAMP,
    task_duration_seconds INTEGER,
    task_retry_count INTEGER DEFAULT 0,
    task_group VARCHAR(250),
    task_type VARCHAR(100),
    data_source VARCHAR(50),
    layer VARCHAR(50),
    rows_processed INTEGER DEFAULT 0,
    rows_inserted INTEGER DEFAULT 0,
    rows_updated INTEGER DEFAULT 0,
    rows_deleted INTEGER DEFAULT 0,
    rows_scraped INTEGER DEFAULT 0,
    rows_posted_discord INTEGER DEFAULT 0,
    dbt_models_run INTEGER DEFAULT 0,
    dbt_models_success INTEGER DEFAULT 0,
    dbt_models_failed INTEGER DEFAULT 0,
    dbt_command TEXT,
    error_message TEXT,
    error_type VARCHAR(250),
    log_url TEXT,
    exception_traceback TEXT,
    discord_posts_sent INTEGER DEFAULT 0,
    discord_posts_failed INTEGER DEFAULT 0,
    discord_channel_id VARCHAR(100),
    executor VARCHAR(100),
    operator VARCHAR(250),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_dag_run UNIQUE (dag_run_id, task_id, execution_date)
);

-- Image table for storing company logos
CREATE TABLE IF NOT EXISTS job_db_sm4x.staging.company_logos(
    logo_id TEXT DEFAULT NULL PRIMARY KEY,
    logo_url TEXT,
    logo_path TEXT DEFAULT NULL,
    is_downloaded BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT NULL
);
