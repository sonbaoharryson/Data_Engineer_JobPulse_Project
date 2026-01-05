from scripts.utils.db_conn import DBConnection
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

def insert_itviec_jobs(jobs):
    """Insert IT Viec jobs into database"""

    engine = DBConnection().engine
    if not jobs:
        print("No IT Viec jobs to insert")

    #UpSert jobs into the database
    #Using ON CONFLICT to handle duplicates based on the URL
    query = text("""
        INSERT INTO staging.itviec_data_job (title, company, logo_url, url, working_location, work_model, tags, descriptions, requirements_and_experiences, posted_to_discord)
                VALUES (:title, :company, :logo, :url, :location, :mode, :tags, :descriptions, :requirements, FALSE)
        ON CONFLICT (url) DO UPDATE SET
            title = EXCLUDED.title,
            company = EXCLUDED.company,
            logo_url = EXCLUDED.logo_url,
            working_location = EXCLUDED.working_location,
            work_model = EXCLUDED.work_model,
            tags = EXCLUDED.tags,
            descriptions = EXCLUDED.descriptions,
            requirements_and_experiences = EXCLUDED.requirements_and_experiences,
            posted_to_discord = FALSE
        """)

    try:
        with engine.connect() as conn:
            transactions = conn.begin()
            try:
                conn.execute(query, jobs)
                transactions.commit()
            except SQLAlchemyError as e:
                transactions.rollback()
                print(f"Error inserting IT Viec jobs: {e}")
    except SQLAlchemyError as e:
        print(f"Database error: {e}")

def insert_topcv_jobs(jobs):
    """Insert TopCV jobs into database"""

    engine = DBConnection().engine    
    if not jobs:
        print("No TopCV jobs to insert")

    query = text("""
        INSERT INTO staging.topcv_data_job (title, company, logo_url, url, working_location, salary, descriptions, requirements, experiences, level_of_education, work_model, posted_to_discord)
                VALUES (:title, :company, :logo, :url, :location, :salary, :descriptions, :requirements, :experience, :education, :type_of_work, FALSE)
        ON CONFLICT (url) DO UPDATE SET
            title = EXCLUDED.title,
            company = EXCLUDED.company,
            logo_url = EXCLUDED.logo_url,
            working_location = EXCLUDED.working_location,
            salary = EXCLUDED.salary,
            descriptions = EXCLUDED.descriptions,
            requirements = EXCLUDED.requirements,
            experiences = EXCLUDED.experiences,
            level_of_education = EXCLUDED.level_of_education,
            work_model = EXCLUDED.work_model,
            posted_to_discord = FALSE
        """)

    try:
        with engine.connect() as conn:
            transactions = conn.begin()
            try:
                conn.execute(query, jobs)
                transactions.commit()
            except SQLAlchemyError as e:
                transactions.rollback()
                print(f"Error inserting TopCV jobs: {e}")
    except SQLAlchemyError as e:
        print(f"Database error: {e}")