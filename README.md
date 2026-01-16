# Job Crawling & Analytics Pipeline

Design and implement an end-to-end data engineering pipeline that automatically crawls job postings from websites, ingests raw data into a **PostgreSQL** staging layer, publishes curated job alerts to a Discord channel, and builds a data warehouse using **dbt** with **Apache Trino** and **Iceberg**.

The pipeline is orchestrated with **Apache Airflow**, uses **MinIO** as object storage, and follows the Medallion Architecture (Bronze–Silver–Gold) to ensure data quality, scalability, and analytical readiness.

## 🎯 Project Objective

This project aims to:
1. **Crawl job data** from multiple sources (TopCV, ITViec, and could extend more sources in future 😊)
2. **Store data** in PostgreSQL for database and MinIO for lakehouse.
3. **Post jobs** to Discord channels for job seekers to discover opportunities
4. **Process data** using python to create script to crawl data from websites. Leveraging dbt to create pipelines for data warehouse following Medallion Architecture (Bronze → Silver → Gold) for analytics.
5. **Future**: Develop an interactive Discord chatbot to help job seekers find suitable jobs based on warehouse data (***MAYBE WHEN I'm 60*** 🤣).

## 🏗️ Architecture Overview

```
┌─────────────────┐
│  Job Sources    │
│ (TopCV, ITViec) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Python Crawling|
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────┐
│  PostgreSQL     │─────▶│  Discord Bot |
│  (Insert data)  │      │  (Job Alerts)│
└────────┬────────┘      └──────────────┘
         │
         ▼
┌─────────────────┐
│  dbt Pipeline   │
│  (Medallion)    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   BI Tools      │
│  (Still Not Working 🤣)   │
└─────────────────┘
```

## 🔄 Data Pipeline Flow

### 1. **Crawling Stage** (Bronze Layer)
- **DAGs**: `job_itviec_pipeline`, `job_topcv_pipeline`
- **Schedule**: Manual run (mainly for individual run purpose)
- **Process**:
  1. Load source URLs from JSON configuration files
  2. Scrape job postings using Selenium + ChromeDriver
  3. Extract job details (title, company, location, salary, etc.)
  4. Insert/update jobs into PostgreSQL staging tables
  5. Query unposted jobs and send to Discord
  6. Mark jobs as posted to avoid duplicates

### 2. **Staging Layer** (PostgreSQL)
- **Schema**: `staging`
- **Tables**:
  - `itviec_data_job`: ITViec job postings
  - `topcv_data_job`: TopCV job postings
- **Features**:
  - Upsert logic (ON CONFLICT) to handle duplicates
  - `posted_to_discord` flag to track Discord posting status

### 3. **Transformation Stage** (dbt - Medallion Architecture)

#### **Bronze Layer** (`models/bronze/`)
- **Purpose**: Raw data from staging tables
- **Materialization**: Incremental table
- **Models**:
  - Please run `dbt docs serve --port 8085` for dbt documentation.

#### **Silver Layer** (`models/silver/`)
- **Purpose**: Cleaned, standardized, and validated data
- **Materialization**: Table
- **Models**:
  - Please run `dbt docs serve --port 8085` for dbt documentation.

#### **Gold Layer** (`models/gold/`)
- **Purpose**: Business-ready analytics tables
- **Materialization**: Tables
- **Models**:
  - Please run `dbt docs serve --port 8085` for dbt documentation.

### 4. **Discord Integration**
- **Functionality**: Automatically posts new job alerts to Discord channels
- **Format**: Rich embeds with job title, company, location, salary
- **Features**:
  - Throttling to avoid rate limits
  - Error handling and logging
  - Tracks posted jobs to prevent duplicates

## 🚀 Getting Started

### Prerequisites

- Python
- Chrome/ChromeDriver (for web scraping)
- Discord bot token and channel ID
- Docker
  - PostgreSQL database
  - Apache Airflow
  - Apache Trino
  - MinIO

### Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd bot_chat_discord/airflow
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure environment variables**
   Create a `.env` file or set environment variables:
   ```bash
   DB_HOST=localhost
   DB_PORT=5432
   DB_USER=your_user
   DB_PASSWORD=your_password
   DB_AIRFLOW=airflow_db
   DB_JOB=job_db_sm4x
   DISCORD_TOKEN=your_discord_token
   DISCORD_CHANNEL_ID=your_channel_id
   AIRFLOW__WEBSERVER__SECRET_KEY=your_secret_key
   ```

4. **Configure source URLs**
   - Edit `scripts/source_itviec.json` for ITViec URLs
   - Edit `scripts/source_topcv.json` for TopCV URLs

5. **Set up database schema**
   - Create staging tables (see database scripts in `../db/scripts/`)
   - Ensure proper permissions

6. **Initialize Airflow**
   ```bash
   airflow db init
   airflow users create \
     --username admin \
     --firstname Admin \
     --lastname User \
     --role Admin \
     --email admin@example.com \
     --password admin
   ```

7. **Start Airflow**
   ```bash
   airflow webserver --port 8080
   airflow scheduler
   ```

### Running with Docker

See `docker-compose.yml` for Docker setup.

## 📊 DAGs Overview

### `itviec_data_pipeline`
- **Schedule**: Every 15 days (for testing); could set based on your preference.
- **Tasks**:
  1. `load_itviec_url`: Load ITViec source URLs
  2. `scrape_itviec_job`: Scrape job postings
  3. `insert_jobs`: Insert into staging table
  4. `post_job_to_discord`: Post new jobs to Discord

### `topcv_data_pipeline`
- **Schedule**: Every 15 days (for testing); could set based on your preference.
- **Tasks**:
  1. `load_topcv_url`: Load TopCV source URLs
  2. `scrape_topcv_job`: Scrape job postings
  3. `insert_jobs`: Insert into staging table
  4. `post_job_to_discord`: Post new jobs to Discord

## 🔧 Configuration

### Source Configuration
Source URLs are configured in JSON files:
- `scripts/source_itviec.json`: ITViec job search URLs
- `scripts/source_topcv.json`: TopCV job search URLs

Format:
```json
{
  "source_name": "https://example.com/jobs?query=..."
}
```

### Database Configuration
- **Staging Schema**: `staging` for crawled data ingestion layer.
- **Database**: Configured via `DB_JOB` environment variable
- **Connection**: Managed via `scripts/utils/db_conn.py`

### dbt Configuration
- **Profile**: `job_warehouse`
- **Database**: `job_db_sm4x`
- **Schemas**: `{default_dbt_schema}_bronze`, `{default_dbt_schema}_silver`, `{default_dbt_schema}_gold`
- See `dbt/job_warehouse/dbt_project.yml` for details

## 📈 Data Models

### Staging Tables

#### `itviec_data_job`
- `id` (PK)
- `title`
- `company`
- `logo_url`
- `url` (unique not null)
- `working_location`
- `work_model`
- `tags`
- `descriptions`
- `requirements_and_experiences`
- `posted_to_discord` (boolean)

#### `topcv_data_job`
- `id` (PK)
- `title`
- `company`
- `logo_url`
- `url` (unique)
- `working_location`
- `salary`
- `descriptions`
- `requirements`
- `experiences`
- `level_of_education`
- `work_model`
- `posted_to_discord` (boolean)

## 🧪 Testing

Test scripts are available in `scripts/test/` (*this test is just make sure scripts could be runable*):
- `test_crawl_it_viec.py`: Test ITViec scraper
- `test_crawl_topcv.py`: Test TopCV scraper
- `test_db_conn.py`: Test database connection
- `send_job.py`: Test Discord posting

## 👤 Author

**Bao Phan (HarrySon)**

---
Please give me 1 star 😋🔥

