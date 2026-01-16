# Job Crawling & Analytics Pipeline

A comprehensive data engineering pipeline for crawling job postings from multiple sources, storing them in a data warehouse, and posting them to Discord for job seekers. Built with Apache Airflow, PostgreSQL, and dbt following the Medallion Architecture.

## 🎯 Project Objective

This project aims to:
1. **Crawl job data** from multiple sources (TopCV, ITViec, and more in the future)
2. **Store data** in PostgreSQL for data warehouse and analytics purposes
3. **Post jobs** to Discord channels for job seekers to discover opportunities
4. **Process data** using dbt pipelines following Medallion Architecture (Bronze → Silver → Gold) for analytics
5. **Future**: Develop an interactive Discord chatbot to help job seekers find suitable jobs based on warehouse data

## 🏗️ Architecture Overview

```
┌─────────────────┐
│  Job Sources    │
│ (TopCV, ITViec) │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Airflow DAGs   │
│  (Crawling)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐      ┌──────────────┐
│  PostgreSQL     │─────▶│  Discord Bot  │
│  (Staging)      │      │  (Job Alerts) │
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
│  Analytics      │
│  (Gold Layer)   │
└─────────────────┘
```

## 📁 Project Structure

```
airflow/
├── dags/                          # Airflow DAG definitions
│   ├── job_itviec_pipeline.py    # ITViec crawling pipeline
│   ├── job_topcv_pipeline.py     # TopCV crawling pipeline
│   ├── sample_dag.py              # Sample DAG template
│   └── test_dag.py                # Test DAG
│
├── scripts/                       # Core application scripts
│   ├── crawl_scripts/            # Web scraping modules
│   │   └── crawl_job/
│   │       ├── crawler.py        # Main crawler interface
│   │       ├── it_viec.py        # ITViec scraper
│   │       ├── topcv.py          # TopCV scraper
│   │       └── helpers/          # Helper functions
│   │
│   ├── utils/                     # Utility modules
│   │   ├── db_conn.py            # Database connection
│   │   ├── insert_data_staging.py # Data insertion to staging
│   │   ├── formatter.py          # Discord embed formatter
│   │   ├── sender.py             # Discord job posting
│   │   └── load_crawl_source.py  # Source configuration loader
│   │
│   ├── source_itviec.json        # ITViec source URLs
│   ├── source_topcv.json         # TopCV source URLs
│   │
│   └── test/                      # Test scripts
│
├── dbt/                           # dbt project for data transformation
│   └── job_warehouse/
│       ├── models/
│       │   ├── bronze/           # Bronze layer (raw data views)
│       │   ├── silver/           # Silver layer (cleaned data)
│       │   └── gold/             # Gold layer (analytics-ready)
│       ├── dbt_project.yml        # dbt configuration
│       └── README.md             # dbt documentation
│
├── config/                        # Configuration files
│   └── airflow.cfg               # Airflow configuration
│
├── entrypoint/                    # Docker entrypoint scripts
│   └── entrypoint_airflow.sh    # Airflow initialization script
│
├── requirements.txt              # Python dependencies
└── README.mdxzz
```

## 🔄 Data Pipeline Flow

### 1. **Crawling Stage** (Bronze Layer)
- **DAGs**: `job_itviec_pipeline`, `job_topcv_pipeline`
- **Schedule**: Every 15 days
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
- **Purpose**: Raw data views from staging tables
- **Materialization**: Views
- **Models**:
  - `sample_query_itviec.sql`: ITViec raw data view
  - `sample_query_topcv.sql`: TopCV raw data view

#### **Silver Layer** (`models/silver/`)
- **Purpose**: Cleaned, standardized, and validated data
- **Materialization**: Incremental tables
- **Process**: Data quality checks, standardization, deduplication

#### **Gold Layer** (`models/gold/`)
- **Purpose**: Business-ready analytics tables
- **Materialization**: Tables
- **Use Cases**: Aggregations, metrics, business intelligence

### 4. **Discord Integration**
- **Functionality**: Automatically posts new job alerts to Discord channels
- **Format**: Rich embeds with job title, company, location, salary
- **Features**:
  - Throttling to avoid rate limits
  - Error handling and logging
  - Tracks posted jobs to prevent duplicates

## 📦 Key Dependencies

- `apache-airflow`: Workflow orchestration
- `selenium`: Web scraping
- `beautifulsoup4`: HTML parsing
- `psycopg2-binary`: PostgreSQL adapter
- `dbt-core`, `dbt-postgres`: Data transformation
- `discord.py`: Discord bot integration
- `sqlalchemy`: Database ORM

See `requirements.txt` for complete list.

## 🚀 Getting Started

### Prerequisites

- Python 3.11+
- PostgreSQL database
- Chrome/ChromeDriver (for web scraping)
- Discord bot token and channel ID
- Apache Airflow 2.9.1

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

## 🔮 Future Enhancements

1. **Additional Job Sources**: Expand to more job boards
2. **Discord Chatbot**: Interactive bot to help job seekers:
   - Search jobs by criteria (location, salary, skills)
   - Get job recommendations
   - Apply directly through Discord
   - Set up job alerts
3. **Advanced Analytics**: 
   - Job market trends
   - Salary analysis
   - Skill demand analysis
4. **Data Quality**: Enhanced validation and cleaning
5. **Monitoring**: Better observability and alerting

## 🤝 Contributing

To add a new job source:

1. Create a new scraper class in `scripts/crawl_scripts/crawl_job/`
2. Add it to `crawler.py`
3. Create a new DAG in `dags/`
4. Add source URLs JSON file
5. Create staging table in database
6. Add dbt models for bronze/silver/gold layers

## 📄 License

See LICENSE file in repository root.

## 👤 Author

**Bao Phan (HarrySon)**

---

For questions or issues, please open an issue in the repository.

