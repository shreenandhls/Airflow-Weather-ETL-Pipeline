# Airflow Weather ETL Pipeline

An automated weather data ETL pipeline built using **Apache Airflow, Python, PostgreSQL, Docker and the Open-Meteo API**.

This project extracts current weather conditions for Tokyo, transforms the API response into structured records, and loads the processed data into a PostgreSQL database using an Airflow-managed workflow.

## Project Overview

The purpose of this project is to demonstrate an end-to-end ETL pipeline using Apache Airflow for workflow orchestration and PostgreSQL for data storage.

The pipeline follows three main stages:

1. **Extract:** Retrieve current weather information from the Open-Meteo API.
2. **Transform:** Process the JSON response and organize weather attributes into a structured record.
3. **Load:** Insert the transformed record into PostgreSQL.

Apache Airflow manages task dependencies, execution and scheduling.

## Architecture

```text
┌─────────────────┐      ┌─────────────────┐    ┌──────────────────┐     ┌─────────────────┐    ┌──────────────────┐
│  Open-Meteo API │ ───▶| Extract Weather │───▶│ Transform Weather│───▶│  Load Weather   │───▶│   PostgreSQL     │
│                 │      │      Data       │    │       Data       │     │      Data       │    │ Weather Database │
└─────────────────┘      └─────────────────┘    └──────────────────┘     └─────────────────┘    └──────────────────┘
                              ▲                       ▲                       ▲
                              └───────────────────────┼───────────────────────┘
                                                      │
                                         ┌────────────────────────┐
                                         │     Apache Airflow     │
                                         │  Orchestrates All Tasks│
                                         └────────────────────────┘
```

## Technologies Used

| Technology | Purpose |
|---|---|
| Python | Data extraction and transformation |
| Apache Airflow | Workflow orchestration |
| PostgreSQL | Relational database |
| Docker | Containerization |
| Astro CLI | Local Airflow development |
| Astro Runtime 3.4-1 | Airflow execution environment |
| Open-Meteo API | Weather data source |
| DBeaver | Database management |
| Git & GitHub | Version control |

## Data Source

The pipeline uses the [Open-Meteo API](https://open-meteo.com/) to retrieve current weather conditions.

**Location:** Tokyo, Japan

**Coordinates:**
- Latitude: 35.6895
- Longitude: 139.6917

### Weather attributes collected

| Attribute | Description |
|---|---|
| observation_time | Weather observation timestamp |
| temperature | Temperature in Celsius |
| humidity | Relative humidity percentage |
| precipitation | Precipitation in millimetres |
| pressure | Atmospheric pressure in hPa |
| weather_code | Weather condition code |
| apparent_temperature | Feels-like temperature |
| is_day | Daytime or nighttime indicator |

All timestamps are handled in GMT/UTC.

## ETL Workflow

### 1. Extraction

The `extract_weather` task uses Airflow's `HttpHook` to send a GET request to the Open-Meteo Forecast API.

It retrieves the current weather response in JSON format.

### 2. Transformation

The `transform_weather` task processes the API response using Python.

Transformation operations include:

- Extracting current weather attributes.
- Parsing the observation timestamp using Pendulum.
- Handling the timestamp in UTC.
- Converting the daytime indicator into a Boolean value.
- Structuring the values into a tuple for database insertion.

### 3. Loading

The `load_weather` task uses Airflow's `PostgresHook` to connect to PostgreSQL.

The task:

- Creates the `weather_data` table if it does not exist.
- Inserts the transformed weather record.
- Uses `observation_time` as the primary key.
- Uses PostgreSQL `ON CONFLICT` to update existing records with matching timestamps.

This prevents duplicate records for the same observation timestamp.

## Database Schema

**Database table:** `weather_data`

| Column | Data Type |
|---|---|
| observation_time | TIMESTAMPTZ (PRIMARY KEY) |
| temperature | DOUBLE PRECISION |
| humidity | DOUBLE PRECISION |
| precipitation | DOUBLE PRECISION |
| pressure | DOUBLE PRECISION |
| weather_code | INTEGER |
| apparent_temperature | DOUBLE PRECISION |
| is_day | BOOLEAN |

## Project Structure

```text
ETL Weather Pipeline/
│
├── dags/
│   ├── weather_data_DAG.py
│   ├── exampledag.py
│   └── .airflowignore
│
├── tests/
│   └── dags/
│       └── test_dag_example.py
│
├── include/
├── plugins/
│
├── Dockerfile
├── requirements.txt
├── packages.txt
├── .dockerignore
├── airflow_settings.yaml
└── README.md
```

## Local Setup and Execution

### Prerequisites

Install the following tools:

- Python
- Docker Desktop
- Astro CLI
- Git
- DBeaver (optional)

### 1. Clone the repository

```bash
git clone https://github.com/shreenandhls/Airflow-Weather-ETL-Pipeline.git

cd Airflow-Weather-ETL-Pipeline
```

### 2. Start the Airflow environment

```bash
astro dev start
```

Astro CLI starts the local Airflow environment using Docker containers.

### 3. Configure Airflow connections

Create the following Airflow connections.

**Open-Meteo HTTP Connection**

| Field | Value |
|---|---|
| Connection ID | open_meteo |
| Connection Type | HTTP |
| Host | api.open-meteo.com |
| Schema | https |

**PostgreSQL Connection**

| Field | Value |
|---|---|
| Connection ID | postgres_default |
| Connection Type | PostgreSQL |
| Host | Your PostgreSQL container hostname |
| Port | 5432 |
| Database | Your PostgreSQL database |
| Username | Your PostgreSQL username |
| Password | Your PostgreSQL password |

Configure credentials for your own environment. Do not commit passwords to GitHub.

### 4. Access the Airflow UI

Open:

http://localhost:8080

Find the `weather_data` DAG and trigger it manually.

### 5. Monitor the workflow

Monitor the following tasks in the Airflow UI:

- `extract_weather`
- `transform_weather`
- `load_weather`

Check the task logs to verify successful execution.

### 6. Verify the database

Connect to PostgreSQL using DBeaver and execute:

```sql
SELECT *
FROM weather_data
ORDER BY observation_time DESC;
```

To check the number of records:

```sql
SELECT COUNT(*)
FROM weather_data;
```

## Airflow Scheduling

The current DAG configuration uses:

```python
schedule="@daily"
catchup=False
max_active_runs=1
```

This schedules the pipeline once per day.

To collect current weather snapshots every hour, change the schedule to:

```python
schedule="@hourly"
```

## Key Features

- Automated ETL workflow using Apache Airflow.
- API integration using Open-Meteo.
- Python-based data transformation.
- PostgreSQL database integration.
- UTC/GMT timestamp handling.
- Duplicate prevention using primary keys and conflict handling.
- Docker-based development environment.
- Airflow task monitoring and execution management.

## Future Enhancements

- Collect weather information from multiple cities.
- Store historical weather observations.
- Integrate hourly and daily weather forecasts.
- Build a weather analytics dashboard.
- Implement data quality checks.
- Add failure notifications and logging.
- Deploy the pipeline to a cloud platform.

## Author

**Shreenandh L S**

GitHub Repository:

https://github.com/shreenandhls/Airflow-Weather-ETL-Pipeline

---

*This project demonstrates practical data engineering concepts, including API integration, ETL development, workflow orchestration, containerization and relational database management.*
