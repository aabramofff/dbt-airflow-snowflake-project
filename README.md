# 📈 Data Vault 2.0 on Snowflake with Airflow & dbt

This project is an automated end-to-end Data Engineering pipeline (ELT) built following the **Data Vault 2.0** methodology. It orchestrates data ingestion and transformation into **Snowflake** using **dbt** for modeling and **Apache Airflow** for orchestration.

---

## 📋 Project Architecture

The data is organized into logical layers according to Data Vault 2.0 standards:

* **Staging Layer**: Prepares raw data, handles type casting, and generates deterministic Hash Keys (`_hk`).
* **Raw Vault**: Permanent storage consisting of **Hubs** (business entities), **Links** (relationships), and **Satellites** (descriptive attributes).
* **Business Vault**: Advanced logic, including **Effectivity Satellite** to track relationship validity over time.
* **Information Marts**: Consumer-ready views and tables optimized for BI tools.

---

## 🛠 Tech Stack

* **Database**: Snowflake
* **Transformation**: dbt (Data Build Tool)
* **Orchestration**: Apache Airflow (utilizing Cosmos SDK)
* **Logging**: Loguru (with log rotation at `/opt/airflow/logs/dbt_custom.log`)
* **Package Manager**: `uv` (extremely fast Python package management)
* **Notifications**: Telegram Bot API

---

## 🚀 Quick Start

### 1. Environment Setup

Ensure you have Docker and Docker Compose installed. Create a `.env` file in the root directory with your credentials:

```env
# Snowflake
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_PASSWORD=your_password
SNOWFLAKE_ROLE=your_role
SNOWFLAKE_WAREHOUSE=your_warehouse
SNOWFLAKE_DATABASE=your_database
SNOWFLAKE_SCHEMA=your_schema

# Optional: if you want to use telegram notifications
TELEGRAM_TOKEN=your_token
TELEGRAM_CHAT_ID=your_chat_id
```

### 2. Launch Infrastructure

Use the provided `Makefile` for streamlined management:

```bash
# 1. Build project images
make build

# 2. Start all services in the background
make up

# 3. Stop all running containers
make down

# 4. Synchronize Python environment and dbt dependencies
make uv-sync
make deps

# 5. Initialize Data Vault (Seeds & Initial Run)
make seed
make dbt-build
```

### 3. Airflow Configuration

1. Access the Airflow UI at `localhost:8080`.
2. Create a Connection named `snowflake_conn` with your Snowflake details.
3. (Optional) Create an Airflow Variable named `telegram_config` (JSON) to manage notifications without restarting containers.

---

## 🛠 Management Commands (Makefile)

The project includes a `Makefile` to automate routine tasks. Below is a list of available commands categorized by their purpose:

### 🐳 Docker Infrastructure
| Command | Description |
| :--- | :--- |
| `make up` | Start all services (Airflow, Postgres) in detached mode |
| `make down` | Stop and remove all containers and networks |
| `make build` | Build or rebuild project Docker images |
| `make build-nocache` | Rebuild images from scratch without using cache |
| `make restart` | Restart all services |
| `make logs` | Stream Airflow Scheduler logs in real-time |
| `make bash` | Access the Airflow Scheduler container's shell |

### ❄️ dbt Operations (Snowflake)
| Command | Description |
| :--- | :--- |
| `make deps` | Install dbt packages (e.g., dbt-utils) |
| `make seed` | Load manual CSV data (seeds) to Snowflake |
| `make run` | Execute all dbt models |
| `make test` | Run dbt data tests to ensure data quality |
| `make dbt-build` | Full dbt execution: **seed + run + test** in one command |
| `make dbt-clean` | Clear dbt-generated files inside the container |

### 🐍 Python & Maintenance
| Command | Description |
| :--- | :--- |
| `make uv-sync` | Synchronize Python dependencies from `uv.lock` |
| `make uv-add pkg=<name>` | Add a new Python package (e.g., `make uv-add pkg=pandas`) |
| `make clean` | Clear local dbt `target/`, `logs/`, and `dbt_packages/` |

---

## 📉 Implementation Details

* **Incremental Strategy**: Hubs and Links utilize dbt's `incremental` materialization to ensure high performance and prevent duplicate business keys.
* **Effectivity Satellites**: Implemented SCD Type 2 logic to track the "active" status of relationships between Customers and Orders.
* **Professional Logging**: Integrated `loguru` for structured, color-coded task logging with 10MB rotation and 10-day retention policies.
* **Fail-safe Notifications**: The Telegram notification system uses a tiered lookup strategy (Variables -> Connections -> Env) for maximum reliability.

---

## 📂 Directory Structure

* `dbt_core/` — The dbt project (models, macros, seeds, snapshots).
* `airflow/dags/` — Main Airflow DAGs.
* `airflow/dags/utils/` — Custom Python utilities (logger, credentials handler, Telegram).
* `docker-compose.yaml` — Infrastructure definition for the Airflow environment.
* `uv.lock` / `pyproject.toml` — Dependency management files.

---
