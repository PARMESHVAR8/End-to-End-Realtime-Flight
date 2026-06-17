# ✈️ Real-Time Flight Data Engineering Pipeline

> End-to-end streaming data platform — from live flight API to interactive dashboard — built with Apache Kafka, Airflow, Snowflake, and Streamlit.

![Pipeline Architecture](https://img.shields.io/badge/Architecture-Medallion-blue) ![Kafka](https://img.shields.io/badge/Apache-Kafka-black) ![Airflow](https://img.shields.io/badge/Apache-Airflow-017CEE) ![Snowflake](https://img.shields.io/badge/Snowflake-Data_Warehouse-29B5E8) ![Python](https://img.shields.io/badge/Python-3.10+-yellow) ![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)

---

## 📌 Project Overview

This project demonstrates a **production-grade real-time data engineering pipeline** that ingests live global flight data, streams it through Apache Kafka, processes it in a 3-layer Snowflake data warehouse, and surfaces analytics on an interactive Streamlit dashboard.

Built to mirror how companies like **IndiGo, Amadeus, and Booking.com** handle operational flight data at scale.

---

## 🏗️ Architecture

```
AviationStack API
       │
       ▼
 Kafka Producer  ──────────►  Kafka Topic (flights_raw)
                                      │
                                      ▼
                              Kafka Consumer
                                      │
                                      ▼
                            PostgreSQL (Staging)
                                      │
                                      ▼
                           Apache Airflow DAGs
                                      │
                    ┌─────────────────┼─────────────────┐
                    ▼                 ▼                  ▼
               RAW Schema       CLEAN Schema     ANALYTICS Schema
              (as-ingested)    (normalized)     (10 business views)
                                                         │
                                                         ▼
                                              Streamlit Dashboard
                                           (Plotly Charts + Pydeck Maps)
```

### Medallion Architecture (Snowflake)

| Layer | Schema | Description |
|-------|--------|-------------|
| Bronze | `RAW` | As-is ingested records, never modified |
| Silver | `CLEAN` | Deduplicated, type-cast, null-handled |
| Gold | `ANALYTICS` | 10 aggregated business intelligence views |

---

## 🛠️ Tech Stack

| Category | Technology |
|----------|-----------|
| **Streaming** | Apache Kafka, Zookeeper |
| **Orchestration** | Apache Airflow |
| **Data Warehouse** | Snowflake |
| **Staging DB** | PostgreSQL |
| **Processing** | Python, Pandas |
| **Infrastructure** | Docker Compose (8 services) |
| **Dashboard** | Streamlit, Plotly, Pydeck |
| **API Source** | AviationStack REST API |

---

## 📊 Key Features

- **Real-time ingestion** — Live flight data streamed every few seconds via Kafka
- **Fault-tolerant pipeline** — Kafka offset management ensures no data loss on restart
- **3-layer data warehouse** — Medallion architecture for clean data lineage and reprocessability
- **10 analytics views** — Airline performance, delay patterns, route analysis, on-time rates
- **Interactive dashboard** — Live flight maps (Pydeck), delay charts (Plotly), KPI metrics
- **Containerized** — Full 8-service stack via Docker Compose, runs locally in one command
- **Monitoring & alerting** — Pipeline health checks, Kafka consumer lag detection

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed
- Python 3.10+
- Snowflake account (free trial works)
- AviationStack API key (free tier available)

### 1. Clone & Configure

```bash
git clone https://github.com/PARMESHVAR8/flight-data-pipeline.git
cd flight-data-pipeline
cp .env.example .env
# Fill in your API keys in .env
```

### 2. Start Infrastructure

```bash
docker-compose up -d
```

Wait ~60 seconds for all services to be healthy.

### 3. Run the Producer

```bash
# In your Python virtual environment (PowerShell on Windows)
python flight_kafka/producer.py
```

### 4. Run the Consumer

```bash
python flight_kafka/consumer.py
```

### 5. Access Services

| Service | URL |
|---------|-----|
| Streamlit Dashboard | http://localhost:8501 |
| Apache Airflow | http://localhost:8080 |
| Kafka UI | http://localhost:8085 |

---

## 🗂️ Project Structure

```
flight-data-pipeline/
│
├── docker-compose.yml          # 8-service infrastructure definition
├── .env.example                # Environment variables template
│
├── flight_kafka/
│   ├── producer.py             # AviationStack API → Kafka topic
│   ├── consumer.py             # Kafka → PostgreSQL staging
│   └── config.py               # Kafka connection settings
│
├── airflow/
│   └── dags/
│       ├── staging_to_raw.py       # PostgreSQL → Snowflake RAW
│       ├── raw_to_clean.py         # RAW → CLEAN transformation
│       └── clean_to_analytics.py   # CLEAN → ANALYTICS views refresh
│
├── snowflake/
│   ├── ddl/
│   │   ├── raw_schema.sql          # RAW layer table definitions
│   │   ├── clean_schema.sql        # CLEAN layer table definitions
│   │   └── analytics_views.sql     # 10 business analytics views
│   └── transforms/
│       └── transformation_engine.py
│
├── dashboard/
│   ├── app.py                  # Main Streamlit application
│   ├── charts.py               # Plotly visualizations
│   └── maps.py                 # Pydeck flight route maps
│
├── monitoring/
│   └── pipeline_monitor.py     # Health checks & alerting
│
└── docs/
    └── architecture.md         # Detailed architecture decisions
```

---

## 📈 Analytics Views (Snowflake)

| View | Business Question |
|------|------------------|
| `airline_performance` | Which airlines have best on-time rates? |
| `route_delay_analysis` | Which routes are most delay-prone? |
| `hourly_flight_volume` | When is peak flight activity? |
| `delay_distribution` | How are delays distributed (short/medium/long)? |
| `top_routes` | Busiest routes by flight count |
| `airline_fleet_summary` | Aircraft types per airline |
| `arrival_performance` | Arrival delay trends |
| `airport_traffic` | Busiest origin/destination airports |
| `weekly_patterns` | Day-of-week flight volume trends |
| `real_time_status` | Live flight status breakdown |

---

## 🧠 Architecture Decisions

**Why Kafka over direct DB writes?**
Kafka decouples producers and consumers, enabling independent scaling, replay capability, and exactly-once semantics. Critical for any pipeline where data loss is unacceptable.

**Why Medallion Architecture?**
Separating RAW, CLEAN, and ANALYTICS layers ensures complete data lineage, easy reprocessing when business logic changes, and clean separation of concerns — the industry standard at Databricks, Airbnb, Uber.

**Why Airflow for orchestration?**
DAGs provide visual dependency management, retry logic, SLA monitoring, and alerting — far beyond what cron scheduling offers. Makes the pipeline observable and debuggable.

---

## 🪟 Windows-Specific Notes

- Docker PostgreSQL runs on port **5433** (5432 occupied by native Windows install)
- Scripts run from **PowerShell with venv activated** — use `localhost`, not Docker service names
- Kafka heap limit: `KAFKA_HEAP_OPTS: "-Xmx512m -Xms256m"` to prevent JVM crash
- Module prefix: `flight_kafka` (not `kafka`)

---

## 📸 Dashboard Screenshots

> *(Add screenshots of your Streamlit dashboard here)*

---

## 🔮 Future Enhancements

- [ ] Flight delay prediction model (XGBoost / LSTM)
- [ ] Real-time ML scoring via Kafka consumer
- [ ] dbt for transformation layer
- [ ] Great Expectations for data quality checks
- [ ] Terraform for cloud infrastructure provisioning

---

## 👤 Author

**Parmeshvar**
Data Engineering Portfolio Project | 2024

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2)](https://linkedin.com/in/YOUR_PROFILE)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717)](https://github.com/YOUR_USERNAME)

---

## 📄 License

MIT License — feel free to use this as a reference for your own data engineering projects.
