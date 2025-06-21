# Qveristy Data Final Project 2025 Pipeline

An end-to-end (E2E) data pipeline that ingests raw JSON from S3, lands it in
Postgres (Bronze), builds **Silver** models, promotes them to a curated **Gold**
star-schema with dbt, and finally surfaces business insights/dashboards.

| Layer  | Tooling                | Purpose                                                  |
| ------ | ---------------------- | -------------------------------------------------------- |
| Raw    | S3                     | Immutable dumps of source files                          |
| Bronze | Python + Airflow       | JSON → Postgres table with no validation                 |
| Silver | dbt (`dbt_run_silver`) | Cleans, dedupes, normalises – produces `silver_*` models |
| Gold   | dbt (`dbt_run_gold`)   | Star-schema facts & dims – drives analytics + dashboards |

---

## 📊 Insights Uncovered

- **ARPU by plan type** – post-paid users monetise 1.7× vs. prepaid
- **Churn red-flags** – 9.3 % of customers with credit buckets _Poor/Fair_ miss ≥2 payments
- **Device mix** – Samsung leads overall, but Apple dominates high-credit segments
- **Top revenue service bundle** – _Streaming + Roaming_ drives +23 % incremental MRR

> ℹ️ Para obtener estos insights de negocio, en la carpeta [`/analysis`](./analysis) encontrarás un conjunto de queries SQL, uno por cada pregunta planteada, que operan exclusivamente sobre las tablas **gold**.  
> Cada consulta responde directamente a un KPI o métrica relevante para el negocio, utilizando hechos y dimensiones ya depurados y enriquecidos.

---

## 👤 Participant

- **Name**: Franco Buenahora
- **Email**: buenahorafranco@gmail.com

---

## 🚀 Quick Start

> **Prereqs:** Docker ≥ 24, docker-compose, 4 GB RAM free.

```bash
# clone & bring services up
git clone https://github.com/buenahora/qversity-data-2025-montevideo-francobuenahora.git
cd qversity-data-2025-montevideo-francobuenahora
docker compose up -d       # airflow + postgres + dbt

# open the Airflow UI → http://localhost:8080 (admin / admin)
# open dbt Docs (once generated) → http://localhost:8081
```

---

## 🏃‍♂️ Run the Pipeline

| Step                   | What it does                        | How to run                                                        |
| ---------------------- | ----------------------------------- | ----------------------------------------------------------------- |
| **1. Trigger E2E DAG** | Orchestrates Bronze → Silver → Gold | docker compose exec airflow airflow dags trigger e2e_pipeline_dag |
| **2. Monitor**         | Watch task-level progress           | Airflow UI → _DAGs_ → **e2e_pipeline_dag**                        |
| **3. Inspect data**    | Explore tables                      | `psql`, dbt Docs, or any SQL IDE                                  |

---

## ✅ Run Tests Manually

```bash
# runs both silver & gold test suites
docker compose exec dbt dbt test
```

- All **schema tests** live in `models/**/schema.yml`
- Unit-style assertions live in `tests/`

---

## 📚 Generate & Serve dbt Docs

```bash
# compiles docs + lineage
docker compose exec dbt dbt docs generate

 # serves at http://localhost:8081
docker compose exec dbt dbt docs serve --port 8080 --host 0.0.0.0
```
