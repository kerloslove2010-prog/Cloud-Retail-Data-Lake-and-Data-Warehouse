"""
Generates the Cloud-Retail-Data-Lake-and-Data-Warehouse project.

Usage (inside your activated venv):
    python create_project.py --data "E:\\date\\data\\raw"

--data  : folder that contains raw_customers.csv, raw_orders.csv, raw_payments.csv (optional)
--dest  : where to create the project folder (default: current folder)
"""
import argparse
import shutil
from pathlib import Path

PROJECT = "Cloud-Retail-Data-Lake-and-Data-Warehouse"
FILES = {}

# ----------------------------------------------------------------------------
# Root files
# ----------------------------------------------------------------------------
FILES["requirements.txt"] = r'''duckdb
dbt-core>=1.10
dbt-duckdb>=1.10
'''

FILES[".gitignore"] = r'''venv/
.venv/
__pycache__/
*.duckdb
*.duckdb.wal
logs/
dbt_project/target/
dbt_project/logs/
lake/*/*
!lake/*/.gitkeep
'''

FILES["README.md"] = r'''# Cloud Retail Data Lake & Data Warehouse

An end-to-end ELT project for retail data (customers, orders, payments):
a **data lake** (raw -> bronze -> gold) feeding a **data warehouse** built with
**dbt + DuckDB** using dimensional modeling (star schema).

> Runs 100% locally and for free. Every component maps 1:1 to a cloud service
> (see "Cloud mapping"), so moving to AWS / Azure / GCP only means swapping
> storage paths and the dbt profile.

## Architecture

```mermaid
graph LR
    A[Source CSVs<br/>data/raw] --> B[Lake RAW<br/>lake/raw<br/>as-is, partitioned by ingest_date]
    B --> C[Lake BRONZE<br/>lake/bronze<br/>Parquet + audit columns]
    C --> D[Warehouse ODS<br/>schema ods]
    D --> E[dbt STAGING<br/>views: clean + rename]
    E --> F[dbt MARTS<br/>dim_* and fct_* tables]
    F --> G[Lake GOLD<br/>lake/gold Parquet]
    F --> H[BI / SQL analysis]
```

| Layer | Location | Purpose |
|---|---|---|
| RAW | `lake/raw/<table>/ingest_date=YYYY-MM-DD/` | Immutable copy of source files |
| BRONZE | `lake/bronze/<table>/ingest_date=.../*.parquet` | Parquet + `_ingested_at`, `_source_file` |
| ODS | DuckDB schema `ods` | Latest bronze snapshot loaded into the warehouse |
| STAGING | DuckDB schema `staging` | Rename, cast, light cleaning (views) |
| MARTS | DuckDB schema `marts` | Star schema for analytics (tables) |
| GOLD | `lake/gold/*.parquet` | Marts exported back to the lake for BI tools |

## Data model (star schema)

```
dim_customers --- fct_orders --- dim_date
                      |
                 fct_payments
customer_summary  (analytical model built on top of the star)
```

- **Dimensions:** `dim_customers`, `dim_date`
- **Facts:** `fct_orders` (grain: one order), `fct_payments` (grain: one payment)
- **Snapshot (SCD Type 2):** `snapshots.customers_snapshot`
- **Tests:** unique / not_null / relationships / accepted_values + a singular test

## Run it

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt

# put the 3 source CSVs in data/raw/, then:
python scripts/run_pipeline.py
```

Useful extras:

```powershell
cd dbt_project
dbt docs generate --profiles-dir .
dbt docs serve --profiles-dir .      # lineage graph in the browser
```

Query the warehouse:

```powershell
python -c "import duckdb; c=duckdb.connect('warehouse/retail.duckdb'); print(c.sql('select * from marts.customer_summary order by lifetime_value desc limit 10'))"
```

More queries are in `analysis/queries.sql`.

## Cloud mapping

| This project | AWS | Azure | GCP |
|---|---|---|---|
| `lake/` folders | S3 bucket (raw/bronze/gold prefixes) | ADLS Gen2 | Cloud Storage |
| `ingest_to_lake.py` | Lambda / Glue job | Data Factory | Cloud Run / Dataflow |
| Parquet catalog | Glue Catalog + Athena | Synapse serverless | BigQuery external tables |
| DuckDB warehouse | Redshift / Snowflake | Synapse / Snowflake | BigQuery / Snowflake |
| dbt profile | `dbt-redshift` / `dbt-snowflake` | `dbt-synapse` / `dbt-snowflake` | `dbt-bigquery` |
| `run_pipeline.py` | MWAA (Airflow) / Step Functions | Data Factory pipelines | Cloud Composer |
| BI | QuickSight | Power BI | Looker Studio |

The dbt models are plain SQL, so they carry over with minor syntax changes.

## Project structure

```
.
├── data/raw/                 source CSVs
├── lake/{raw,bronze,gold}/   data lake zones
├── warehouse/                DuckDB file
├── scripts/                  config, ingest, load, run_pipeline
├── dbt_project/              models, tests, snapshots, macros
└── analysis/queries.sql      example analytics queries
```

## Roadmap

- [ ] Replace local lake with an S3 bucket (boto3 / DuckDB httpfs)
- [ ] Schedule with Airflow
- [ ] CI with GitHub Actions (`dbt build` on every push)
- [ ] BI dashboard (Power BI / Metabase) on top of `marts`
'''

# ----------------------------------------------------------------------------
# Python scripts
# ----------------------------------------------------------------------------
FILES["scripts/config.py"] = r'''from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

RAW_DIR = ROOT / "data" / "raw"
LAKE_RAW = ROOT / "lake" / "raw"
LAKE_BRONZE = ROOT / "lake" / "bronze"
LAKE_GOLD = ROOT / "lake" / "gold"
DB_PATH = ROOT / "warehouse" / "retail.duckdb"
DBT_DIR = ROOT / "dbt_project"

TABLES = ["raw_customers", "raw_orders", "raw_payments"]
'''

FILES["scripts/ingest_to_lake.py"] = r'''# Step 1: source CSVs -> data lake (RAW as-is, BRONZE as Parquet)
import shutil
from datetime import date

import duckdb

from config import LAKE_BRONZE, LAKE_RAW, RAW_DIR, ROOT, TABLES


def main():
    ingest_date = date.today().isoformat()
    con = duckdb.connect()

    for table in TABLES:
        src = RAW_DIR / f"{table}.csv"
        if not src.exists():
            raise FileNotFoundError(f"Missing source file: {src}")

        raw_dir = LAKE_RAW / table / f"ingest_date={ingest_date}"
        bronze_dir = LAKE_BRONZE / table / f"ingest_date={ingest_date}"
        raw_dir.mkdir(parents=True, exist_ok=True)
        bronze_dir.mkdir(parents=True, exist_ok=True)

        # RAW zone: untouched copy of the source file
        shutil.copy2(src, raw_dir / src.name)

        # BRONZE zone: Parquet + audit columns
        out = bronze_dir / f"{table}.parquet"
        con.execute(f"""
            COPY (
                SELECT *,
                       current_timestamp AS _ingested_at,
                       '{src.name}'      AS _source_file
                FROM read_csv_auto('{src.as_posix()}', header = true)
            ) TO '{out.as_posix()}' (FORMAT PARQUET)
        """)

        n = con.execute(
            f"SELECT count(*) FROM read_parquet('{out.as_posix()}')"
        ).fetchone()[0]
        print(f"[ingest] {table}: {n} rows -> {out.relative_to(ROOT)}")

    con.close()


if __name__ == "__main__":
    main()
'''

FILES["scripts/load_to_ods.py"] = r'''# Step 2: latest BRONZE partition -> warehouse schema "ods"
import duckdb

from config import DB_PATH, LAKE_BRONZE, TABLES


def latest_file(table):
    partitions = sorted((LAKE_BRONZE / table).glob("ingest_date=*"))
    if not partitions:
        raise FileNotFoundError(f"No bronze data for {table}. Run ingest_to_lake.py first.")
    files = list(partitions[-1].glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet file in {partitions[-1]}")
    return files[0]


def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS ods")

    for table in TABLES:
        f = latest_file(table)
        con.execute(
            f"CREATE OR REPLACE TABLE ods.{table} AS "
            f"SELECT * FROM read_parquet('{f.as_posix()}')"
        )
        n = con.execute(f"SELECT count(*) FROM ods.{table}").fetchone()[0]
        print(f"[ods] ods.{table}: {n} rows")

    con.close()  # release the file lock before dbt runs


if __name__ == "__main__":
    main()
'''

FILES["scripts/run_pipeline.py"] = r'''# Runs the whole pipeline: ingest -> ods -> dbt build -> export gold
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb

import ingest_to_lake
import load_to_ods
from config import DB_PATH, DBT_DIR, LAKE_GOLD

GOLD_TABLES = [
    "dim_customers",
    "dim_date",
    "fct_orders",
    "fct_payments",
    "customer_summary",
]


def run_dbt(*args):
    exe = shutil.which("dbt", path=str(Path(sys.executable).parent)) or shutil.which("dbt")
    if not exe:
        sys.exit("dbt not found. Activate the venv and run: pip install -r requirements.txt")
    cmd = [exe, *args, "--profiles-dir", "."]
    print("[dbt]", " ".join(cmd))
    subprocess.run(cmd, cwd=DBT_DIR, check=True)


def export_gold():
    LAKE_GOLD.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH), read_only=True)
    for table in GOLD_TABLES:
        out = LAKE_GOLD / f"{table}.parquet"
        con.execute(f"COPY marts.{table} TO '{out.as_posix()}' (FORMAT PARQUET)")
        print(f"[gold] {table} -> {out.name}")
    con.close()


def main():
    print("== 1/4 Ingest: source -> data lake (raw + bronze)")
    ingest_to_lake.main()
    print("== 2/4 Load: bronze -> warehouse ODS")
    load_to_ods.main()
    print("== 3/4 Transform + test: dbt build")
    run_dbt("build")
    print("== 4/4 Publish: marts -> lake/gold")
    export_gold()
    print("Done.")


if __name__ == "__main__":
    main()
'''

# ----------------------------------------------------------------------------
# dbt project
# ----------------------------------------------------------------------------
FILES["dbt_project/dbt_project.yml"] = r'''name: retail_dw
version: "1.0.0"
config-version: 2
profile: retail_dw

model-paths: ["models"]
test-paths: ["tests"]
snapshot-paths: ["snapshots"]
macro-paths: ["macros"]
clean-targets: ["target", "dbt_packages"]

models:
  retail_dw:
    staging:
      +materialized: view
      +schema: staging
    marts:
      +materialized: table
      +schema: marts
'''

FILES["dbt_project/profiles.yml"] = r'''retail_dw:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: ../warehouse/retail.duckdb
      threads: 4
'''

FILES["dbt_project/macros/generate_schema_name.sql"] = r'''{#- Use the custom schema name as-is (staging / marts) instead of main_staging -#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
'''

# ---- staging ---------------------------------------------------------------
FILES["dbt_project/models/staging/sources.yml"] = r'''version: 2

sources:
  - name: ods
    schema: ods
    tables:
      - name: raw_customers
      - name: raw_orders
      - name: raw_payments
'''

FILES["dbt_project/models/staging/stg_customers.sql"] = r'''select
    id as customer_id,
    first_name,
    last_name
from {{ source('ods', 'raw_customers') }}
'''

FILES["dbt_project/models/staging/stg_orders.sql"] = r'''select
    id as order_id,
    user_id as customer_id,
    cast(order_date as date) as order_date,
    status
from {{ source('ods', 'raw_orders') }}
'''

FILES["dbt_project/models/staging/stg_payments.sql"] = r'''select
    id as payment_id,
    order_id,
    payment_method,
    amount / 100.0 as amount  -- source amounts are in cents
from {{ source('ods', 'raw_payments') }}
'''

FILES["dbt_project/models/staging/schema.yml"] = r'''version: 2

models:
  - name: stg_customers
    columns:
      - name: customer_id
        data_tests: [unique, not_null]

  - name: stg_orders
    columns:
      - name: order_id
        data_tests: [unique, not_null]
      - name: status
        data_tests:
          - accepted_values:
              arguments:
                values: ['placed', 'shipped', 'completed', 'return_pending', 'returned']

  - name: stg_payments
    columns:
      - name: payment_id
        data_tests: [unique, not_null]
'''

# ---- marts -----------------------------------------------------------------
FILES["dbt_project/models/marts/dim_customers.sql"] = r'''-- Dimension: one row per customer (descriptive attributes only)
select
    customer_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name
from {{ ref('stg_customers') }}
'''

FILES["dbt_project/models/marts/dim_date.sql"] = r'''-- Dimension: one row per calendar day
with days as (
    select cast(range as date) as date_day
    from range(date '2018-01-01', date '2019-01-01', interval 1 day)
)
select
    date_day,
    year(date_day)          as year_num,
    quarter(date_day)       as quarter_num,
    month(date_day)         as month_num,
    monthname(date_day)     as month_name,
    day(date_day)           as day_of_month,
    dayname(date_day)       as day_name,
    dayofweek(date_day) in (0, 6) as is_weekend
from days
'''

FILES["dbt_project/models/marts/fct_orders.sql"] = r'''-- Fact: one row per order
with orders as (
    select * from {{ ref('stg_orders') }}
),
payments as (
    select order_id, sum(amount) as amount
    from {{ ref('stg_payments') }}
    group by 1
)
select
    o.order_id,
    o.customer_id,
    o.order_date,
    o.status,
    coalesce(p.amount, 0) as amount
from orders o
left join payments p using (order_id)
'''

FILES["dbt_project/models/marts/fct_payments.sql"] = r'''-- Fact: one row per payment
select
    p.payment_id,
    p.order_id,
    o.customer_id,
    o.order_date,
    p.payment_method,
    p.amount
from {{ ref('stg_payments') }} p
left join {{ ref('stg_orders') }} o using (order_id)
'''

FILES["dbt_project/models/marts/customer_summary.sql"] = r'''-- Analytical model: customer-level measures built on the star schema
select
    c.customer_id,
    c.full_name,
    min(o.order_date)                  as first_order,
    max(o.order_date)                  as most_recent_order,
    count(o.order_id)                  as number_of_orders,
    coalesce(sum(o.amount), 0)         as lifetime_value
from {{ ref('dim_customers') }} c
left join {{ ref('fct_orders') }} o using (customer_id)
group by 1, 2
'''

FILES["dbt_project/models/marts/schema.yml"] = r'''version: 2

models:
  - name: dim_customers
    columns:
      - name: customer_id
        data_tests: [unique, not_null]

  - name: dim_date
    columns:
      - name: date_day
        data_tests: [unique, not_null]

  - name: fct_orders
    columns:
      - name: order_id
        data_tests: [unique, not_null]
      - name: customer_id
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('dim_customers')
                field: customer_id
      - name: order_date
        data_tests:
          - relationships:
              arguments:
                to: ref('dim_date')
                field: date_day

  - name: fct_payments
    columns:
      - name: payment_id
        data_tests: [unique, not_null]
      - name: order_id
        data_tests:
          - not_null
          - relationships:
              arguments:
                to: ref('fct_orders')
                field: order_id
      - name: payment_method
        data_tests:
          - accepted_values:
              arguments:
                values: ['credit_card', 'coupon', 'bank_transfer', 'gift_card']

  - name: customer_summary
    columns:
      - name: customer_id
        data_tests: [unique, not_null]
'''

FILES["dbt_project/tests/assert_no_negative_payments.sql"] = r'''-- Singular test: fails if any payment amount is negative
select *
from {{ ref('fct_payments') }}
where amount < 0
'''

FILES["dbt_project/snapshots/customers_snapshot.sql"] = r'''{% snapshot customers_snapshot %}

{{
    config(
        target_schema='snapshots',
        unique_key='customer_id',
        strategy='check',
        check_cols=['first_name', 'last_name']
    )
}}

select * from {{ ref('stg_customers') }}

{% endsnapshot %}
'''

# ----------------------------------------------------------------------------
# Analysis queries
# ----------------------------------------------------------------------------
FILES["analysis/queries.sql"] = r'''-- Revenue by month
select d.year_num, d.month_num, d.month_name,
       count(*)            as orders,
       sum(o.amount)       as revenue
from marts.fct_orders o
join marts.dim_date d on d.date_day = o.order_date
group by 1, 2, 3
order by 1, 2;

-- Top 10 customers by lifetime value
select customer_id, full_name, number_of_orders, lifetime_value
from marts.customer_summary
order by lifetime_value desc
limit 10;

-- Payment method share
select payment_method,
       count(*)                                         as payments,
       sum(amount)                                      as total_amount,
       round(100 * sum(amount) / sum(sum(amount)) over (), 1) as pct_of_total
from marts.fct_payments
group by 1
order by total_amount desc;

-- Orders by status
select status, count(*) as orders, sum(amount) as amount
from marts.fct_orders
group by 1
order by orders desc;
'''


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data", help="folder with raw_customers.csv, raw_orders.csv, raw_payments.csv")
    ap.add_argument("--dest", default=".", help="where to create the project folder")
    args = ap.parse_args()

    root = Path(args.dest).resolve() / PROJECT
    for rel, content in FILES.items():
        path = root / rel
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8", newline="\n") as f:
            f.write(content)

    for d in ["data/raw", "lake/raw", "lake/bronze", "lake/gold", "warehouse"]:
        (root / d).mkdir(parents=True, exist_ok=True)
        (root / d / ".gitkeep").touch()

    if args.data:
        src_dir = Path(args.data)
        for name in ["raw_customers.csv", "raw_orders.csv", "raw_payments.csv"]:
            src = src_dir / name
            if src.exists():
                shutil.copy2(src, root / "data" / "raw" / name)
                print(f"copied {name}")
            else:
                print(f"WARNING: {src} not found")

    print(f"\nProject created at: {root}")
    print("Next:")
    print(f'  cd "{root}"')
    print("  pip install -r requirements.txt")
    print("  python scripts/run_pipeline.py")


if __name__ == "__main__":
    main()
