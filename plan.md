# Prefect Cloud Banking Pipeline — Build Plan

A self-contained learning project to explore how Prefect Cloud handles data orchestration. The pipeline generates synthetic retail banking data, loads it into DuckDB, and runs a ~20-model dbt project on top. Designed to deploy to **Prefect Cloud** with a real work pool and schedule.

## Goals

The reader (Yetunde) wants to understand Prefect Cloud's capabilities by reading and running real code. The build should make the following Prefect concepts concrete and inspectable in the Cloud UI:

1. **Flows and tasks** — the core programming model.
2. **Retries** — task-level and flow-level retry behavior.
3. **Caching** — `task_input_hash`-based caching with TTL, so re-runs skip unchanged work.
4. **Deployments** — going from local Python to a scheduled run on Prefect Cloud.
5. **Work pools and workers** — how Prefect Cloud dispatches runs to compute.
6. **Schedules** — cron-based scheduling defined in `prefect.yaml`.

Out of scope (per Yetunde's answers): Prefect Blocks, Events, Automations, Artifacts, and any Airflow comparison.

## Tech stack

- **Prefect 3.x** (Prefect Cloud as the control plane)
- **dbt-core 1.8+** with the `dbt-duckdb` adapter
- **DuckDB** as the warehouse (single file, ephemeral per run)
- **Python 3.11+**, `uv` for dependency management (or `pip` + venv as fallback)
- **Faker** + **NumPy** for synthetic data generation

## Directory structure

```
prefect-banking-pipeline/
├── README.md                       # Setup + run instructions, including Prefect Cloud login
├── pyproject.toml                  # uv-managed deps; pins prefect, dbt-duckdb, faker, numpy, pandas
├── .env.example                    # PREFECT_API_KEY, PREFECT_API_URL placeholders
├── .gitignore                      # .venv, *.duckdb, target/, logs/, .env
├── prefect.yaml                    # Deployment manifest (work pool, schedule, entrypoint)
├── flows/
│   ├── __init__.py
│   ├── generate_data.py            # Tasks + flow that emit synthetic CSVs into seeds/
│   ├── load_data.py                # (Optional) load CSVs into DuckDB; otherwise dbt seed handles it
│   ├── run_dbt.py                  # Wraps `dbt deps`, `dbt seed`, `dbt run`, `dbt test`
│   └── pipeline.py                 # Parent flow: generate -> dbt run/test
├── data_generation/
│   ├── __init__.py
│   └── synth.py                    # Pure-Python data generators (no Prefect imports)
└── dbt_project/
    ├── dbt_project.yml
    ├── profiles.yml                # DuckDB profile pointing at ../warehouse.duckdb
    ├── packages.yml                # dbt_utils only (keeps it small)
    ├── seeds/                      # CSVs written by generate_data flow live here
    └── models/
        ├── staging/                # 6 models
        ├── intermediate/           # 4 models
        ├── marts/                  # 6 models
        └── analytics/              # 4 models
```

## Synthetic data spec (retail banking)

Generate deterministically from a `seed: int = 42` parameter. Volumes kept small so a full run finishes in <60 seconds.

| Entity | Rows | Notes |
|---|---|---|
| customers | 1,000 | Name, DOB, address, signup_date, segment, KYC status |
| branches | 20 | Branch ID, region, city, opened_date |
| products | 5 | Checking, Savings, Money Market, CD, Credit Card |
| accounts | ~1,500 | 1–3 per customer; opened_date ≥ customer signup_date; product_id; branch_id |
| merchants | 100 | Name, MCC code, country (mostly domestic, ~5% foreign) |
| transactions | ~50,000 | 6 months of activity; debit/credit; amount distribution skewed lognormal; ~0.5% flagged anomalies (off-hours + foreign + > $5K) |

Each generator returns a `pandas.DataFrame` and writes a CSV into `dbt_project/seeds/`. Keep generators pure (no Prefect imports) so they're unit-testable.

## The 20 dbt models

### Staging (6) — 1:1 with raw seeds; rename, cast, and lightly clean

1. `stg_customers`
2. `stg_accounts`
3. `stg_transactions`
4. `stg_branches`
5. `stg_merchants`
6. `stg_products`

Each staging model is a `view`. Define seeds in `_sources.yml` and add a few `not_null` / `unique` tests on primary keys.

### Intermediate (4) — joins and reshapes

7. `int_transactions_enriched` — join transactions to account → customer → merchant → product. One row per transaction with all dimensional context flattened.
8. `int_account_daily_balances` — running balance per account per day using window functions (`SUM(...) OVER (PARTITION BY account_id ORDER BY transaction_date)`); densified to one row per account per day.
9. `int_customer_segments` — bucket customers into Mass / Mass Affluent / Affluent based on total balances + transaction velocity over the last 90 days.
10. `int_merchants_categorized` — group merchants into spend categories (Groceries, Travel, Dining, Cash, Other) from MCC ranges.

### Marts (6) — Kimball-style dims and facts

11. `dim_customers` — one row per customer; joins in segment from `int_customer_segments`.
12. `dim_accounts` — one row per account; joins in product and branch attributes.
13. `dim_branches`
14. `dim_merchants` — joins in spend category from `int_merchants_categorized`.
15. `fct_transactions` — grain: one row per transaction. Surrogate keys to all dims.
16. `fct_daily_balances` — grain: one row per account per day, sourced from `int_account_daily_balances`.

Materialize marts as `table`. Add basic tests: PK uniqueness on dims, `relationships` tests on FKs in facts.

### Analytics (4) — business-ready aggregates

17. `mart_customer_ltv` — per-customer lifetime deposit/credit volume, fee revenue, and a simple LTV tier (Gold / Silver / Bronze).
18. `mart_fraud_signals` — flag transactions that meet ≥2 of: foreign merchant, >$5K, off-hours (00:00–05:00 local), >2σ from customer's 30-day average. One row per flagged transaction with reason flags.
19. `mart_branch_performance` — per-branch: customer count, total deposits, total transaction volume, MoM growth.
20. `mart_monthly_customer_summary` — per-customer per-month: txn count, net flow, ending balance, top spend category.

## Prefect flow design

### `flows/generate_data.py`

```python
from datetime import timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash

@task(retries=2, retry_delay_seconds=5,
      cache_key_fn=task_input_hash, cache_expiration=timedelta(hours=12))
def generate_customers(seed: int, n: int = 1000) -> str: ...

# One @task per entity (branches, products, accounts, merchants, transactions)
# with the same retry + cache decorators. Each writes a CSV to dbt_project/seeds/
# and returns the path as a string (so cache keys serialize cleanly).

@flow(name="generate-banking-data", log_prints=True)
def generate_banking_data(seed: int = 42) -> dict[str, str]:
    customers = generate_customers(seed)
    branches = generate_branches(seed)
    products = generate_products(seed)
    accounts = generate_accounts(seed, customers, branches, products)
    merchants = generate_merchants(seed)
    transactions = generate_transactions(seed, accounts, merchants)
    return {...}
```

**Why caching here:** the data generators are deterministic in `seed`. Caching demonstrates that re-runs with the same seed skip work — a clear contrast with always-rerun task semantics in some other orchestrators.

### `flows/run_dbt.py`

```python
import subprocess
from prefect import flow, task

@task(retries=2, retry_delay_seconds=10)
def dbt_command(args: list[str], project_dir: str = "dbt_project") -> None:
    result = subprocess.run(
        ["dbt", *args, "--project-dir", project_dir, "--profiles-dir", project_dir],
        check=True, capture_output=True, text=True,
    )
    print(result.stdout)

@flow(name="run-dbt", log_prints=True)
def run_dbt() -> None:
    dbt_command(["deps"])
    dbt_command(["seed"])
    dbt_command(["run"])
    dbt_command(["test"])
```

Note: keep `run_dbt` simple (subprocess) for clarity. If Yetunde later wants to see Prefect's dbt integration, swap in `prefect-dbt` — call this out in the README but don't add the dependency upfront.

### `flows/pipeline.py`

```python
from prefect import flow
from flows.generate_data import generate_banking_data
from flows.run_dbt import run_dbt

@flow(name="banking-pipeline", retries=1, log_prints=True)
def banking_pipeline(seed: int = 42) -> None:
    generate_banking_data(seed)
    run_dbt()
```

Parent flow has `retries=1` to demonstrate flow-level retry on top of task-level retries.

## `prefect.yaml` (deployment manifest)

```yaml
name: prefect-banking-pipeline
prefect-version: 3.0.0

deployments:
  - name: banking-pipeline-daily
    entrypoint: flows/pipeline.py:banking_pipeline
    work_pool:
      name: banking-process-pool
      work_queue_name: default
    schedules:
      - cron: "0 6 * * *"
        timezone: UTC
        active: true
    parameters:
      seed: 42
    tags: [banking, daily]
    description: |
      Daily synthetic banking pipeline. Generates customers/accounts/transactions,
      loads to DuckDB via dbt seed, and builds 20 dbt models.
```

## README content (must include, in this order)

1. **What this is** — one paragraph.
2. **Prerequisites** — Python 3.11+, `uv` (or pip), free Prefect Cloud account.
3. **Local setup** — `uv sync`, copy `.env.example` to `.env`.
4. **Authenticate to Prefect Cloud**:
   ```bash
   prefect cloud login
   # Or set PREFECT_API_KEY and PREFECT_API_URL in .env and run:
   # prefect config set PREFECT_API_URL=$PREFECT_API_URL
   ```
5. **Create a work pool**:
   ```bash
   prefect work-pool create banking-process-pool --type process
   ```
6. **Run the flow locally first** (no deployment needed):
   ```bash
   uv run python -m flows.pipeline
   # Visit Prefect Cloud UI -> Flow Runs to see it
   ```
7. **Deploy with a schedule**:
   ```bash
   prefect deploy --all
   ```
8. **Start a worker** (in a second terminal — explain why):
   ```bash
   prefect worker start --pool banking-process-pool
   ```
9. **Trigger an ad-hoc run from the CLI or UI**:
   ```bash
   prefect deployment run banking-pipeline/banking-pipeline-daily
   ```
10. **What to look at in the Cloud UI** — flow run timeline, task tab showing cache hits on second run, logs, schedule view.

## Suggested implementation order

1. `pyproject.toml`, `.gitignore`, `.env.example`, `README.md` skeleton.
2. `data_generation/synth.py` — pure generators, runnable standalone with `python -m data_generation.synth`. Verify CSVs look right before touching dbt.
3. `dbt_project/` skeleton: `dbt_project.yml`, `profiles.yml`, `packages.yml`, `_sources.yml`. Run `dbt debug` to confirm DuckDB profile works.
4. Staging models (6) + sources YAML + a couple of tests. Run `dbt run --select staging` and `dbt test --select staging`.
5. Intermediate models (4). Run them.
6. Marts (6) + tests. Run them.
7. Analytics (4). Run them.
8. `flows/generate_data.py` — wrap generators in `@task`s with retries + caching.
9. `flows/run_dbt.py`.
10. `flows/pipeline.py` — parent flow.
11. Run end-to-end locally against Prefect Cloud (already authenticated).
12. `prefect.yaml` and `prefect deploy --all`.
13. Start worker, trigger ad-hoc run, verify in Cloud UI.

Each step should leave the project in a working state — don't write all the dbt models before validating the first one runs.

## Things to demonstrate explicitly in code comments

- On `generate_customers`: "second run with same seed will hit Prefect's result cache — see Cloud UI 'Cached' badge on the task."
- On `dbt_command`: "task-level retries kick in on transient subprocess failures (network blips fetching dbt deps, etc.)."
- On `banking_pipeline`: "flow-level retry retries the whole pipeline once if a downstream task exhausts its own retries."
- In `prefect.yaml`: "schedule + work pool define where and when this runs in production. Local `python -m flows.pipeline` bypasses both — that's the dev loop."

## Open decisions for the implementer

- **dbt seed vs. direct DuckDB load:** plan above uses `dbt seed`. If seed times out at 50K transactions, switch `flows/load_data.py` to load CSVs directly with `duckdb.read_csv` and demote the txn CSV from a dbt seed to a dbt source.
- **Worker on local machine vs. Prefect Managed:** plan assumes a local `prefect worker`. If Yetunde wants zero infra, mention Prefect Managed Work Pools (paid) in the README as an alternative.
- **Volume scaling knob:** expose `n_customers`, `n_transactions` as flow parameters so she can stress-test with larger volumes later.

---

Hand this plan to Claude Code along with the empty scaffold at `prefect-banking-pipeline/` (already created).