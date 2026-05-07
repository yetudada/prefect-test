# Prefect Banking Pipeline

A self-contained learning project that orchestrates a synthetic retail-banking pipeline with **Prefect Cloud**: it generates fake customers, accounts and transactions, loads them into **DuckDB**, then builds 20 **dbt** models on top.

The point isn't the data — it's seeing Prefect's flows, tasks, retries, caching, deployments, work pools and schedules light up in the Cloud UI against something more interesting than `print("hello")`.

## Prerequisites

- Python 3.11+ (3.13 is what this repo's venv is pinned to)
- [`uv`](https://docs.astral.sh/uv/) (preferred) or `pip` + venv
- A free [Prefect Cloud](https://app.prefect.cloud) account

## Local setup

```bash
uv sync
cp .env.example .env  # then fill in PREFECT_API_KEY / PREFECT_API_URL
```

## Authenticate to Prefect Cloud

```bash
prefect cloud login
# Or, if you populated .env:
# prefect config set PREFECT_API_URL=$PREFECT_API_URL
# prefect config set PREFECT_API_KEY=$PREFECT_API_KEY
```

## Create a work pool

```bash
prefect work-pool create banking-process-pool --type process
```

## Run the flow locally first (no deployment needed)

```bash
uv run python -m flows.pipeline
# Visit Prefect Cloud UI -> Flow Runs to watch it execute.
```

## Deploy with a schedule

```bash
prefect deploy --all
```

## Start a worker (in a second terminal)

A deployed flow is just metadata until a worker picks it up. The worker polls the work pool, leases scheduled runs, and executes them on whatever machine it's started on — your laptop, here.

```bash
prefect worker start --pool banking-process-pool
```

## Trigger an ad-hoc run

```bash
prefect deployment run banking-pipeline/banking-pipeline-daily
```

## What to look at in the Cloud UI

- **Flow Runs** — timeline of the parent `banking-pipeline` flow with subflows nested underneath.
- **Task tab** — on a *second* run with the same `seed`, the data-generation tasks should show a **Cached** badge instead of re-running.
- **Logs** — `log_prints=True` is on, so `print()` from inside tasks streams into Cloud.
- **Schedule view** — the daily 06:00 UTC cron from `prefect.yaml`.

## Project layout

```
.
├── flows/                  # Prefect flows + tasks
│   ├── generate_data.py    # Synthetic data generation tasks (cached)
│   ├── run_dbt.py          # dbt deps/seed/run/test wrapped as tasks
│   └── pipeline.py         # Parent flow chaining the two
├── data_generation/
│   └── synth.py            # Pure-Python generators (no Prefect imports)
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── packages.yml
│   ├── seeds/              # CSVs written by generate_banking_data
│   └── models/
│       ├── staging/        # 6 views, 1:1 with seeds
│       ├── intermediate/   # 4 joins/reshapes
│       ├── marts/          # 6 dims + facts
│       └── analytics/      # 4 business-ready aggregates
└── prefect.yaml            # Deployment manifest
```

## Notes / future explorations

- `flows/run_dbt.py` shells out via `subprocess` for clarity. The `prefect-dbt` integration is a drop-in upgrade if you want richer dbt artefacts in the Cloud UI.
- If you don't want to run a worker on your laptop, **Prefect Managed Work Pools** (paid) host workers for you — swap the `work_pool.name` in `prefect.yaml` for one you've created in Cloud.
- Volumes are knobs: `n_customers` and `n_transactions` are flow parameters if you want to stress-test larger runs.