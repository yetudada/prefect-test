"""Parent banking pipeline flow.

`retries=1` at the flow level retries the whole pipeline once if a
downstream task exhausts its own retries. That's two layers of safety net:
- task-level retries handle transient blips (subprocess failures, network)
- flow-level retry handles broader failures (e.g. dbt schema drift) by
  rerunning the entire DAG once.
"""

from __future__ import annotations

from prefect import flow

from flows.generate_data import generate_banking_data
from flows.run_dbt import run_dbt


@flow(name="banking-pipeline", retries=1, log_prints=True)
def banking_pipeline(
    seed: int = 42,
    n_customers: int = 1000,
    n_transactions: int = 50_000,
) -> None:
    generate_banking_data(seed=seed, n_customers=n_customers, n_transactions=n_transactions)
    run_dbt()


if __name__ == "__main__":
    banking_pipeline()