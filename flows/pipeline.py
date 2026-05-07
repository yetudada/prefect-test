"""Parent banking pipeline flow.

`retries=1` at the flow level retries the whole pipeline once if a
downstream task exhausts its own retries. That's two layers of safety net:
- task-level retries handle transient blips (subprocess failures, network)
- flow-level retry handles broader failures (e.g. dbt schema drift) by
  rerunning the entire DAG once.
"""

from __future__ import annotations

from prefect import flow, task
from prefect.runtime import flow_run as runtime_flow_run

from flows.generate_data import generate_banking_data
from flows.run_dbt import run_dbt

# DEMO: when True, `flaky_check` raises on the first attempt of the parent
# flow. With retries=0 on the task and retries=1 on the flow, this forces
# the whole pipeline to retry once before succeeding — the pure flow-level
# retry showcase. Flip to False to disable.
_DEMO_FLOW_FLAKY = True


@task(retries=0)
def flaky_check() -> None:
    """No task retries: any failure here propagates straight to the flow."""
    if not _DEMO_FLOW_FLAKY:
        return
    attempt = runtime_flow_run.run_count
    print(f"flaky_check: parent flow attempt {attempt}")
    if attempt == 1:
        raise RuntimeError(
            "Simulated post-pipeline check failure on flow attempt 1 "
            "(DEMO_FLOW_FLAKY=True). The flow-level retry will re-run the "
            "whole DAG and this task will succeed on attempt 2."
        )


@flow(name="banking-pipeline", retries=1, log_prints=True)
def banking_pipeline(
    seed: int = 42,
    n_customers: int = 1000,
    n_transactions: int = 50_000,
) -> None:
    generate_banking_data(seed=seed, n_customers=n_customers, n_transactions=n_transactions)
    run_dbt()
    flaky_check()


if __name__ == "__main__":
    banking_pipeline()