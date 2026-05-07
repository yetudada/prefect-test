"""dbt commands wrapped as Prefect tasks via the prefect-dbt integration.

`PrefectDbtRunner` invokes dbt programmatically (no subprocess), reads the
generated `manifest.json`, and emits a Prefect Asset event for every model
in the project. The 20-node dbt DAG therefore shows up as 20 lineage nodes
under the run's Assets tab in the Cloud UI — and any failed model becomes a
stale asset that downstream models can react to.
"""

from __future__ import annotations

from pathlib import Path

from prefect import flow, task
from prefect.context import get_run_context
from prefect_dbt import PrefectDbtRunner
from prefect_dbt.core.settings import PrefectDbtSettings

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"

# DEMO: when True, the first attempt of `dbt deps` simulates a transient
# failure so you can see Prefect's task retries surface in the Cloud UI.
# Flip to False to disable.
_DEMO_FLAKY = True


def _runner() -> PrefectDbtRunner:
    return PrefectDbtRunner(
        settings=PrefectDbtSettings(
            project_dir=DBT_PROJECT_DIR,
            profiles_dir=DBT_PROJECT_DIR,
        )
    )


@task(retries=2, retry_delay_seconds=10)
def dbt_command(args: list[str]) -> None:
    """Task-level retries kick in on transient subprocess failures (e.g. a
    network blip while `dbt deps` fetches packages)."""
    if _DEMO_FLAKY and args[:1] == ["deps"]:
        attempt = get_run_context().task_run.run_count
        if attempt == 1:
            raise RuntimeError(
                "Simulated transient failure on first attempt of `dbt deps` "
                "(DEMO_FLAKY=True). The retry will succeed."
            )

    print(f"$ dbt {' '.join(args)}")
    _runner().invoke(args)


@flow(name="run-dbt", log_prints=True)
def run_dbt() -> None:
    dbt_command(["deps"])
    dbt_command(["seed"])
    dbt_command(["run"])
    dbt_command(["test"])


if __name__ == "__main__":
    run_dbt()