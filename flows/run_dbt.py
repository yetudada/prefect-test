"""dbt commands wrapped as Prefect tasks.

Kept simple on purpose: shells out to the dbt CLI via subprocess. If you
want richer artefact rendering in the Cloud UI later, swap to the
`prefect-dbt` integration (no behaviour change at the orchestration layer).
"""

from __future__ import annotations

import subprocess
from pathlib import Path

from prefect import flow, task
from prefect.context import get_run_context

PROJECT_ROOT = Path(__file__).resolve().parents[1]
DBT_PROJECT_DIR = PROJECT_ROOT / "dbt_project"

# DEMO: when True, the first attempt of `dbt deps` simulates a transient
# failure so you can see Prefect's task retries surface in the Cloud UI.
# Flip to False to disable.
_DEMO_FLAKY = True


@task(retries=2, retry_delay_seconds=10)
def dbt_command(args: list[str], project_dir: Path = DBT_PROJECT_DIR) -> None:
    """Task-level retries kick in on transient subprocess failures (e.g. a
    network blip while `dbt deps` fetches packages)."""
    if _DEMO_FLAKY and args[:1] == ["deps"]:
        attempt = get_run_context().task_run.run_count
        if attempt == 1:
            raise RuntimeError(
                "Simulated transient failure on first attempt of `dbt deps` "
                "(DEMO_FLAKY=True). The retry will succeed."
            )

    cmd = [
        "dbt",
        *args,
        "--project-dir",
        str(project_dir),
        "--profiles-dir",
        str(project_dir),
    ]
    print(f"$ {' '.join(cmd)}")
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if result.stdout:
        print(result.stdout)
    if result.returncode != 0:
        if result.stderr:
            print(result.stderr)
        raise RuntimeError(f"dbt {' '.join(args)} failed (exit {result.returncode})")


@flow(name="run-dbt", log_prints=True)
def run_dbt() -> None:
    dbt_command(["deps"])
    dbt_command(["seed"])
    dbt_command(["run"])
    dbt_command(["test"])


if __name__ == "__main__":
    run_dbt()