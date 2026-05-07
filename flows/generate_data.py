"""Synthetic banking data generation as Prefect tasks.

Each task wraps a pure generator from `data_generation.synth`, adds retry +
caching policy, writes a CSV under `dbt_project/seeds/`, and returns the
path as a string. Returning paths (rather than DataFrames) keeps cache keys
small and serialisable: a second run with the same `seed` will hit the
result cache and the Cloud UI will show a 'Cached' badge on each task.
"""

from __future__ import annotations

from datetime import timedelta
from pathlib import Path

import pandas as pd
from prefect import flow
from prefect.assets import Asset, materialize
from prefect.tasks import task_input_hash

from data_generation import synth

CACHE_EXPIRATION = timedelta(hours=12)
COMMON_TASK_KWARGS = dict(
    retries=2,
    retry_delay_seconds=5,
    cache_key_fn=task_input_hash,
    cache_expiration=CACHE_EXPIRATION,
)


def _seed_asset(name: str) -> Asset:
    return Asset(key=f"file://dbt_project/seeds/{name}.csv")


CUSTOMERS_ASSET = _seed_asset("customers")
BRANCHES_ASSET = _seed_asset("branches")
PRODUCTS_ASSET = _seed_asset("products")
ACCOUNTS_ASSET = _seed_asset("accounts")
MERCHANTS_ASSET = _seed_asset("merchants")
TRANSACTIONS_ASSET = _seed_asset("transactions")


def _write(df: pd.DataFrame, name: str) -> str:
    synth.SEEDS_DIR.mkdir(parents=True, exist_ok=True)
    path = synth.SEEDS_DIR / f"{name}.csv"
    df.to_csv(path, index=False)
    print(f"wrote {len(df):>6,} rows -> {path}")
    return str(path)


@materialize(CUSTOMERS_ASSET, **COMMON_TASK_KWARGS)
def generate_customers(seed: int, n: int = 1000) -> str:
    """Second run with the same seed will hit Prefect's result cache —
    look for a 'Cached' badge on this task in the Cloud UI."""
    return _write(synth.generate_customers(seed=seed, n=n), "customers")


@materialize(BRANCHES_ASSET, **COMMON_TASK_KWARGS)
def generate_branches(seed: int, n: int = 20) -> str:
    return _write(synth.generate_branches(seed=seed, n=n), "branches")


@materialize(PRODUCTS_ASSET, **COMMON_TASK_KWARGS)
def generate_products() -> str:
    return _write(synth.generate_products(), "products")


@materialize(ACCOUNTS_ASSET, **COMMON_TASK_KWARGS)
def generate_accounts(seed: int, customers_path: str, branches_path: str, products_path: str) -> str:
    customers = pd.read_csv(customers_path, parse_dates=["signup_date", "date_of_birth"])
    branches = pd.read_csv(branches_path, parse_dates=["opened_date"])
    products = pd.read_csv(products_path)
    df = synth.generate_accounts(seed, customers, branches, products)
    return _write(df, "accounts")


@materialize(MERCHANTS_ASSET, **COMMON_TASK_KWARGS)
def generate_merchants(seed: int, n: int = 100) -> str:
    return _write(synth.generate_merchants(seed=seed, n=n), "merchants")


@materialize(TRANSACTIONS_ASSET, **COMMON_TASK_KWARGS)
def generate_transactions(
    seed: int,
    accounts_path: str,
    merchants_path: str,
    n: int = 50_000,
) -> str:
    accounts = pd.read_csv(accounts_path, parse_dates=["opened_date"])
    merchants = pd.read_csv(merchants_path)
    df = synth.generate_transactions(seed, accounts, merchants, n=n)
    return _write(df, "transactions")


@flow(name="generate-banking-data", log_prints=True)
def generate_banking_data(
    seed: int = 42,
    n_customers: int = 1000,
    n_transactions: int = 50_000,
) -> dict[str, str]:
    """Generate the six seed CSVs used by the dbt project."""
    customers = generate_customers(seed=seed, n=n_customers)
    branches = generate_branches(seed=seed)
    products = generate_products()
    merchants = generate_merchants(seed=seed)
    accounts = generate_accounts(
        seed=seed,
        customers_path=customers,
        branches_path=branches,
        products_path=products,
    )
    transactions = generate_transactions(
        seed=seed,
        accounts_path=accounts,
        merchants_path=merchants,
        n=n_transactions,
    )
    return {
        "customers": customers,
        "branches": branches,
        "products": products,
        "accounts": accounts,
        "merchants": merchants,
        "transactions": transactions,
    }


if __name__ == "__main__":
    generate_banking_data()