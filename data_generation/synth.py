"""Synthetic retail-banking data generators.

Pure Python; no Prefect imports. Each generator returns a `pandas.DataFrame`,
deterministic in the integer `seed`. `write_all` materialises every entity as
a CSV under `dbt_project/seeds/`.

Run standalone to regenerate seeds:

    python -m data_generation.synth
"""

from __future__ import annotations

from datetime import date, datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from faker import Faker

SEEDS_DIR = Path(__file__).resolve().parents[1] / "dbt_project" / "seeds"

# Fixed reference "today" so CSV outputs are reproducible across machines/dates.
TODAY = date(2024, 12, 31)
WINDOW_DAYS = 180  # transactions span the last 6 months

PRODUCTS: list[tuple[str, str, str]] = [
    ("p1", "Checking", "Deposit"),
    ("p2", "Savings", "Deposit"),
    ("p3", "Money Market", "Deposit"),
    ("p4", "CD", "Deposit"),
    ("p5", "Credit Card", "Credit"),
]

REGIONS: dict[str, list[tuple[str, str]]] = {
    "Northeast": [("New York", "NY"), ("Boston", "MA"), ("Philadelphia", "PA"), ("Newark", "NJ")],
    "Southeast": [("Atlanta", "GA"), ("Miami", "FL"), ("Charlotte", "NC")],
    "Midwest": [("Chicago", "IL"), ("Detroit", "MI"), ("Minneapolis", "MN")],
    "Southwest": [("Houston", "TX"), ("Dallas", "TX"), ("Phoenix", "AZ")],
    "West": [("Seattle", "WA"), ("San Francisco", "CA"), ("Los Angeles", "CA"), ("Portland", "OR")],
}

SEGMENTS = ["Mass", "Mass Affluent", "Affluent"]
SEGMENT_WEIGHTS = [0.70, 0.22, 0.08]

KYC_STATUSES = ["Approved", "Pending", "Rejected"]
KYC_WEIGHTS = [0.92, 0.06, 0.02]

# Rough MCC ranges per spend category. The dbt int_merchants_categorized model
# re-derives the category from the MCC so that the bucketing logic lives in dbt.
MCC_CATEGORIES: dict[str, list[tuple[int, int]]] = {
    "Groceries": [(5411, 5499)],
    "Travel": [(3000, 3299), (4511, 4511), (7011, 7011)],
    "Dining": [(5811, 5814)],
    "Cash": [(6010, 6011)],
    "Other": [(5000, 5999), (7000, 7999)],
}
MCC_CATEGORY_WEIGHTS = {
    "Groceries": 0.30,
    "Travel": 0.15,
    "Dining": 0.25,
    "Cash": 0.10,
    "Other": 0.20,
}

FOREIGN_COUNTRIES = ["GBR", "DEU", "FRA", "JPN", "CAN", "MEX", "ESP", "AUS"]


def _rng(seed: int) -> np.random.Generator:
    return np.random.default_rng(seed)


def _faker(seed: int) -> Faker:
    fake = Faker("en_US")
    Faker.seed(seed)
    return fake


def _pick_city(rng: np.random.Generator, region: str) -> tuple[str, str]:
    options = REGIONS[region]
    return options[int(rng.integers(0, len(options)))]


def generate_customers(seed: int = 42, n: int = 1000) -> pd.DataFrame:
    rng = _rng(seed)
    fake = _faker(seed)

    first_names = [fake.first_name() for _ in range(n)]
    last_names = [fake.last_name() for _ in range(n)]
    dobs = [fake.date_of_birth(minimum_age=18, maximum_age=85) for _ in range(n)]
    streets = [fake.street_address() for _ in range(n)]

    chosen_regions = rng.choice(list(REGIONS.keys()), size=n)
    cities, states = [], []
    for r in chosen_regions:
        c, s = _pick_city(rng, str(r))
        cities.append(c)
        states.append(s)

    postal_codes = [f"{int(rng.integers(10000, 100000)):05d}" for _ in range(n)]

    signup_offsets = rng.integers(0, 8 * 365, size=n)
    signup_dates = [TODAY - timedelta(days=int(d)) for d in signup_offsets]

    emails = [
        f"{fn.lower()}.{ln.lower()}{i+1}@example.com"
        for i, (fn, ln) in enumerate(zip(first_names, last_names))
    ]

    return pd.DataFrame({
        "customer_id": [f"c{i:05d}" for i in range(1, n + 1)],
        "first_name": first_names,
        "last_name": last_names,
        "date_of_birth": dobs,
        "email": emails,
        "street_address": streets,
        "city": cities,
        "state": states,
        "postal_code": postal_codes,
        "signup_date": signup_dates,
        "segment": rng.choice(SEGMENTS, size=n, p=SEGMENT_WEIGHTS),
        "kyc_status": rng.choice(KYC_STATUSES, size=n, p=KYC_WEIGHTS),
    })


def generate_branches(seed: int = 42, n: int = 20) -> pd.DataFrame:
    rng = _rng(seed + 1)

    chosen_regions = rng.choice(list(REGIONS.keys()), size=n)
    cities, states = [], []
    for r in chosen_regions:
        c, s = _pick_city(rng, str(r))
        cities.append(c)
        states.append(s)

    opened_offsets = rng.integers(365, 30 * 365, size=n)
    opened_dates = [TODAY - timedelta(days=int(d)) for d in opened_offsets]

    return pd.DataFrame({
        "branch_id": [f"b{i:03d}" for i in range(1, n + 1)],
        "region": chosen_regions,
        "city": cities,
        "state": states,
        "opened_date": opened_dates,
    })


def generate_products() -> pd.DataFrame:
    return pd.DataFrame(PRODUCTS, columns=["product_id", "product_name", "product_type"])


def generate_accounts(
    seed: int,
    customers: pd.DataFrame,
    branches: pd.DataFrame,
    products: pd.DataFrame,
) -> pd.DataFrame:
    rng = _rng(seed + 2)

    # 1-3 accounts per customer, skewed toward 1 to hit ~1,500 total at n=1,000.
    n_per_customer = rng.choice([1, 2, 3], size=len(customers), p=[0.60, 0.30, 0.10])
    total = int(n_per_customer.sum())

    customer_ids = np.repeat(customers["customer_id"].to_numpy(), n_per_customer)
    signups = np.repeat(customers["signup_date"].to_numpy(), n_per_customer)

    product_ids = rng.choice(products["product_id"].to_numpy(), size=total)
    branch_ids = rng.choice(branches["branch_id"].to_numpy(), size=total)

    days_to_today = np.array([max((TODAY - s).days, 1) for s in signups])
    offsets = (rng.random(size=total) * days_to_today).astype(int)
    opened_dates = [s + timedelta(days=int(o)) for s, o in zip(signups, offsets)]

    statuses = np.where(rng.random(size=total) > 0.05, "Active", "Closed")

    is_credit_card = product_ids == "p5"
    credit_limits_raw = rng.integers(1000, 25001, size=total)
    credit_limits = pd.array(
        [int(v) if cc else pd.NA for v, cc in zip(credit_limits_raw, is_credit_card)],
        dtype="Int64",
    )

    return pd.DataFrame({
        "account_id": [f"a{i:06d}" for i in range(1, total + 1)],
        "customer_id": customer_ids,
        "product_id": product_ids,
        "branch_id": branch_ids,
        "opened_date": opened_dates,
        "status": statuses,
        "credit_limit": credit_limits,
    })


def generate_merchants(seed: int = 42, n: int = 100) -> pd.DataFrame:
    rng = _rng(seed + 3)
    fake = _faker(seed + 3)

    names = [fake.company() for _ in range(n)]

    cats = list(MCC_CATEGORY_WEIGHTS.keys())
    weights = [MCC_CATEGORY_WEIGHTS[c] for c in cats]
    chosen_cats = rng.choice(cats, size=n, p=weights)

    mccs: list[int] = []
    for cat in chosen_cats:
        ranges = MCC_CATEGORIES[str(cat)]
        lo, hi = ranges[int(rng.integers(0, len(ranges)))]
        mccs.append(int(rng.integers(lo, hi + 1)))

    countries = ["USA"] * n
    n_foreign = max(1, int(round(n * 0.05)))
    foreign_idx = rng.choice(n, size=n_foreign, replace=False)
    for i in foreign_idx:
        countries[int(i)] = FOREIGN_COUNTRIES[int(rng.integers(0, len(FOREIGN_COUNTRIES)))]

    return pd.DataFrame({
        "merchant_id": [f"m{i:03d}" for i in range(1, n + 1)],
        "merchant_name": names,
        "mcc_code": mccs,
        "country": countries,
    })


def generate_transactions(
    seed: int,
    accounts: pd.DataFrame,
    merchants: pd.DataFrame,
    n: int = 50_000,
) -> pd.DataFrame:
    """Vectorised transaction generation.

    ~99.5% of rows are 'normal': domestic merchant, business hours, lognormal
    amounts. ~0.5% are planted anomalies (foreign merchant + off-hours +
    >$5K) so that the dbt fraud-signals rules have something to fire on.
    """
    rng = _rng(seed + 4)

    end = datetime.combine(TODAY, datetime.max.time()).replace(microsecond=0)
    start = end - timedelta(days=WINDOW_DAYS)
    start_midnight = datetime.combine(start.date(), datetime.min.time())

    active = accounts.loc[accounts["status"] == "Active", "account_id"].to_numpy()
    all_merchants = merchants["merchant_id"].to_numpy()
    foreign_merchants = merchants.loc[merchants["country"] != "USA", "merchant_id"].to_numpy()
    if len(foreign_merchants) == 0:
        foreign_merchants = all_merchants  # fallback so anomaly slice always succeeds

    n_anom = max(1, int(round(n * 0.005)))
    n_normal = n - n_anom

    # Normal slice
    seconds_window = int((end - start).total_seconds())
    normal_secs = rng.integers(0, seconds_window, size=n_normal)
    normal_ts = pd.Timestamp(start) + pd.to_timedelta(normal_secs, unit="s")
    normal_accounts = rng.choice(active, size=n_normal)
    normal_merchants = rng.choice(all_merchants, size=n_normal)
    normal_amounts = np.round(rng.lognormal(mean=3.5, sigma=1.0, size=n_normal), 2)
    normal_directions = np.where(rng.random(size=n_normal) > 0.15, "debit", "credit")

    # Anomaly slice — off-hours (00:00-04:59), foreign, large
    anom_day_offsets = rng.integers(0, WINDOW_DAYS, size=n_anom)
    anom_seconds_in_off_hours = rng.integers(0, 5 * 3600, size=n_anom)
    anom_ts = (
        pd.Timestamp(start_midnight)
        + pd.to_timedelta(anom_day_offsets, unit="D")
        + pd.to_timedelta(anom_seconds_in_off_hours, unit="s")
    )
    anom_accounts = rng.choice(active, size=n_anom)
    anom_merchants = rng.choice(foreign_merchants, size=n_anom)
    anom_amounts = np.round(rng.uniform(5000, 20000, size=n_anom), 2)
    anom_directions = np.full(n_anom, "debit")

    df = pd.DataFrame({
        "account_id": np.concatenate([normal_accounts, anom_accounts]),
        "merchant_id": np.concatenate([normal_merchants, anom_merchants]),
        "transaction_ts": np.concatenate([normal_ts.values, anom_ts.values]),
        "amount": np.concatenate([normal_amounts, anom_amounts]),
        "direction": np.concatenate([normal_directions, anom_directions]),
    })

    df = df.sample(frac=1.0, random_state=seed).reset_index(drop=True)
    df.insert(0, "transaction_id", [f"t{i:07d}" for i in range(1, len(df) + 1)])
    return df


def write_all(seed: int = 42, out_dir: Path = SEEDS_DIR) -> dict[str, str]:
    out_dir.mkdir(parents=True, exist_ok=True)

    customers = generate_customers(seed)
    branches = generate_branches(seed)
    products = generate_products()
    accounts = generate_accounts(seed, customers, branches, products)
    merchants = generate_merchants(seed)
    transactions = generate_transactions(seed, accounts, merchants)

    paths: dict[str, str] = {}
    for name, df in [
        ("customers", customers),
        ("branches", branches),
        ("products", products),
        ("accounts", accounts),
        ("merchants", merchants),
        ("transactions", transactions),
    ]:
        p = out_dir / f"{name}.csv"
        df.to_csv(p, index=False)
        paths[name] = str(p)
        print(f"wrote {len(df):>6,} rows -> {p.relative_to(out_dir.parents[1])}")
    return paths


if __name__ == "__main__":
    write_all()