"""
data_generator.py — Synthetic Procurement Transaction Dataset Generator.

Generates a realistic 90-day operations procurement dataset with controlled
anomaly injection. Anomalies are placed at known indices so the detection
engine can be validated deterministically.

Outputs:
    data/raw/transactions.csv   — primary dataset
"""

import logging
import os
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)


def load_config(config_path: str = "config.yaml") -> dict[str, Any]:
    """Load YAML configuration file.

    Args:
        config_path: Path to config.yaml relative to project root.

    Returns:
        Parsed configuration dictionary.

    Raises:
        FileNotFoundError: If config file does not exist.
        yaml.YAMLError: If config file is malformed.
    """
    config_path = Path(config_path)
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    with open(config_path, "r") as fh:
        config = yaml.safe_load(fh)
    logger.debug("Configuration loaded from %s", config_path)
    return config


def _build_transaction_id(index: int) -> str:
    """Generate a zero-padded transaction reference.

    Args:
        index: Sequential transaction index.

    Returns:
        Formatted transaction ID string, e.g. 'TXN-000042'.
    """
    return f"TXN-{index:06d}"


def _generate_base_transactions(
    cfg: dict[str, Any],
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Generate the baseline transaction corpus without anomalies.

    Creates realistic daily transaction volumes using a normal distribution
    around the configured mean, with per-supplier price variation drawn from
    a truncated normal distribution around each supplier's baseline rate.

    Args:
        cfg: Full configuration dictionary.
        rng: Seeded NumPy random generator for reproducibility.

    Returns:
        DataFrame with columns:
            transaction_id, date, supplier_id, supplier_name, category,
            baseline_rate, invoice_amount, expected_delivery_date,
            actual_delivery_date, po_number, region, approved_by
    """
    gen_cfg = cfg["data_generation"]
    days = gen_cfg["days_history"]
    mean_txn = gen_cfg["transactions_per_day_mean"]
    std_txn = gen_cfg["transactions_per_day_std"]
    suppliers = gen_cfg["suppliers"]
    sla_map = gen_cfg["sla_days_map"]
    seed = gen_cfg["seed"]

    random.seed(seed)
    start_date = datetime.today() - timedelta(days=days)

    regions = ["London", "Manchester", "Birmingham", "Leeds", "Bristol", "Edinburgh"]
    approvers = ["J.Harrison", "S.Patel", "M.Okonkwo", "L.Chen", "R.Fitzgerald"]
    po_counter = 10000

    records = []
    txn_index = 1

    for day_offset in range(days):
        current_date = start_date + timedelta(days=day_offset)
        # Weekday weighting — fewer transactions on weekends
        if current_date.weekday() >= 5:
            day_mean = max(5, mean_txn * 0.3)
        else:
            day_mean = mean_txn

        n_transactions = max(1, int(rng.normal(day_mean, std_txn)))

        for _ in range(n_transactions):
            supplier = random.choice(suppliers)
            sla_days = sla_map.get(supplier["category"], 3)
            # Invoice amount: ±8% natural price variation around baseline
            invoice_amount = round(
                float(rng.normal(supplier["baseline_rate"], supplier["baseline_rate"] * 0.08)),
                2,
            )
            invoice_amount = max(10.0, invoice_amount)  # floor at £10

            expected_delivery = current_date + timedelta(days=sla_days)
            # Normal delivery: within SLA ±1 day
            delivery_offset = int(rng.integers(-1, 2))
            actual_delivery = expected_delivery + timedelta(days=delivery_offset)

            po_counter += 1
            records.append(
                {
                    "transaction_id": _build_transaction_id(txn_index),
                    "date": current_date.strftime("%Y-%m-%d"),
                    "supplier_id": supplier["id"],
                    "supplier_name": supplier["name"],
                    "category": supplier["category"],
                    "baseline_rate": supplier["baseline_rate"],
                    "invoice_amount": invoice_amount,
                    "expected_delivery_date": expected_delivery.strftime("%Y-%m-%d"),
                    "actual_delivery_date": actual_delivery.strftime("%Y-%m-%d"),
                    "po_number": f"PO-{po_counter}",
                    "region": random.choice(regions),
                    "approved_by": random.choice(approvers),
                    "is_anomaly": False,
                    "anomaly_type": "",
                }
            )
            txn_index += 1

    df = pd.DataFrame(records)
    logger.info(
        "Generated %d base transactions across %d days", len(df), days
    )
    return df


def _inject_duplicates(
    df: pd.DataFrame,
    rate: float,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Inject duplicate transactions by cloning existing rows ±1 day.

    A duplicate is defined as the same supplier + same invoice amount
    appearing within a 1-day window of the original. This simulates
    double-billing by vendors or AP processing errors.

    Args:
        df: Base transaction DataFrame.
        rate: Proportion of rows to duplicate (e.g. 0.025 = 2.5%).
        rng: Seeded NumPy random generator.

    Returns:
        DataFrame with duplicate rows appended and flagged.
    """
    n_dupes = max(1, int(len(df) * rate))
    source_indices = rng.choice(df.index, size=n_dupes, replace=False)
    dupes = df.loc[source_indices].copy()

    for idx in dupes.index:
        offset = int(rng.choice([-1, 1]))
        original_date = datetime.strptime(dupes.at[idx, "date"], "%Y-%m-%d")
        dupes.at[idx, "date"] = (original_date + timedelta(days=offset)).strftime(
            "%Y-%m-%d"
        )
        dupes.at[idx, "transaction_id"] = f"TXN-DUP-{idx:06d}"
        dupes.at[idx, "po_number"] = f"PO-DUP-{idx}"
        dupes.at[idx, "is_anomaly"] = True
        dupes.at[idx, "anomaly_type"] = "duplicate"

    result = pd.concat([df, dupes], ignore_index=True)
    logger.info("Injected %d duplicate transactions", n_dupes)
    return result


def _inject_price_overcharges(
    df: pd.DataFrame,
    rate: float,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Inflate invoice amounts on selected rows to simulate overcharging.

    Overcharges are set to 20–45% above baseline to ensure they exceed
    the configured price_variance_threshold of 15%.

    Args:
        df: Transaction DataFrame.
        rate: Proportion of rows to inflate.
        rng: Seeded NumPy random generator.

    Returns:
        DataFrame with invoice_amount inflated on selected rows.
    """
    n_overcharge = max(1, int(len(df) * rate))
    indices = rng.choice(df.index, size=n_overcharge, replace=False)

    for idx in indices:
        multiplier = float(rng.uniform(1.20, 1.45))
        df.at[idx, "invoice_amount"] = round(
            df.at[idx, "baseline_rate"] * multiplier, 2
        )
        df.at[idx, "is_anomaly"] = True
        df.at[idx, "anomaly_type"] = (
            "price_variance"
            if df.at[idx, "anomaly_type"] == ""
            else df.at[idx, "anomaly_type"] + "|price_variance"
        )

    logger.info("Injected %d price overcharge transactions", n_overcharge)
    return df


def _inject_sla_breaches(
    df: pd.DataFrame,
    rate: float,
    rng: np.random.Generator,
) -> pd.DataFrame:
    """Push actual_delivery_date beyond the SLA window on selected rows.

    Breach delays are drawn from 3–15 days beyond the expected date,
    simulating vendor reliability failures.

    Args:
        df: Transaction DataFrame.
        rate: Proportion of rows to breach.
        rng: Seeded NumPy random generator.

    Returns:
        DataFrame with actual_delivery_date delayed on selected rows.
    """
    n_breach = max(1, int(len(df) * rate))
    indices = rng.choice(df.index, size=n_breach, replace=False)

    for idx in indices:
        expected = datetime.strptime(df.at[idx, "expected_delivery_date"], "%Y-%m-%d")
        extra_days = int(rng.integers(3, 16))
        df.at[idx, "actual_delivery_date"] = (
            expected + timedelta(days=extra_days)
        ).strftime("%Y-%m-%d")
        df.at[idx, "is_anomaly"] = True
        df.at[idx, "anomaly_type"] = (
            "sla_breach"
            if df.at[idx, "anomaly_type"] == ""
            else df.at[idx, "anomaly_type"] + "|sla_breach"
        )

    logger.info("Injected %d SLA breach transactions", n_breach)
    return df


def _inject_volume_spikes(
    df: pd.DataFrame,
    spike_days: int,
    rng: np.random.Generator,
    cfg: dict[str, Any],
) -> pd.DataFrame:
    """Add burst transactions on selected dates to simulate volume anomalies.

    On spike days, extra transactions are injected (3–5× the daily mean)
    to create outlier counts that exceed the rolling-mean + 2σ threshold.

    Args:
        df: Transaction DataFrame.
        spike_days: Number of distinct days to spike.
        rng: Seeded NumPy random generator.
        cfg: Full configuration dictionary.

    Returns:
        DataFrame with additional spike-day transactions appended.
    """
    gen_cfg = cfg["data_generation"]
    suppliers = gen_cfg["suppliers"]
    sla_map = gen_cfg["sla_days_map"]
    mean_txn = gen_cfg["transactions_per_day_mean"]
    regions = ["London", "Manchester", "Birmingham", "Leeds", "Bristol", "Edinburgh"]
    approvers = ["J.Harrison", "S.Patel", "M.Okonkwo", "L.Chen", "R.Fitzgerald"]

    unique_dates = df["date"].unique()
    # Avoid the first and last 7 days so the rolling mean has context
    spike_date_pool = unique_dates[7:-7]
    chosen_dates = rng.choice(spike_date_pool, size=min(spike_days, len(spike_date_pool)), replace=False)

    extra_records = []
    spike_txn_index = 900000

    for spike_date in chosen_dates:
        n_extra = int(rng.integers(int(mean_txn * 3), int(mean_txn * 5)))
        for _ in range(n_extra):
            supplier = suppliers[int(rng.integers(0, len(suppliers)))]
            sla_days = sla_map.get(supplier["category"], 3)
            invoice_amount = round(
                float(rng.normal(supplier["baseline_rate"], supplier["baseline_rate"] * 0.08)),
                2,
            )
            expected_delivery = datetime.strptime(spike_date, "%Y-%m-%d") + timedelta(days=sla_days)
            actual_delivery = expected_delivery + timedelta(days=int(rng.integers(0, 2)))

            spike_txn_index += 1
            extra_records.append(
                {
                    "transaction_id": f"TXN-SPIKE-{spike_txn_index:06d}",
                    "date": spike_date,
                    "supplier_id": supplier["id"],
                    "supplier_name": supplier["name"],
                    "category": supplier["category"],
                    "baseline_rate": supplier["baseline_rate"],
                    "invoice_amount": max(10.0, invoice_amount),
                    "expected_delivery_date": expected_delivery.strftime("%Y-%m-%d"),
                    "actual_delivery_date": actual_delivery.strftime("%Y-%m-%d"),
                    "po_number": f"PO-SPIKE-{spike_txn_index}",
                    "region": str(rng.choice(regions)),
                    "approved_by": str(rng.choice(approvers)),
                    "is_anomaly": True,
                    "anomaly_type": "volume_spike",
                }
            )

    result = pd.concat([df, pd.DataFrame(extra_records)], ignore_index=True)
    logger.info(
        "Injected volume spikes on %d days (%d extra transactions)",
        len(chosen_dates),
        len(extra_records),
    )
    return result


def generate_dataset(config_path: str = "config.yaml") -> pd.DataFrame:
    """Orchestrate full synthetic dataset generation.

    Runs base generation followed by all anomaly injection steps in sequence.
    The resulting CSV is written to the path specified in config.yaml.

    Args:
        config_path: Path to configuration YAML file.

    Returns:
        Complete transaction DataFrame including injected anomalies.

    Raises:
        OSError: If the output directory cannot be created or written to.
    """
    cfg = load_config(config_path)
    seed = cfg["data_generation"]["seed"]
    rng = np.random.default_rng(seed)

    logger.info("Starting dataset generation (seed=%d)", seed)

    df = _generate_base_transactions(cfg, rng)

    anomaly_rates = cfg["data_generation"]["anomaly_rates"]
    df = _inject_duplicates(df, anomaly_rates["duplicate_rate"], rng)
    df = _inject_price_overcharges(df, anomaly_rates["price_overcharge_rate"], rng)
    df = _inject_sla_breaches(df, anomaly_rates["sla_breach_rate"], rng)
    df = _inject_volume_spikes(df, anomaly_rates["volume_spike_days"], rng, cfg)

    # Sort by date then transaction ID for a clean presentation
    df = df.sort_values(["date", "transaction_id"]).reset_index(drop=True)

    # Write CSV
    output_path = Path(cfg["paths"]["raw_data"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)

    total_anomalies = df["is_anomaly"].sum()
    total_value = df["invoice_amount"].sum()
    logger.info(
        "Dataset written to %s — %d rows | %d anomalies | £%.2f total value",
        output_path,
        len(df),
        total_anomalies,
        total_value,
    )
    return df
