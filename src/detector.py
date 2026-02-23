"""
detector.py — Multi-Rule Anomaly Detection Engine.

Applies four independent detection rules to a transaction DataFrame and
returns every flagged row annotated with the triggering rule and detail.
Rules are intentionally independent so multiple rules can fire on the
same transaction (e.g. a volume-spike day that also contains duplicates).

Detection Rules:
    1. Duplicate Transactions  — same supplier + amount within N days
    2. Price Variance          — invoice > baseline × threshold
    3. SLA Breach              — actual delivery > expected + grace days
    4. Volume Spike            — daily count > rolling mean + N×σ
"""

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)


def load_transactions(csv_path: str) -> pd.DataFrame:
    """Load and validate the transactions CSV from disk.

    Performs type coercion on date columns and numeric fields to ensure
    downstream detection logic operates on correct Python types.

    Args:
        csv_path: Absolute or relative path to transactions.csv.

    Returns:
        Typed transaction DataFrame.

    Raises:
        FileNotFoundError: If the CSV does not exist.
        ValueError: If required columns are missing.
    """
    path = Path(csv_path)
    if not path.exists():
        raise FileNotFoundError(
            f"Transaction data not found at {path}. "
            "Run with --generate-data first."
        )

    df = pd.read_csv(path)

    required_columns = {
        "transaction_id", "date", "supplier_id", "supplier_name",
        "category", "baseline_rate", "invoice_amount",
        "expected_delivery_date", "actual_delivery_date",
    }
    missing = required_columns - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Type coercion
    df["date"] = pd.to_datetime(df["date"])
    df["expected_delivery_date"] = pd.to_datetime(df["expected_delivery_date"])
    df["actual_delivery_date"] = pd.to_datetime(df["actual_delivery_date"])
    df["invoice_amount"] = pd.to_numeric(df["invoice_amount"], errors="coerce")
    df["baseline_rate"] = pd.to_numeric(df["baseline_rate"], errors="coerce")

    logger.info(
        "Loaded %d transactions from %s (date range: %s to %s)",
        len(df),
        path,
        df["date"].min().date(),
        df["date"].max().date(),
    )
    return df


# ---------------------------------------------------------------------------
# Rule 1: Duplicate Detection
# ---------------------------------------------------------------------------

def detect_duplicates(
    df: pd.DataFrame,
    window_days: int = 1,
) -> pd.DataFrame:
    """Flag transactions that share supplier + invoice_amount within a date window.

    Two transactions are considered duplicates when they share the same
    supplier_id and invoice_amount (rounded to the nearest pound) and their
    dates are within `window_days` of each other.

    The algorithm uses a self-join on supplier_id + rounded_amount, then
    filters by the absolute day difference. The first occurrence (earliest
    date) is treated as legitimate; later occurrences are flagged.

    Args:
        df: Full transaction DataFrame.
        window_days: Maximum number of days between a pair to classify as
            duplicate. Defaults to 1 (same or adjacent day).

    Returns:
        DataFrame of flagged rows with added columns:
            rule_triggered, rule_detail, leakage_amount_gbp
    """
    logger.info("Running Rule 1: Duplicate Detection (window=%d days)", window_days)

    # Round to nearest pound to absorb floating-point noise
    df = df.copy()
    df["_amount_key"] = df["invoice_amount"].round(0)

    # Sort so that, for any duplicate pair, the earlier date comes first
    df_sorted = df.sort_values("date").reset_index(drop=True)

    # Group by supplier + rounded amount; within each group find rows
    # where another row exists within window_days
    flagged_ids = set()

    grouped = df_sorted.groupby(["supplier_id", "_amount_key"])
    for _, group in grouped:
        if len(group) < 2:
            continue
        dates = group["date"].tolist()
        txn_ids = group["transaction_id"].tolist()
        # Check every pair in the group
        for i in range(len(dates)):
            for j in range(i + 1, len(dates)):
                delta = abs((dates[j] - dates[i]).days)
                if delta <= window_days:
                    # Flag the later occurrence (index j)
                    flagged_ids.add(txn_ids[j])

    mask = df["transaction_id"].isin(flagged_ids)
    flagged = df[mask].copy()
    flagged["rule_triggered"] = "duplicate"
    flagged["rule_detail"] = flagged.apply(
        lambda r: (
            f"Duplicate of supplier {r['supplier_id']} "
            f"invoice £{r['invoice_amount']:,.2f} within {window_days}d window"
        ),
        axis=1,
    )
    flagged["leakage_amount_gbp"] = flagged["invoice_amount"]

    df.drop(columns=["_amount_key"], inplace=True)

    logger.info("Rule 1 flagged %d duplicate transactions", len(flagged))
    return flagged.drop(columns=["_amount_key"], errors="ignore")


# ---------------------------------------------------------------------------
# Rule 2: Price Variance
# ---------------------------------------------------------------------------

def detect_price_variance(
    df: pd.DataFrame,
    threshold: float = 1.15,
) -> pd.DataFrame:
    """Flag invoices where the charged amount exceeds baseline by a threshold.

    Compares each invoice_amount against the supplier's configured
    baseline_rate. Transactions where the ratio exceeds `threshold`
    (e.g. 1.15 = 15% over baseline) are flagged as potential overcharges.

    Args:
        df: Full transaction DataFrame.
        threshold: Multiplier above baseline to trigger flag (e.g. 1.15).

    Returns:
        DataFrame of flagged rows with leakage estimated as the excess above
        the threshold-adjusted baseline.
    """
    logger.info(
        "Running Rule 2: Price Variance Detection (threshold=%.0f%% over baseline)",
        (threshold - 1) * 100,
    )

    df = df.copy()
    df["_price_ratio"] = df["invoice_amount"] / df["baseline_rate"]
    mask = df["_price_ratio"] > threshold

    flagged = df[mask].copy()
    pct_over = ((flagged["_price_ratio"] - 1) * 100).round(1)
    flagged["rule_triggered"] = "price_variance"
    flagged["rule_detail"] = flagged.apply(
        lambda r: (
            f"Invoice £{r['invoice_amount']:,.2f} is "
            f"{((r['_price_ratio'] - 1) * 100):.1f}% above "
            f"baseline £{r['baseline_rate']:,.2f} "
            f"(threshold: {(threshold - 1) * 100:.0f}%)"
        ),
        axis=1,
    )
    # Leakage = amount charged above the threshold-adjusted baseline
    flagged["leakage_amount_gbp"] = (
        flagged["invoice_amount"] - flagged["baseline_rate"] * threshold
    ).round(2)

    flagged.drop(columns=["_price_ratio"], inplace=True)

    logger.info(
        "Rule 2 flagged %d overcharge transactions | "
        "estimated leakage £%.2f",
        len(flagged),
        flagged["leakage_amount_gbp"].sum(),
    )
    return flagged


# ---------------------------------------------------------------------------
# Rule 3: SLA Breach
# ---------------------------------------------------------------------------

def detect_sla_breaches(
    df: pd.DataFrame,
    grace_days: int = 0,
) -> pd.DataFrame:
    """Flag transactions where actual delivery exceeded the expected SLA date.

    SLA breach is defined as:
        actual_delivery_date > expected_delivery_date + grace_days

    Financial leakage is estimated using a £150/day penalty rate
    (configurable via the scoring layer).

    Args:
        df: Full transaction DataFrame.
        grace_days: Additional days of tolerance before flagging. Defaults to 0.

    Returns:
        DataFrame of flagged rows with breach_days and leakage_amount_gbp.
    """
    PENALTY_PER_DAY_GBP = 150.0  # Internal ops cost rate for late delivery

    logger.info(
        "Running Rule 3: SLA Breach Detection (grace=%d days)", grace_days
    )

    df = df.copy()
    df["_breach_days"] = (
        df["actual_delivery_date"] - df["expected_delivery_date"]
    ).dt.days - grace_days

    mask = df["_breach_days"] > 0
    flagged = df[mask].copy()

    flagged["rule_triggered"] = "sla_breach"
    if flagged.empty:
        flagged["rule_detail"] = pd.Series(dtype=str)
        flagged["leakage_amount_gbp"] = pd.Series(dtype=float)
        flagged.rename(columns={"_breach_days": "breach_days"}, inplace=True)
    else:
        flagged["rule_detail"] = flagged.apply(
            lambda r: (
                f"Delivery {r['_breach_days']:.0f} days late: "
                f"expected {r['expected_delivery_date'].date()}, "
                f"actual {r['actual_delivery_date'].date()}"
            ),
            axis=1,
        )
        flagged["leakage_amount_gbp"] = (
            flagged["_breach_days"] * PENALTY_PER_DAY_GBP
        ).round(2)
        flagged.rename(columns={"_breach_days": "breach_days"}, inplace=True)

    logger.info(
        "Rule 3 flagged %d SLA breach transactions | "
        "max breach %d days | estimated leakage £%.2f",
        len(flagged),
        int(flagged["breach_days"].max()) if len(flagged) > 0 else 0,
        flagged["leakage_amount_gbp"].sum(),
    )
    return flagged


# ---------------------------------------------------------------------------
# Rule 4: Volume Spike
# ---------------------------------------------------------------------------

def detect_volume_spikes(
    df: pd.DataFrame,
    sigma_threshold: float = 2.0,
    rolling_window: int = 14,
) -> pd.DataFrame:
    """Flag all transactions on days where daily volume exceeds a rolling baseline.

    Computes a rolling 14-day mean and standard deviation of daily transaction
    counts. Days where the count exceeds mean + N×σ are classified as spikes.
    All transactions on spike days are flagged for review.

    This rule catches bulk-ordering fraud, unauthorised procurement surges,
    and system processing errors that generate phantom transactions.

    Args:
        df: Full transaction DataFrame.
        sigma_threshold: Number of standard deviations above the rolling mean
            to trigger a spike flag. Defaults to 2.0.
        rolling_window: Look-back window (days) for computing the baseline.
            Defaults to 14.

    Returns:
        DataFrame of flagged rows with spike_count, rolling_mean, rolling_std
        columns added.
    """
    logger.info(
        "Running Rule 4: Volume Spike Detection (sigma=%.1f, window=%d days)",
        sigma_threshold,
        rolling_window,
    )

    daily_counts = (
        df.groupby("date")
        .size()
        .rename("daily_count")
        .reset_index()
        .sort_values("date")
    )

    daily_counts["rolling_mean"] = (
        daily_counts["daily_count"]
        .shift(1)
        .rolling(window=rolling_window, min_periods=3)
        .mean()
    )
    daily_counts["rolling_std"] = (
        daily_counts["daily_count"]
        .shift(1)
        .rolling(window=rolling_window, min_periods=3)
        .std()
    )
    daily_counts["upper_bound"] = (
        daily_counts["rolling_mean"] + sigma_threshold * daily_counts["rolling_std"]
    )
    spike_days = daily_counts[
        daily_counts["daily_count"] > daily_counts["upper_bound"]
    ]["date"]

    flagged = df[df["date"].isin(spike_days)].copy()

    # Merge spike stats back for context
    flagged = flagged.merge(
        daily_counts[["date", "daily_count", "rolling_mean", "rolling_std"]],
        on="date",
        how="left",
    )

    flagged["rule_triggered"] = "volume_spike"
    if flagged.empty:
        flagged["rule_detail"] = pd.Series(dtype=str)
    else:
        flagged["rule_detail"] = flagged.apply(
            lambda r: (
                f"Date {r['date'].date()}: {r['daily_count']:.0f} transactions "
                f"(baseline mean={r['rolling_mean']:.1f}, "
                f"std={r['rolling_std']:.1f}, "
                f"threshold={r['rolling_mean'] + sigma_threshold * r['rolling_std']:.1f})"
            ),
            axis=1,
        )
    flagged["leakage_amount_gbp"] = 0.0  # Flagged for review; no direct £ leakage

    logger.info(
        "Rule 4 flagged %d transactions across %d spike days",
        len(flagged),
        spike_days.nunique(),
    )
    return flagged


# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

def run_detection(config_path: str = "config.yaml") -> tuple[pd.DataFrame, dict[str, Any]]:
    """Run the full detection pipeline and return annotated results.

    Loads configuration, reads the transaction CSV, applies all four
    detection rules, concatenates results, and deduplicates on
    transaction_id + rule_triggered so each (transaction, rule) pair
    appears exactly once.

    Args:
        config_path: Path to configuration YAML.

    Returns:
        Tuple of:
            flagged_df  — DataFrame of all flagged (transaction, rule) pairs
            summary     — Dict with aggregate stats for downstream reporting
    """
    with open(config_path, "r") as fh:
        cfg = yaml.safe_load(fh)

    det_cfg = cfg["detection"]
    df = load_transactions(cfg["paths"]["raw_data"])

    results = []

    results.append(
        detect_duplicates(df, window_days=det_cfg["duplicate_window_days"])
    )
    results.append(
        detect_price_variance(df, threshold=det_cfg["price_variance_threshold"])
    )
    results.append(
        detect_sla_breaches(df, grace_days=det_cfg["sla_grace_days"])
    )
    results.append(
        detect_volume_spikes(
            df,
            sigma_threshold=det_cfg["volume_spike_sigma"],
            rolling_window=det_cfg["volume_rolling_window"],
        )
    )

    flagged = pd.concat(results, ignore_index=True)

    # Ensure each (transaction, rule) pair is unique
    flagged = flagged.drop_duplicates(
        subset=["transaction_id", "rule_triggered"]
    ).reset_index(drop=True)

    summary = {
        "total_transactions": len(df),
        "total_flagged": len(flagged),
        "total_leakage_gbp": round(flagged["leakage_amount_gbp"].sum(), 2),
        "by_rule": flagged.groupby("rule_triggered")["leakage_amount_gbp"]
        .agg(count="count", leakage_gbp="sum")
        .round(2)
        .to_dict(orient="index"),
        "date_range_start": str(df["date"].min().date()),
        "date_range_end": str(df["date"].max().date()),
    }

    logger.info(
        "Detection complete — %d flags across %d transactions | "
        "total estimated leakage £%.2f",
        len(flagged),
        flagged["transaction_id"].nunique(),
        summary["total_leakage_gbp"],
    )
    return flagged, summary
