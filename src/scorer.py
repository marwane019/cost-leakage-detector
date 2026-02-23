"""
scorer.py — Severity Scoring Engine.

Transforms raw detection flags into actionable severity classifications.
Each flagged transaction receives a composite score (0–100) derived from:
    - Base score per rule type (configured in config.yaml)
    - Financial impact band (scaled by leakage_amount_gbp)
    - Optional confidence modifier (future: ML-based)

Final severity labels:
    Critical  ≥ 80  — Immediate escalation, Slack alert triggered
    High      ≥ 60  — Same-day review required
    Medium    ≥ 35  — Weekly review queue
    Low       < 35  — Monitor and log
"""

import logging
from pathlib import Path
from typing import Any

import pandas as pd
import yaml

logger = logging.getLogger(__name__)

# Severity label ordering for sort/comparison
SEVERITY_ORDER = {"Critical": 4, "High": 3, "Medium": 2, "Low": 1}


def _financial_impact_score(amount_gbp: float, thresholds: dict[str, int]) -> float:
    """Compute a 0–30 score based on financial leakage magnitude.

    Scales linearly within bands so that amounts approaching the next
    threshold boundary receive a proportionally higher score.

    Args:
        amount_gbp: Estimated leakage in GBP.
        thresholds: Dict with keys low_threshold, medium_threshold,
            high_threshold (GBP values from config).

    Returns:
        Float score in range [0, 30].
    """
    low = thresholds["low_threshold"]
    med = thresholds["medium_threshold"]
    high = thresholds["high_threshold"]

    if amount_gbp <= 0:
        return 0.0
    elif amount_gbp < low:
        return round(5 + (amount_gbp / low) * 5, 2)
    elif amount_gbp < med:
        return round(10 + ((amount_gbp - low) / (med - low)) * 10, 2)
    elif amount_gbp < high:
        return round(20 + ((amount_gbp - med) / (high - med)) * 8, 2)
    else:
        return 30.0


def _classify_severity(score: float, bands: dict[str, int]) -> str:
    """Map a composite score to a severity label.

    Args:
        score: Composite score in range [0, 100].
        bands: Dict with keys critical, high, medium, low (score thresholds).

    Returns:
        One of: 'Critical', 'High', 'Medium', 'Low'.
    """
    if score >= bands["critical"]:
        return "Critical"
    elif score >= bands["high"]:
        return "High"
    elif score >= bands["medium"]:
        return "Medium"
    else:
        return "Low"


def score_flagged_transactions(
    flagged: pd.DataFrame,
    config_path: str = "config.yaml",
) -> pd.DataFrame:
    """Apply severity scoring to all flagged transactions.

    For each row in the flagged DataFrame, computes:
        composite_score = base_rule_score + financial_impact_score
        severity        = band classification of composite_score

    Adds columns:
        base_score          — rule-specific base score (0–70)
        financial_score     — financial impact score (0–30)
        composite_score     — total score (0–100)
        severity            — 'Low' | 'Medium' | 'High' | 'Critical'
        severity_rank       — integer rank for sorting (1=Low, 4=Critical)
        action_required     — human-readable next step

    Args:
        flagged: Output DataFrame from detector.run_detection().
        config_path: Path to configuration YAML.

    Returns:
        Scored and classified DataFrame.

    Raises:
        ValueError: If flagged DataFrame is missing required columns.
    """
    required = {"rule_triggered", "leakage_amount_gbp"}
    missing = required - set(flagged.columns)
    if missing:
        raise ValueError(f"Flagged DataFrame missing columns: {missing}")

    with open(config_path, "r") as fh:
        cfg = yaml.safe_load(fh)

    scoring_cfg = cfg["scoring"]
    base_scores = scoring_cfg["rule_base_scores"]
    fin_thresholds = scoring_cfg["financial_multipliers"]
    severity_bands = scoring_cfg["severity_bands"]

    logger.info("Scoring %d flagged transactions", len(flagged))

    df = flagged.copy()

    df["base_score"] = df["rule_triggered"].map(base_scores).fillna(30.0)

    df["financial_score"] = df["leakage_amount_gbp"].apply(
        lambda x: _financial_impact_score(x, fin_thresholds)
    )

    df["composite_score"] = (df["base_score"] + df["financial_score"]).clip(upper=100)

    df["severity"] = df["composite_score"].apply(
        lambda s: _classify_severity(s, severity_bands)
    )

    df["severity_rank"] = df["severity"].map(SEVERITY_ORDER)

    action_map = {
        "Critical": "IMMEDIATE: Escalate to Finance Director. Freeze supplier payments pending review.",
        "High":     "TODAY: Assign to senior analyst for same-day investigation.",
        "Medium":   "THIS WEEK: Add to weekly ops review. Request supplier clarification.",
        "Low":      "MONITOR: Log for trend analysis. Review at end of month.",
    }
    df["action_required"] = df["severity"].map(action_map)

    # Sort: Critical first, then by leakage descending
    df = df.sort_values(
        ["severity_rank", "leakage_amount_gbp"],
        ascending=[False, False],
    ).reset_index(drop=True)

    severity_counts = df["severity"].value_counts().to_dict()
    logger.info(
        "Scoring complete — Critical: %d | High: %d | Medium: %d | Low: %d",
        severity_counts.get("Critical", 0),
        severity_counts.get("High", 0),
        severity_counts.get("Medium", 0),
        severity_counts.get("Low", 0),
    )
    return df


def build_executive_summary(
    scored: pd.DataFrame,
    raw_summary: dict[str, Any],
    config_path: str = "config.yaml",
) -> dict[str, Any]:
    """Build an executive-level summary dict for reporting and alerting.

    Aggregates scored data into headline figures suitable for a CFO dashboard
    or Slack notification. Includes leakage breakdown by rule, category,
    and severity.

    Args:
        scored: Output of score_flagged_transactions().
        raw_summary: Summary dict from detector.run_detection().
        config_path: Path to configuration YAML.

    Returns:
        Dict with keys:
            headline_gbp, headline_transactions, severity_breakdown,
            by_category, by_rule, top_suppliers, date_range
    """
    with open(config_path, "r") as fh:
        cfg = yaml.safe_load(fh)

    currency = cfg["project"]["currency"]

    critical_count = int((scored["severity"] == "Critical").sum())
    high_count = int((scored["severity"] == "High").sum())
    medium_count = int((scored["severity"] == "Medium").sum())
    low_count = int((scored["severity"] == "Low").sum())

    total_leakage = round(scored["leakage_amount_gbp"].sum(), 2)

    by_category = (
        scored.groupby("category")["leakage_amount_gbp"]
        .sum()
        .round(2)
        .sort_values(ascending=False)
        .to_dict()
    )

    by_rule = (
        scored.groupby("rule_triggered")
        .agg(
            count=("transaction_id", "count"),
            leakage_gbp=("leakage_amount_gbp", "sum"),
        )
        .round(2)
        .to_dict(orient="index")
    )

    top_suppliers = (
        scored.groupby("supplier_name")["leakage_amount_gbp"]
        .sum()
        .round(2)
        .sort_values(ascending=False)
        .head(5)
        .to_dict()
    )

    summary = {
        "headline_gbp": total_leakage,
        "headline_transactions": int(scored["transaction_id"].nunique()),
        "total_flags": len(scored),
        "severity_breakdown": {
            "Critical": critical_count,
            "High": high_count,
            "Medium": medium_count,
            "Low": low_count,
        },
        "by_category": by_category,
        "by_rule": by_rule,
        "top_suppliers": top_suppliers,
        "date_range": {
            "start": raw_summary["date_range_start"],
            "end": raw_summary["date_range_end"],
        },
        "total_transactions_analysed": raw_summary["total_transactions"],
        "currency": currency,
    }

    logger.info(
        "Executive summary built — £%.2f potential leakage | "
        "%d Critical | %d High flags",
        total_leakage,
        critical_count,
        high_count,
    )
    return summary
