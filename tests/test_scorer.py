"""
test_scorer.py — Unit tests for the severity scoring engine.

Tests cover:
    - Financial impact score boundary values
    - Severity classification at band boundaries
    - Full scoring pipeline produces required columns
    - Executive summary dict structure
"""

import sys
from pathlib import Path
from datetime import datetime

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.scorer import (
    _financial_impact_score,
    _classify_severity,
    score_flagged_transactions,
    build_executive_summary,
)


# ---------------------------------------------------------------------------
# Financial impact score
# ---------------------------------------------------------------------------

class TestFinancialImpactScore:
    """Tests for the _financial_impact_score helper."""

    THRESHOLDS = {
        "low_threshold": 500,
        "medium_threshold": 2000,
        "high_threshold": 10000,
    }

    def test_zero_amount_returns_zero(self):
        assert _financial_impact_score(0.0, self.THRESHOLDS) == 0.0

    def test_negative_amount_returns_zero(self):
        assert _financial_impact_score(-100.0, self.THRESHOLDS) == 0.0

    def test_below_low_threshold_returns_low_score(self):
        score = _financial_impact_score(100.0, self.THRESHOLDS)
        assert 5.0 <= score < 10.0

    def test_above_high_threshold_returns_max(self):
        score = _financial_impact_score(50000.0, self.THRESHOLDS)
        assert score == 30.0

    def test_score_increases_with_amount(self):
        """Score should be monotonically non-decreasing with amount."""
        amounts = [0, 100, 500, 1000, 2000, 5000, 10000, 50000]
        scores = [_financial_impact_score(a, self.THRESHOLDS) for a in amounts]
        for i in range(1, len(scores)):
            assert scores[i] >= scores[i - 1], (
                f"Score dropped: amount {amounts[i-1]}→{amounts[i]}, "
                f"score {scores[i-1]}→{scores[i]}"
            )


# ---------------------------------------------------------------------------
# Severity classification
# ---------------------------------------------------------------------------

class TestClassifySeverity:
    """Tests for the _classify_severity helper."""

    BANDS = {"critical": 80, "high": 60, "medium": 35, "low": 0}

    def test_score_above_critical_threshold(self):
        assert _classify_severity(85.0, self.BANDS) == "Critical"

    def test_score_at_critical_threshold(self):
        assert _classify_severity(80.0, self.BANDS) == "Critical"

    def test_score_in_high_band(self):
        assert _classify_severity(70.0, self.BANDS) == "High"

    def test_score_at_high_threshold(self):
        assert _classify_severity(60.0, self.BANDS) == "High"

    def test_score_in_medium_band(self):
        assert _classify_severity(50.0, self.BANDS) == "Medium"

    def test_score_at_medium_threshold(self):
        assert _classify_severity(35.0, self.BANDS) == "Medium"

    def test_score_in_low_band(self):
        assert _classify_severity(20.0, self.BANDS) == "Low"

    def test_score_zero(self):
        assert _classify_severity(0.0, self.BANDS) == "Low"


# ---------------------------------------------------------------------------
# Full scoring pipeline
# ---------------------------------------------------------------------------

def _make_flagged_df() -> pd.DataFrame:
    """Build a minimal flagged DataFrame with all required columns."""
    return pd.DataFrame([
        {
            "transaction_id": "TXN-001",
            "date": datetime(2024, 1, 15),
            "supplier_id": "SUP001",
            "supplier_name": "Test Supplier A",
            "category": "Logistics",
            "baseline_rate": 1000.0,
            "invoice_amount": 1300.0,
            "rule_triggered": "price_variance",
            "rule_detail": "Invoice 30% over baseline",
            "leakage_amount_gbp": 150.0,
            "region": "London",
            "approved_by": "J.Harrison",
        },
        {
            "transaction_id": "TXN-002",
            "date": datetime(2024, 1, 16),
            "supplier_id": "SUP002",
            "supplier_name": "Test Supplier B",
            "category": "IT",
            "baseline_rate": 3000.0,
            "invoice_amount": 3500.0,
            "rule_triggered": "duplicate",
            "rule_detail": "Duplicate of TXN-001",
            "leakage_amount_gbp": 3500.0,
            "region": "Manchester",
            "approved_by": "S.Patel",
        },
        {
            "transaction_id": "TXN-003",
            "date": datetime(2024, 1, 17),
            "supplier_id": "SUP003",
            "supplier_name": "Test Supplier C",
            "category": "Facilities",
            "baseline_rate": 800.0,
            "invoice_amount": 800.0,
            "rule_triggered": "sla_breach",
            "rule_detail": "3 days late",
            "leakage_amount_gbp": 450.0,
            "region": "Birmingham",
            "approved_by": "M.Okonkwo",
        },
    ])


class TestScoreFlaggedTransactions:
    """Tests for the full score_flagged_transactions function."""

    def test_required_output_columns_present(self):
        """Output must contain all severity scoring columns."""
        df = _make_flagged_df()
        scored = score_flagged_transactions(df, config_path="config.yaml")
        for col in ["base_score", "financial_score", "composite_score",
                    "severity", "severity_rank", "action_required"]:
            assert col in scored.columns, f"Missing column: {col}"

    def test_severity_values_valid(self):
        """All severity values must be one of the four expected labels."""
        df = _make_flagged_df()
        scored = score_flagged_transactions(df, config_path="config.yaml")
        valid = {"Critical", "High", "Medium", "Low"}
        assert set(scored["severity"].unique()).issubset(valid)

    def test_composite_score_within_bounds(self):
        """Composite score must be in [0, 100]."""
        df = _make_flagged_df()
        scored = score_flagged_transactions(df, config_path="config.yaml")
        assert (scored["composite_score"] >= 0).all()
        assert (scored["composite_score"] <= 100).all()

    def test_sorted_by_severity_rank_desc(self):
        """Output should be sorted with highest severity first."""
        df = _make_flagged_df()
        scored = score_flagged_transactions(df, config_path="config.yaml")
        ranks = scored["severity_rank"].tolist()
        assert ranks == sorted(ranks, reverse=True)

    def test_missing_required_column_raises(self):
        """Passing a DataFrame without rule_triggered should raise ValueError."""
        df = _make_flagged_df().drop(columns=["rule_triggered"])
        with pytest.raises(ValueError, match="rule_triggered"):
            score_flagged_transactions(df, config_path="config.yaml")

    def test_row_count_unchanged(self):
        """Scoring should not add or remove rows."""
        df = _make_flagged_df()
        scored = score_flagged_transactions(df, config_path="config.yaml")
        assert len(scored) == len(df)


# ---------------------------------------------------------------------------
# Executive summary
# ---------------------------------------------------------------------------

class TestBuildExecutiveSummary:
    """Tests for build_executive_summary output structure."""

    def test_required_keys_present(self):
        """Executive summary dict must contain all required top-level keys."""
        df = _make_flagged_df()
        scored = score_flagged_transactions(df, config_path="config.yaml")
        raw_summary = {
            "total_transactions": 1000,
            "total_leakage_gbp": scored["leakage_amount_gbp"].sum(),
            "date_range_start": "2024-01-01",
            "date_range_end": "2024-03-31",
            "by_rule": {},
        }
        summary = build_executive_summary(scored, raw_summary, config_path="config.yaml")
        for key in [
            "headline_gbp", "headline_transactions", "total_flags",
            "severity_breakdown", "by_category", "by_rule",
            "top_suppliers", "date_range", "total_transactions_analysed",
        ]:
            assert key in summary, f"Missing key in executive summary: {key}"

    def test_severity_breakdown_keys(self):
        """severity_breakdown must contain all four severity labels."""
        df = _make_flagged_df()
        scored = score_flagged_transactions(df, config_path="config.yaml")
        raw_summary = {
            "total_transactions": 1000,
            "total_leakage_gbp": scored["leakage_amount_gbp"].sum(),
            "date_range_start": "2024-01-01",
            "date_range_end": "2024-03-31",
            "by_rule": {},
        }
        summary = build_executive_summary(scored, raw_summary, config_path="config.yaml")
        for sev in ["Critical", "High", "Medium", "Low"]:
            assert sev in summary["severity_breakdown"]

    def test_headline_gbp_matches_scored_total(self):
        """headline_gbp should equal the sum of leakage_amount_gbp in scored df."""
        df = _make_flagged_df()
        scored = score_flagged_transactions(df, config_path="config.yaml")
        expected_total = round(scored["leakage_amount_gbp"].sum(), 2)
        raw_summary = {
            "total_transactions": 1000,
            "total_leakage_gbp": expected_total,
            "date_range_start": "2024-01-01",
            "date_range_end": "2024-03-31",
            "by_rule": {},
        }
        summary = build_executive_summary(scored, raw_summary, config_path="config.yaml")
        assert abs(summary["headline_gbp"] - expected_total) < 0.01
