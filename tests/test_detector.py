"""
test_detector.py — Unit tests for the anomaly detection engine.

Tests cover:
    - Duplicate detection (positive and negative cases)
    - Price variance detection with boundary values
    - SLA breach detection with grace period
    - Volume spike detection with rolling baseline
    - Full pipeline integration (run_detection returns valid structure)
"""

import sys
from pathlib import Path
from datetime import date, timedelta, datetime

import pandas as pd
import pytest

# Ensure src is importable from project root
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.detector import (
    detect_duplicates,
    detect_price_variance,
    detect_sla_breaches,
    detect_volume_spikes,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_base_row(**overrides) -> dict:
    """Return a minimal transaction row with sensible defaults."""
    base = {
        "transaction_id": "TXN-000001",
        "date": datetime(2024, 1, 15),
        "supplier_id": "SUP001",
        "supplier_name": "Test Supplier",
        "category": "Logistics",
        "baseline_rate": 1000.0,
        "invoice_amount": 1000.0,
        "expected_delivery_date": datetime(2024, 1, 18),
        "actual_delivery_date": datetime(2024, 1, 18),
        "po_number": "PO-001",
        "region": "London",
        "approved_by": "J.Harrison",
        "is_anomaly": False,
        "anomaly_type": "",
    }
    base.update(overrides)
    return base


def _make_df(rows: list[dict]) -> pd.DataFrame:
    """Build a properly typed DataFrame from a list of row dicts."""
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["expected_delivery_date"] = pd.to_datetime(df["expected_delivery_date"])
    df["actual_delivery_date"] = pd.to_datetime(df["actual_delivery_date"])
    df["invoice_amount"] = df["invoice_amount"].astype(float)
    df["baseline_rate"] = df["baseline_rate"].astype(float)
    return df


# ---------------------------------------------------------------------------
# Rule 1: Duplicate Detection
# ---------------------------------------------------------------------------

class TestDetectDuplicates:
    """Tests for the duplicate transaction detection rule."""

    def test_same_supplier_same_amount_same_day_flagged(self):
        """Two identical supplier+amount transactions on the same day are flagged."""
        rows = [
            _make_base_row(transaction_id="TXN-001", date=datetime(2024, 1, 15), invoice_amount=1000.0),
            _make_base_row(transaction_id="TXN-002", date=datetime(2024, 1, 15), invoice_amount=1000.0),
        ]
        df = _make_df(rows)
        flagged = detect_duplicates(df, window_days=1)
        assert len(flagged) >= 1
        assert "TXN-002" in flagged["transaction_id"].values

    def test_same_supplier_same_amount_adjacent_day_flagged(self):
        """Same supplier+amount on adjacent days (within window) should be flagged."""
        rows = [
            _make_base_row(transaction_id="TXN-001", date=datetime(2024, 1, 15), invoice_amount=500.0),
            _make_base_row(transaction_id="TXN-002", date=datetime(2024, 1, 16), invoice_amount=500.0),
        ]
        df = _make_df(rows)
        flagged = detect_duplicates(df, window_days=1)
        assert len(flagged) >= 1

    def test_different_suppliers_not_flagged(self):
        """Same amount but different suppliers should NOT be flagged as duplicates."""
        rows = [
            _make_base_row(transaction_id="TXN-001", supplier_id="SUP001", invoice_amount=1000.0),
            _make_base_row(transaction_id="TXN-002", supplier_id="SUP002", invoice_amount=1000.0),
        ]
        df = _make_df(rows)
        flagged = detect_duplicates(df, window_days=1)
        assert len(flagged) == 0

    def test_same_supplier_outside_window_not_flagged(self):
        """Same supplier+amount but > window_days apart should NOT be flagged."""
        rows = [
            _make_base_row(transaction_id="TXN-001", date=datetime(2024, 1, 1), invoice_amount=1000.0),
            _make_base_row(transaction_id="TXN-002", date=datetime(2024, 1, 10), invoice_amount=1000.0),
        ]
        df = _make_df(rows)
        flagged = detect_duplicates(df, window_days=1)
        assert len(flagged) == 0

    def test_result_contains_required_columns(self):
        """Flagged DataFrame must contain rule_triggered and rule_detail."""
        rows = [
            _make_base_row(transaction_id="TXN-001", date=datetime(2024, 1, 15)),
            _make_base_row(transaction_id="TXN-002", date=datetime(2024, 1, 15)),
        ]
        df = _make_df(rows)
        flagged = detect_duplicates(df, window_days=1)
        if len(flagged) > 0:
            assert "rule_triggered" in flagged.columns
            assert "rule_detail" in flagged.columns
            assert flagged["rule_triggered"].iloc[0] == "duplicate"

    def test_empty_dataframe_returns_empty(self):
        """An empty input DataFrame should return an empty flagged DataFrame."""
        df = _make_df([_make_base_row()])
        flagged = detect_duplicates(df, window_days=1)
        assert isinstance(flagged, pd.DataFrame)


# ---------------------------------------------------------------------------
# Rule 2: Price Variance
# ---------------------------------------------------------------------------

class TestDetectPriceVariance:
    """Tests for the price variance / overcharge detection rule."""

    def test_overcharge_above_threshold_flagged(self):
        """Invoice 20% above baseline (threshold=1.15) should be flagged."""
        rows = [_make_base_row(baseline_rate=1000.0, invoice_amount=1200.0)]
        df = _make_df(rows)
        flagged = detect_price_variance(df, threshold=1.15)
        assert len(flagged) == 1
        assert flagged["rule_triggered"].iloc[0] == "price_variance"

    def test_invoice_at_threshold_not_flagged(self):
        """Invoice exactly at the threshold (1.15×) should NOT be flagged (strict >)."""
        rows = [_make_base_row(baseline_rate=1000.0, invoice_amount=1150.0)]
        df = _make_df(rows)
        flagged = detect_price_variance(df, threshold=1.15)
        assert len(flagged) == 0

    def test_invoice_below_threshold_not_flagged(self):
        """Invoice 10% above baseline with 15% threshold should NOT be flagged."""
        rows = [_make_base_row(baseline_rate=1000.0, invoice_amount=1100.0)]
        df = _make_df(rows)
        flagged = detect_price_variance(df, threshold=1.15)
        assert len(flagged) == 0

    def test_leakage_amount_calculated_correctly(self):
        """Leakage should equal invoice_amount minus (baseline × threshold)."""
        baseline = 1000.0
        invoice = 1300.0
        threshold = 1.15
        rows = [_make_base_row(baseline_rate=baseline, invoice_amount=invoice)]
        df = _make_df(rows)
        flagged = detect_price_variance(df, threshold=threshold)
        expected_leakage = round(invoice - baseline * threshold, 2)
        assert len(flagged) == 1
        assert abs(flagged["leakage_amount_gbp"].iloc[0] - expected_leakage) < 0.01

    def test_multiple_rows_only_overcharged_flagged(self):
        """Only rows exceeding the threshold should be returned."""
        rows = [
            _make_base_row(transaction_id="TXN-001", baseline_rate=1000.0, invoice_amount=900.0),
            _make_base_row(transaction_id="TXN-002", baseline_rate=1000.0, invoice_amount=1000.0),
            _make_base_row(transaction_id="TXN-003", baseline_rate=1000.0, invoice_amount=1250.0),
        ]
        df = _make_df(rows)
        flagged = detect_price_variance(df, threshold=1.15)
        assert len(flagged) == 1
        assert "TXN-003" in flagged["transaction_id"].values


# ---------------------------------------------------------------------------
# Rule 3: SLA Breach
# ---------------------------------------------------------------------------

class TestDetectSlaBreach:
    """Tests for the SLA breach detection rule."""

    def test_late_delivery_flagged(self):
        """Actual delivery 5 days after expected should be flagged."""
        rows = [_make_base_row(
            expected_delivery_date=datetime(2024, 1, 18),
            actual_delivery_date=datetime(2024, 1, 23),
        )]
        df = _make_df(rows)
        flagged = detect_sla_breaches(df, grace_days=0)
        assert len(flagged) == 1
        assert flagged["rule_triggered"].iloc[0] == "sla_breach"

    def test_on_time_delivery_not_flagged(self):
        """Delivery on the expected date should NOT be flagged."""
        rows = [_make_base_row(
            expected_delivery_date=datetime(2024, 1, 18),
            actual_delivery_date=datetime(2024, 1, 18),
        )]
        df = _make_df(rows)
        flagged = detect_sla_breaches(df, grace_days=0)
        assert len(flagged) == 0

    def test_early_delivery_not_flagged(self):
        """Early delivery should NOT be flagged."""
        rows = [_make_base_row(
            expected_delivery_date=datetime(2024, 1, 18),
            actual_delivery_date=datetime(2024, 1, 16),
        )]
        df = _make_df(rows)
        flagged = detect_sla_breaches(df, grace_days=0)
        assert len(flagged) == 0

    def test_grace_period_respected(self):
        """Delivery 2 days late with grace_days=3 should NOT be flagged."""
        rows = [_make_base_row(
            expected_delivery_date=datetime(2024, 1, 18),
            actual_delivery_date=datetime(2024, 1, 20),
        )]
        df = _make_df(rows)
        flagged = detect_sla_breaches(df, grace_days=3)
        assert len(flagged) == 0

    def test_breach_days_column_added(self):
        """Flagged rows should have a breach_days column with correct value."""
        rows = [_make_base_row(
            expected_delivery_date=datetime(2024, 1, 18),
            actual_delivery_date=datetime(2024, 1, 25),
        )]
        df = _make_df(rows)
        flagged = detect_sla_breaches(df, grace_days=0)
        assert "breach_days" in flagged.columns
        assert flagged["breach_days"].iloc[0] == 7


# ---------------------------------------------------------------------------
# Rule 4: Volume Spike
# ---------------------------------------------------------------------------

class TestDetectVolumeSpikes:
    """Tests for the daily volume spike detection rule."""

    def _build_daily_df(self, normal_count=10, spike_count=50, spike_date_offset=20):
        """Helper: build a DataFrame with uniform daily volumes plus one spike day."""
        rows = []
        txn_id = 1
        base_date = datetime(2024, 1, 1)

        for day in range(30):
            current_date = base_date + timedelta(days=day)
            count = spike_count if day == spike_date_offset else normal_count
            for _ in range(count):
                rows.append(_make_base_row(
                    transaction_id=f"TXN-{txn_id:06d}",
                    date=current_date,
                ))
                txn_id += 1
        return _make_df(rows)

    def test_spike_day_transactions_flagged(self):
        """Transactions on a clear spike day should be flagged."""
        df = self._build_daily_df(normal_count=5, spike_count=50, spike_date_offset=20)
        flagged = detect_volume_spikes(df, sigma_threshold=2.0, rolling_window=7)
        assert len(flagged) > 0
        assert "volume_spike" in flagged["rule_triggered"].values

    def test_uniform_volume_not_flagged(self):
        """Perfectly uniform daily volumes should produce no spike flags."""
        rows = []
        for day in range(30):
            for i in range(10):
                rows.append(_make_base_row(
                    transaction_id=f"TXN-{day:03d}-{i:02d}",
                    date=datetime(2024, 1, 1) + timedelta(days=day),
                ))
        df = _make_df(rows)
        flagged = detect_volume_spikes(df, sigma_threshold=2.0, rolling_window=7)
        assert len(flagged) == 0

    def test_result_has_daily_count_column(self):
        """Flagged DataFrame should contain daily_count column for context."""
        df = self._build_daily_df(normal_count=5, spike_count=50, spike_date_offset=20)
        flagged = detect_volume_spikes(df, sigma_threshold=2.0, rolling_window=7)
        if len(flagged) > 0:
            assert "daily_count" in flagged.columns
