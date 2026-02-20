"""Unit tests for tests/cert/make_cert_datasets.py — no Docker required.

Validates that the cert dataset generator produces the correct row structure
for any dataset with a valid manifest (tested against the taxi reference dataset).
"""

from __future__ import annotations

import re
import sys
from datetime import datetime
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent.parent
sys.path.insert(0, str(REPO_ROOT))

from scripts.dataset_manifest import parse_manifest
from tests.cert.make_cert_datasets import (
    N_DUPS,
    N_VALID_BASE,
    N_VALID_CLEAN,
    _extract_date_range,
    _violation_override,
    make_clean,
    make_edge_cases,
)


@pytest.fixture(scope="module")
def taxi_manifest():
    _, m = parse_manifest("taxi")
    return m


# ---------------------------------------------------------------------------
# Clean dataset
# ---------------------------------------------------------------------------


class TestCleanDataset:
    def test_row_count(self, taxi_manifest):
        table, n = make_clean(taxi_manifest)
        assert n == N_VALID_CLEAN
        assert table.num_rows == N_VALID_CLEAN

    def test_all_columns_present(self, taxi_manifest):
        table, _ = make_clean(taxi_manifest)
        for col in taxi_manifest.columns:
            assert col.name in table.column_names, f"Missing column: {col.name}"

    def test_no_nulls_in_not_null_filter_columns(self, taxi_manifest):
        """Columns targeted by IS NOT NULL quality_filters must have zero nulls."""
        table, _ = make_clean(taxi_manifest)
        not_null_cols = []
        for f in taxi_manifest.quality_filters:
            m = re.match(r"(\w+)\s+IS\s+NOT\s+NULL", f.strip(), re.IGNORECASE)
            if m:
                not_null_cols.append(m.group(1))

        for col_name in not_null_cols:
            if col_name in table.column_names:
                arr = table.column(col_name)
                assert arr.null_count == 0, (
                    f"Column '{col_name}' should have no nulls in the clean dataset"
                )

    def test_deterministic_across_calls(self, taxi_manifest):
        """Same seed → identical tables on every call."""
        t1, _ = make_clean(taxi_manifest, n=10)
        t2, _ = make_clean(taxi_manifest, n=10)
        assert t1.equals(t2), "make_clean is not deterministic"


# ---------------------------------------------------------------------------
# Edge-case dataset
# ---------------------------------------------------------------------------


class TestEdgeCaseDataset:
    def test_row_count_breakdown(self, taxi_manifest):
        table, meta = make_edge_cases(taxi_manifest)
        n_violations = meta["n_violation_rows"]
        expected_total = N_VALID_BASE + n_violations + N_DUPS
        assert table.num_rows == expected_total
        assert meta["parquet_rows"] == expected_total
        assert meta["n_valid_base"] == N_VALID_BASE
        assert meta["n_dup_rows"] == N_DUPS

    def test_expected_silver_equals_valid_base(self, taxi_manifest):
        _table, meta = make_edge_cases(taxi_manifest)
        # After quality filter + dedup: only the N_VALID_BASE unique valid rows survive
        assert meta["expected_silver"] == N_VALID_BASE

    def test_expected_dropped_equals_violation_count(self, taxi_manifest):
        _table, meta = make_edge_cases(taxi_manifest)
        assert meta["expected_dropped"] == meta["n_violation_rows"]

    def test_expected_deduped_equals_dup_count(self, taxi_manifest):
        _table, meta = make_edge_cases(taxi_manifest)
        assert meta["expected_deduped"] == N_DUPS

    def test_taxi_has_seven_quality_filter_violations(self, taxi_manifest):
        """Taxi manifest has 7 quality_filters — expect 7 violation rows."""
        _table, meta = make_edge_cases(taxi_manifest)
        assert meta["n_violation_rows"] == 7, (
            f"Expected 7 violation rows for taxi (one per quality_filter), "
            f"got {meta['n_violation_rows']}"
        )

    def test_violation_rows_have_correct_null_positions(self, taxi_manifest):
        """
        Rows in the violation group should have None in the column targeted
        by the IS NOT NULL filter.
        """
        table, meta = make_edge_cases(taxi_manifest)
        n_valid = N_VALID_BASE
        n_violations = meta["n_violation_rows"]

        # Find IS NOT NULL filters and their columns
        for filt in taxi_manifest.quality_filters:
            m = re.match(r"(\w+)\s+IS\s+NOT\s+NULL", filt.strip(), re.IGNORECASE)
            if not m:
                continue
            col_name = m.group(1)
            if col_name not in table.column_names:
                continue

            # At least one row in the violation group should be null in this column
            arr = table.column(col_name)
            violation_slice = arr.slice(n_valid, n_violations)
            assert violation_slice.null_count > 0, (
                f"Expected at least one null in '{col_name}' among violation rows "
                f"(filter: {filt!r})"
            )

    def test_duplicate_rows_match_dedup_key_of_originals(self, taxi_manifest):
        """Group C rows must have identical dedup_key fields as the first N_DUPS valid rows."""
        table, meta = make_edge_cases(taxi_manifest)
        n_valid = N_VALID_BASE
        n_violations = meta["n_violation_rows"]
        dup_start = n_valid + n_violations

        for key_col in taxi_manifest.dedup_key:
            if key_col not in table.column_names:
                continue
            arr = table.column(key_col)
            for i in range(N_DUPS):
                orig_val = arr[i].as_py()
                dup_val = arr[dup_start + i].as_py()
                assert orig_val == dup_val, (
                    f"Dedup key mismatch at row {i}: "
                    f"original[{key_col}]={orig_val!r} != duplicate[{key_col}]={dup_val!r}"
                )

    def test_deterministic_across_calls(self, taxi_manifest):
        t1, m1 = make_edge_cases(taxi_manifest)
        t2, m2 = make_edge_cases(taxi_manifest)
        assert t1.equals(t2)
        assert m1 == m2


# ---------------------------------------------------------------------------
# Date range extraction
# ---------------------------------------------------------------------------


class TestDateRangeExtraction:
    def test_taxi_date_range(self, taxi_manifest):
        valid_from, valid_before = _extract_date_range(taxi_manifest.quality_filters)
        assert valid_from == datetime(2024, 1, 1)
        assert valid_before == datetime(2024, 2, 1)

    def test_fallback_when_no_date_filters(self):
        valid_from, valid_before = _extract_date_range(["total_amount > 0"])
        # Falls back to Jan 2024
        assert valid_from < valid_before

    def test_custom_date_range(self):
        filters = [
            "CAST(event_ts AS DATE) >= DATE '2023-06-01'",
            "CAST(event_ts AS DATE) < DATE '2023-07-01'",
        ]
        valid_from, valid_before = _extract_date_range(filters)
        assert valid_from == datetime(2023, 6, 1)
        assert valid_before == datetime(2023, 7, 1)


# ---------------------------------------------------------------------------
# Violation generation
# ---------------------------------------------------------------------------


class TestViolationGeneration:
    def _col_map(self, taxi_manifest):
        return {c.name: c for c in taxi_manifest.columns}

    def test_is_not_null_violation(self, taxi_manifest):
        result = _violation_override("tpep_pickup_datetime IS NOT NULL", self._col_map(taxi_manifest))
        assert result == {"tpep_pickup_datetime": None}

    def test_gt_zero_violation(self, taxi_manifest):
        result = _violation_override("total_amount > 0", self._col_map(taxi_manifest))
        assert result is not None
        assert result.get("total_amount") == 0.0

    def test_gte_zero_violation(self, taxi_manifest):
        result = _violation_override("trip_distance >= 0", self._col_map(taxi_manifest))
        assert result is not None
        assert result.get("trip_distance") == -1.0

    def test_cast_date_gte_violation(self, taxi_manifest):
        result = _violation_override(
            "CAST(tpep_pickup_datetime AS DATE) >= DATE '2024-01-01'",
            self._col_map(taxi_manifest),
        )
        assert result is not None
        dt = result.get("tpep_pickup_datetime")
        assert dt is not None
        assert dt < datetime(2024, 1, 1), (
            f"Expected timestamp before 2024-01-01, got {dt}"
        )

    def test_cast_date_lt_violation(self, taxi_manifest):
        result = _violation_override(
            "CAST(tpep_pickup_datetime AS DATE) <  DATE '2024-02-01'",
            self._col_map(taxi_manifest),
        )
        assert result is not None
        dt = result.get("tpep_pickup_datetime")
        assert dt is not None
        assert dt >= datetime(2024, 2, 1), (
            f"Expected timestamp on or after 2024-02-01, got {dt}"
        )

    def test_unknown_filter_returns_none(self, taxi_manifest):
        result = _violation_override(
            "SOME_FUNCTION(x, y) BETWEEN 1 AND 10",
            self._col_map(taxi_manifest),
        )
        assert result is None

    def test_unknown_column_returns_none(self, taxi_manifest):
        # Filter references a column not in manifest → should return None
        result = _violation_override("nonexistent_col IS NOT NULL", self._col_map(taxi_manifest))
        assert result is None

    def test_all_taxi_filters_produce_violations(self, taxi_manifest):
        """Every taxi quality_filter should produce a recognizable violation override."""
        col_map = self._col_map(taxi_manifest)
        skipped = []
        for filt in taxi_manifest.quality_filters:
            result = _violation_override(filt, col_map)
            if result is None:
                skipped.append(filt)
        assert not skipped, (
            f"These quality_filters produced no violation override (add a new pattern):\n"
            + "\n".join(f"  {f!r}" for f in skipped)
        )
