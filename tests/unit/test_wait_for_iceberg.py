"""Unit tests for ALLOW_EMPTY timeout gating logic."""

from scripts.wait_for_iceberg import _allow_empty_bypass_reason


def test_allow_empty_disabled_never_bypasses():
    bypass, reason = _allow_empty_bypass_reason(
        allow_empty=False,
        successful_queries=0,
        saw_zero_row_success=False,
        saw_non_zero_row_success=False,
        timeout_seconds=30,
        min_rows=1,
    )
    assert bypass is False
    assert reason is None


def test_allow_empty_does_not_bypass_when_table_never_queryable():
    bypass, reason = _allow_empty_bypass_reason(
        allow_empty=True,
        successful_queries=0,
        saw_zero_row_success=False,
        saw_non_zero_row_success=False,
        timeout_seconds=30,
        min_rows=1,
    )
    assert bypass is False
    assert reason is not None
    assert "never queryable" in reason


def test_allow_empty_bypasses_for_stable_zero_row_reads():
    bypass, reason = _allow_empty_bypass_reason(
        allow_empty=True,
        successful_queries=3,
        saw_zero_row_success=True,
        saw_non_zero_row_success=False,
        timeout_seconds=30,
        min_rows=1,
    )
    assert bypass is True
    assert reason is not None
    assert "bypassing readiness gate" in reason


def test_allow_empty_does_not_bypass_if_non_zero_rows_observed():
    bypass, reason = _allow_empty_bypass_reason(
        allow_empty=True,
        successful_queries=3,
        saw_zero_row_success=True,
        saw_non_zero_row_success=True,
        timeout_seconds=30,
        min_rows=5,
    )
    assert bypass is False
    assert reason is not None
    assert "observed non-zero rows below threshold 5" in reason
