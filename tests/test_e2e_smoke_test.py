from pathlib import Path

import pytest

from src.e2e_smoke_test import (
    EXPECTED_METRICS,
    assert_idempotent,
    validate_metrics,
    validate_output_root,
)


def test_validate_metrics_accepts_expected_values():
    validate_metrics(EXPECTED_METRICS.copy())


def test_validate_metrics_rejects_different_count():
    metrics = EXPECTED_METRICS.copy()
    metrics["silver_records"] = 99

    with pytest.raises(
        AssertionError,
        match="silver_records",
    ):
        validate_metrics(metrics)


def test_validate_metrics_rejects_missing_metric():
    metrics = EXPECTED_METRICS.copy()
    metrics.pop("quarantine_records")

    with pytest.raises(
        AssertionError,
        match="quarantine_records",
    ):
        validate_metrics(metrics)


def test_assert_idempotent_accepts_equal_runs():
    first_run = EXPECTED_METRICS.copy()
    second_run = EXPECTED_METRICS.copy()

    assert_idempotent(first_run, second_run)


def test_assert_idempotent_rejects_different_runs():
    first_run = EXPECTED_METRICS.copy()
    second_run = EXPECTED_METRICS.copy()
    second_run["gold_rows"] = 2

    with pytest.raises(
        AssertionError,
        match="reexecução",
    ):
        assert_idempotent(first_run, second_run)


def test_validate_output_root_accepts_nested_directory(
    tmp_path,
):
    output_root = tmp_path / "smoke-output"

    assert validate_output_root(output_root) == (
        output_root.resolve()
    )


@pytest.mark.parametrize(
    "protected_path",
    [
        Path("/"),
        Path("/tmp"),
        Path.cwd(),
    ],
)
def test_validate_output_root_rejects_protected_path(
    protected_path,
):
    with pytest.raises(
        ValueError,
        match="muito amplo",
    ):
        validate_output_root(protected_path)
