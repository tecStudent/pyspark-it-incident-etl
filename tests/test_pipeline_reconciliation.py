import json

from src.pipeline_reconciliation import (
    append_reconciliation_run,
    build_reconciliation_report,
    dq_split_differences,
    intake_differences,
    metadata_differences,
    successful_batches_by_id,
)


def controls():
    bronze = {
        "batches": [
            {
                "batch_id": "batch-1",
                "source_file": "batch-1.csv",
                "file_hash": "hash-1",
                "status": "SUCCESS",
                "records_written": 2,
            },
            {
                "batch_id": "batch-2",
                "source_file": "batch-2.csv",
                "file_hash": "hash-2",
                "status": "SUCCESS",
                "records_written": 2,
            },
        ]
    }
    silver = {
        "batches": [
            {
                "batch_id": "batch-1",
                "source_file": "batch-1.csv",
                "file_hash": "hash-1",
                "status": "SUCCESS",
                "records_received": 2,
                "valid_records_received": 2,
                "invalid_records_received": 0,
                "silver_total_records": 2,
                "quarantine_total_records": 0,
                "processed_at": "2026-08-13T10:00:00+00:00",
            },
            {
                "batch_id": "batch-2",
                "source_file": "batch-2.csv",
                "file_hash": "hash-2",
                "status": "SUCCESS",
                "records_received": 2,
                "valid_records_received": 1,
                "invalid_records_received": 1,
                "silver_total_records": 2,
                "quarantine_total_records": 1,
                "processed_at": "2026-08-13T11:00:00+00:00",
            },
        ]
    }
    gold = {
        "batches": [
            {
                "batch_id": "batch-1",
                "source_file": "batch-1.csv",
                "file_hash": "hash-1",
                "status": "SUCCESS",
                "gold_snapshot_records": 2,
                "processed_at": "2026-08-13T10:00:00+00:00",
            },
            {
                "batch_id": "batch-2",
                "source_file": "batch-2.csv",
                "file_hash": "hash-2",
                "status": "SUCCESS",
                "gold_snapshot_records": 2,
                "processed_at": "2026-08-13T11:00:00+00:00",
            },
        ]
    }

    return bronze, silver, gold


def physical_state():
    return {
        "bronze_records": 4,
        "silver_records": 2,
        "quarantine_records": 1,
        "missing_bronze_paths": [],
        "missing_gold_outputs": [],
    }


def report_with(
    bronze=None,
    silver=None,
    gold=None,
    physical=None,
):
    default_bronze, default_silver, default_gold = controls()

    return build_reconciliation_report(
        bronze or default_bronze,
        silver or default_silver,
        gold or default_gold,
        physical or physical_state(),
        generated_at="2026-08-13T12:00:00+00:00",
    )


def failed_check(report, name):
    return next(
        check
        for check in report["checks"]
        if check["name"] == name
    )


def test_successful_batches_filters_failed_and_keeps_latest():
    control = {
        "batches": [
            {
                "batch_id": "batch-1",
                "status": "FAILED",
            },
            {
                "batch_id": "batch-1",
                "status": "SUCCESS",
                "records_written": 1,
            },
            {
                "batch_id": "batch-1",
                "status": "SUCCESS",
                "records_written": 2,
            },
        ]
    }

    result = successful_batches_by_id(control)

    assert list(result) == ["batch-1"]
    assert result["batch-1"]["records_written"] == 2


def test_consistent_controls_pass_all_checks():
    report = report_with()

    assert report["status"] == "PASS"
    assert report["checks_failed"] == 0
    assert report["checks_passed"] == report["checks_total"]
    assert report["metrics"]["duplicates_removed"] == 1


def test_detects_batch_set_mismatch():
    bronze, silver, gold = controls()
    gold["batches"].pop()

    report = report_with(bronze, silver, gold)

    assert report["status"] == "FAIL"
    assert failed_check(
        report,
        "batch_sets_match",
    )["status"] == "FAIL"


def test_detects_metadata_mismatch():
    bronze, silver, gold = controls()
    silver["batches"][0]["file_hash"] = "changed"

    differences = metadata_differences(
        successful_batches_by_id(bronze),
        successful_batches_by_id(silver),
        successful_batches_by_id(gold),
    )

    assert differences[0]["field"] == "file_hash"
    assert report_with(
        bronze,
        silver,
        gold,
    )["status"] == "FAIL"


def test_detects_bronze_to_silver_intake_mismatch():
    bronze, silver, _ = controls()
    silver["batches"][0]["records_received"] = 3

    differences = intake_differences(
        successful_batches_by_id(bronze),
        successful_batches_by_id(silver),
    )

    assert differences == [
        {
            "batch_id": "batch-1",
            "expected": 2,
            "actual": 3,
        }
    ]


def test_detects_invalid_dq_split():
    _, silver, _ = controls()
    silver["batches"][1]["invalid_records_received"] = 0

    differences = dq_split_differences(
        successful_batches_by_id(silver)
    )

    assert differences[0]["batch_id"] == "batch-2"


def test_detects_negative_duplicate_balance():
    bronze, silver, gold = controls()
    silver["batches"][-1]["silver_total_records"] = 5

    report = report_with(bronze, silver, gold)

    assert failed_check(
        report,
        "snapshot_conservation",
    )["actual"] == -2


def test_detects_gold_snapshot_mismatch():
    bronze, silver, gold = controls()
    gold["batches"][-1]["gold_snapshot_records"] = 99

    report = report_with(bronze, silver, gold)

    assert failed_check(
        report,
        "gold_snapshot_matches_silver",
    )["status"] == "FAIL"


def test_detects_physical_bronze_mismatch():
    physical = physical_state()
    physical["bronze_records"] = 3

    report = report_with(physical=physical)

    assert failed_check(
        report,
        "bronze_control_matches_storage",
    )["status"] == "FAIL"


def test_detects_physical_silver_mismatch():
    physical = physical_state()
    physical["silver_records"] = 1

    report = report_with(physical=physical)

    assert failed_check(
        report,
        "silver_control_matches_storage",
    )["status"] == "FAIL"


def test_detects_physical_quarantine_mismatch():
    physical = physical_state()
    physical["quarantine_records"] = 0

    report = report_with(physical=physical)

    assert failed_check(
        report,
        "quarantine_control_matches_storage",
    )["status"] == "FAIL"


def test_detects_missing_paths_and_preserves_history(
    tmp_path,
):
    physical = physical_state()
    physical["missing_bronze_paths"] = ["missing/bronze"]
    physical["missing_gold_outputs"] = ["missing/gold"]

    report = report_with(physical=physical)

    assert report["status"] == "FAIL"
    assert failed_check(
        report,
        "bronze_paths_exist",
    )["status"] == "FAIL"
    assert failed_check(
        report,
        "gold_outputs_exist",
    )["status"] == "FAIL"

    history_path = tmp_path / "reconciliation_runs.json"

    append_reconciliation_run(report, history_path)
    append_reconciliation_run(report_with(), history_path)

    history = json.loads(
        history_path.read_text(encoding="utf-8")
    )

    assert [
        run["status"]
        for run in history["runs"]
    ] == ["FAIL", "PASS"]
