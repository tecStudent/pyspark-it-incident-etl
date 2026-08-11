import json
from pathlib import Path

from src import pipeline_audit


def write_json(
    path: Path,
    content: dict,
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    path.write_text(
        json.dumps(content),
        encoding="utf-8",
    )


def test_successful_batches_filters_and_deduplicates(
    tmp_path,
) -> None:
    control_path = tmp_path / "control.json"

    write_json(
        control_path,
        {
            "version": 1,
            "batches": [
                {
                    "batch_id": "batch-1",
                    "status": "FAILED",
                },
                {
                    "batch_id": "batch-1",
                    "status": "SUCCESS",
                    "records_written": 10,
                },
                {
                    "batch_id": "batch-2",
                    "status": "SUCCESS",
                    "records_written": 5,
                },
            ],
        },
    )

    result = pipeline_audit.successful_batches(
        control_path
    )

    assert len(result) == 2

    assert {
        batch["batch_id"]
        for batch in result
    } == {
        "batch-1",
        "batch-2",
    }


def test_latest_batch_returns_most_recent() -> None:
    batches = [
        {
            "batch_id": "old",
            "processed_at": (
                "2026-08-10T10:00:00+00:00"
            ),
        },
        {
            "batch_id": "new",
            "processed_at": (
                "2026-08-11T10:00:00+00:00"
            ),
        },
    ]

    result = pipeline_audit.latest_batch(
        batches
    )

    assert result["batch_id"] == "new"

    assert pipeline_audit.latest_batch(
        []
    ) == {}


def test_build_control_snapshot(
    tmp_path,
    monkeypatch,
) -> None:
    landing_dir = tmp_path / "landing"
    landing_dir.mkdir()

    (landing_dir / "batch_1.csv").touch()
    (landing_dir / "batch_2.csv").touch()

    bronze_control = tmp_path / "bronze.json"
    silver_control = tmp_path / "silver.json"
    gold_control = tmp_path / "gold.json"

    write_json(
        bronze_control,
        {
            "batches": [
                {
                    "batch_id": "1",
                    "status": "SUCCESS",
                    "records_written": 10,
                },
                {
                    "batch_id": "2",
                    "status": "SUCCESS",
                    "records_written": 5,
                },
                {
                    "batch_id": "3",
                    "status": "FAILED",
                    "records_written": 100,
                },
            ],
        },
    )

    write_json(
        silver_control,
        {
            "batches": [
                {
                    "batch_id": "1",
                    "status": "SUCCESS",
                    "silver_total_records": 10,
                    "quarantine_total_records": 0,
                    "processed_at": (
                        "2026-08-10T10:00:00+00:00"
                    ),
                },
                {
                    "batch_id": "2",
                    "status": "SUCCESS",
                    "silver_total_records": 14,
                    "quarantine_total_records": 1,
                    "processed_at": (
                        "2026-08-11T10:00:00+00:00"
                    ),
                },
            ],
        },
    )

    write_json(
        gold_control,
        {
            "batches": [
                {
                    "batch_id": "1",
                    "status": "SUCCESS",
                    "gold_snapshot_records": 10,
                    "processed_at": (
                        "2026-08-10T10:00:00+00:00"
                    ),
                },
                {
                    "batch_id": "2",
                    "status": "SUCCESS",
                    "gold_snapshot_records": 14,
                    "processed_at": (
                        "2026-08-11T10:00:00+00:00"
                    ),
                },
            ],
        },
    )

    monkeypatch.setattr(
        pipeline_audit,
        "LANDING_DIR",
        landing_dir,
    )

    monkeypatch.setattr(
        pipeline_audit,
        "BRONZE_CONTROL",
        bronze_control,
    )

    monkeypatch.setattr(
        pipeline_audit,
        "SILVER_CONTROL",
        silver_control,
    )

    monkeypatch.setattr(
        pipeline_audit,
        "GOLD_CONTROL",
        gold_control,
    )

    result = (
        pipeline_audit.build_control_snapshot()
    )

    assert result == {
        "landing_files": 2,
        "bronze": {
            "successful_batches": 2,
            "records_written": 15,
        },
        "silver": {
            "successful_batches": 2,
            "total_records": 14,
            "quarantine_records": 1,
        },
        "gold": {
            "successful_batches": 2,
            "snapshot_records": 14,
        },
    }


def test_append_pipeline_run_creates_control(
    tmp_path,
    monkeypatch,
) -> None:
    runs_path = tmp_path / "pipeline_runs.json"

    monkeypatch.setattr(
        pipeline_audit,
        "PIPELINE_RUNS",
        runs_path,
    )

    pipeline_audit.append_pipeline_run(
        {
            "run_id": "run-1",
            "status": "SUCCESS",
        }
    )

    content = json.loads(
        runs_path.read_text(
            encoding="utf-8"
        )
    )

    assert content["version"] == 1
    assert len(content["runs"]) == 1
    assert content["runs"][0]["run_id"] == "run-1"


def test_append_pipeline_run_preserves_history(
    tmp_path,
    monkeypatch,
) -> None:
    runs_path = tmp_path / "pipeline_runs.json"

    monkeypatch.setattr(
        pipeline_audit,
        "PIPELINE_RUNS",
        runs_path,
    )

    pipeline_audit.append_pipeline_run(
        {
            "run_id": "run-1",
            "status": "SUCCESS",
        }
    )

    pipeline_audit.append_pipeline_run(
        {
            "run_id": "run-2",
            "status": "FAILED",
        }
    )

    content = json.loads(
        runs_path.read_text(
            encoding="utf-8"
        )
    )

    assert [
        run["run_id"]
        for run in content["runs"]
    ] == [
        "run-1",
        "run-2",
    ]