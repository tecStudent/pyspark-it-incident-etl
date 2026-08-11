import json
import os
from pathlib import Path
from typing import Any


BRONZE_CONTROL = Path(
    "data/control/processed_batches.json"
)

SILVER_CONTROL = Path(
    "data/control/silver_batches.json"
)

GOLD_CONTROL = Path(
    "data/control/gold_batches.json"
)

PIPELINE_RUNS = Path(
    "data/control/pipeline_runs.json"
)

LANDING_DIR = Path("data/landing")


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {
            "version": 1,
            "batches": [],
        }

    with path.open(
        "r",
        encoding="utf-8",
    ) as source_file:
        return json.load(source_file)


def successful_batches(
    path: Path,
) -> list[dict[str, Any]]:
    control = load_json(path)
    batches_by_id = {}

    for batch in control.get("batches", []):
        if batch.get("status") != "SUCCESS":
            continue

        batches_by_id[
            batch["batch_id"]
        ] = batch

    return list(batches_by_id.values())


def latest_batch(
    batches: list[dict[str, Any]],
) -> dict[str, Any]:
    if not batches:
        return {}

    return max(
        batches,
        key=lambda batch: batch.get(
            "processed_at",
            "",
        ),
    )


def build_control_snapshot() -> dict[str, Any]:
    bronze_batches = successful_batches(
        BRONZE_CONTROL
    )

    silver_batches = successful_batches(
        SILVER_CONTROL
    )

    gold_batches = successful_batches(
        GOLD_CONTROL
    )

    latest_silver = latest_batch(
        silver_batches
    )

    latest_gold = latest_batch(
        gold_batches
    )

    return {
        "landing_files": len(
            list(LANDING_DIR.glob("*.csv"))
        ),
        "bronze": {
            "successful_batches": len(
                bronze_batches
            ),
            "records_written": sum(
                batch.get("records_written", 0)
                for batch in bronze_batches
            ),
        },
        "silver": {
            "successful_batches": len(
                silver_batches
            ),
            "total_records": latest_silver.get(
                "silver_total_records",
                0,
            ),
            "quarantine_records": (
                latest_silver.get(
                    "quarantine_total_records",
                    0,
                )
            ),
        },
        "gold": {
            "successful_batches": len(
                gold_batches
            ),
            "snapshot_records": latest_gold.get(
                "gold_snapshot_records",
                0,
            ),
        },
    }


def append_pipeline_run(
    run_record: dict[str, Any],
) -> None:
    if PIPELINE_RUNS.exists():
        control = load_json(
            PIPELINE_RUNS
        )
    else:
        control = {
            "version": 1,
            "runs": [],
        }

    control.setdefault(
        "runs",
        [],
    ).append(run_record)

    PIPELINE_RUNS.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path = (
        PIPELINE_RUNS.with_suffix(".tmp")
    )

    with temporary_path.open(
        "w",
        encoding="utf-8",
    ) as output_file:
        json.dump(
            control,
            output_file,
            ensure_ascii=False,
            indent=2,
        )

    os.replace(
        temporary_path,
        PIPELINE_RUNS,
    )