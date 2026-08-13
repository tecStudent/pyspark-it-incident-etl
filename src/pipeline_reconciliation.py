import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4


BRONZE_CONTROL_PATH = Path(
    "data/control/processed_batches.json"
)
SILVER_CONTROL_PATH = Path(
    "data/control/silver_batches.json"
)
GOLD_CONTROL_PATH = Path(
    "data/control/gold_batches.json"
)
RECONCILIATION_RUNS_PATH = Path(
    "data/control/reconciliation_runs.json"
)

SILVER_OUTPUT_PATH = Path(
    "data/silver/incremental_incidents"
)
QUARANTINE_OUTPUT_PATH = Path(
    "data/quarantine/incidents"
)

GOLD_OUTPUT_PATHS = (
    Path("data/gold/incremental/monthly_kpis"),
    Path("data/gold/incremental/priority_summary"),
    Path("data/gold/incremental/team_summary"),
    Path("data/gold/incremental/dashboard_summary"),
    Path("data/gold/incremental/daily_trends"),
    Path("data/gold/incremental/operational_kpi_summary"),
    Path("data/gold/incremental/annual_ola_summary"),
    Path("data/gold/incremental/risk_summary"),
    Path("data/gold/incremental/forecast_history"),
    Path("data/gold/incremental/forecast_summary"),
    Path("data/gold/incremental/recommendations"),
)


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_control(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise FileNotFoundError(
            f"Arquivo de controle não encontrado: {path}"
        )

    with path.open("r", encoding="utf-8") as source_file:
        control = json.load(source_file)

    if not isinstance(control.get("batches"), list):
        raise ValueError(
            f"Controle inválido, campo 'batches' ausente: {path}"
        )

    return control


def successful_batches_by_id(
    control: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    result = {}

    for batch in control.get("batches", []):
        if batch.get("status") != "SUCCESS":
            continue

        batch_id = batch.get("batch_id")

        if not batch_id:
            raise ValueError(
                "Batch SUCCESS sem batch_id no controle."
            )

        result[batch_id] = batch

    return result


def latest_batch(
    batches: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    if not batches:
        return {}

    return max(
        batches.values(),
        key=lambda batch: batch.get("processed_at", ""),
    )


def add_check(
    checks: list[dict[str, Any]],
    name: str,
    passed: bool,
    expected: Any,
    actual: Any,
    message: str,
) -> None:
    checks.append(
        {
            "name": name,
            "status": "PASS" if passed else "FAIL",
            "expected": expected,
            "actual": actual,
            "message": message,
        }
    )


def metadata_differences(
    bronze_batches: dict[str, dict[str, Any]],
    silver_batches: dict[str, dict[str, Any]],
    gold_batches: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    differences = []

    common_ids = (
        set(bronze_batches)
        & set(silver_batches)
        & set(gold_batches)
    )

    for batch_id in sorted(common_ids):
        bronze = bronze_batches[batch_id]

        for layer, batch in (
            ("silver", silver_batches[batch_id]),
            ("gold", gold_batches[batch_id]),
        ):
            for field in ("source_file", "file_hash"):
                if batch.get(field) != bronze.get(field):
                    differences.append(
                        {
                            "batch_id": batch_id,
                            "layer": layer,
                            "field": field,
                            "expected": bronze.get(field),
                            "actual": batch.get(field),
                        }
                    )

    return differences


def intake_differences(
    bronze_batches: dict[str, dict[str, Any]],
    silver_batches: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    differences = []

    for batch_id in sorted(
        set(bronze_batches) & set(silver_batches)
    ):
        expected = bronze_batches[batch_id].get(
            "records_written",
            0,
        )
        actual = silver_batches[batch_id].get(
            "records_received",
            0,
        )

        if expected != actual:
            differences.append(
                {
                    "batch_id": batch_id,
                    "expected": expected,
                    "actual": actual,
                }
            )

    return differences


def dq_split_differences(
    silver_batches: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    differences = []

    for batch_id, batch in sorted(
        silver_batches.items()
    ):
        expected = batch.get("records_received", 0)
        actual = (
            batch.get("valid_records_received", 0)
            + batch.get("invalid_records_received", 0)
        )

        if expected != actual:
            differences.append(
                {
                    "batch_id": batch_id,
                    "expected": expected,
                    "actual": actual,
                }
            )

    return differences


def build_reconciliation_report(
    bronze_control: dict[str, Any],
    silver_control: dict[str, Any],
    gold_control: dict[str, Any],
    physical_state: dict[str, Any],
    generated_at: str | None = None,
) -> dict[str, Any]:
    bronze_batches = successful_batches_by_id(
        bronze_control
    )
    silver_batches = successful_batches_by_id(
        silver_control
    )
    gold_batches = successful_batches_by_id(
        gold_control
    )

    bronze_ids = sorted(bronze_batches)
    silver_ids = sorted(silver_batches)
    gold_ids = sorted(gold_batches)

    bronze_total = sum(
        batch.get("records_written", 0)
        for batch in bronze_batches.values()
    )

    latest_silver = latest_batch(silver_batches)
    latest_gold = latest_batch(gold_batches)

    silver_total = latest_silver.get(
        "silver_total_records",
        0,
    )
    quarantine_total = latest_silver.get(
        "quarantine_total_records",
        0,
    )
    gold_snapshot = latest_gold.get(
        "gold_snapshot_records",
        0,
    )
    duplicates_removed = (
        bronze_total - silver_total - quarantine_total
    )

    checks = []

    batch_sets = {
        "bronze": bronze_ids,
        "silver": silver_ids,
        "gold": gold_ids,
    }
    batches_match = (
        bool(bronze_ids)
        and bronze_ids == silver_ids == gold_ids
    )

    add_check(
        checks,
        "batch_sets_match",
        batches_match,
        bronze_ids,
        {
            "silver": silver_ids,
            "gold": gold_ids,
        },
        "Os mesmos batches SUCCESS devem existir nas três camadas.",
    )

    metadata_errors = metadata_differences(
        bronze_batches,
        silver_batches,
        gold_batches,
    )
    add_check(
        checks,
        "batch_metadata_match",
        not metadata_errors,
        [],
        metadata_errors,
        "Arquivo de origem e hash devem permanecer iguais entre camadas.",
    )

    intake_errors = intake_differences(
        bronze_batches,
        silver_batches,
    )
    add_check(
        checks,
        "batch_intake_counts_match",
        not intake_errors,
        [],
        intake_errors,
        "Cada batch Silver deve receber o volume gravado na Bronze.",
    )

    dq_errors = dq_split_differences(silver_batches)
    add_check(
        checks,
        "batch_dq_split_matches",
        not dq_errors,
        [],
        dq_errors,
        "Válidos e inválidos recebidos devem totalizar o batch.",
    )

    add_check(
        checks,
        "snapshot_conservation",
        duplicates_removed >= 0,
        "valor maior ou igual a zero",
        duplicates_removed,
        "Bronze menos Silver e quarentena define duplicidades removidas.",
    )

    add_check(
        checks,
        "gold_snapshot_matches_silver",
        gold_snapshot == silver_total,
        silver_total,
        gold_snapshot,
        "O snapshot processado pela Gold deve corresponder à Silver válida.",
    )

    missing_bronze_paths = physical_state.get(
        "missing_bronze_paths",
        [],
    )
    add_check(
        checks,
        "bronze_paths_exist",
        not missing_bronze_paths,
        [],
        missing_bronze_paths,
        "Todos os Parquets Bronze registrados devem existir.",
    )

    physical_bronze = physical_state.get(
        "bronze_records",
        0,
    )
    add_check(
        checks,
        "bronze_control_matches_storage",
        physical_bronze == bronze_total,
        bronze_total,
        physical_bronze,
        "A contagem física da Bronze deve corresponder ao controle.",
    )

    physical_silver = physical_state.get(
        "silver_records",
        0,
    )
    add_check(
        checks,
        "silver_control_matches_storage",
        physical_silver == silver_total,
        silver_total,
        physical_silver,
        "A contagem física da Silver deve corresponder ao controle.",
    )

    physical_quarantine = physical_state.get(
        "quarantine_records",
        0,
    )
    add_check(
        checks,
        "quarantine_control_matches_storage",
        physical_quarantine == quarantine_total,
        quarantine_total,
        physical_quarantine,
        "A contagem física da quarentena deve corresponder ao controle.",
    )

    missing_gold_outputs = physical_state.get(
        "missing_gold_outputs",
        [],
    )
    add_check(
        checks,
        "gold_outputs_exist",
        not missing_gold_outputs,
        [],
        missing_gold_outputs,
        "Todos os produtos Gold incrementais devem existir.",
    )

    failed_checks = [
        check for check in checks
        if check["status"] == "FAIL"
    ]

    return {
        "version": 1,
        "reconciliation_id": uuid4().hex,
        "generated_at": generated_at or utc_now(),
        "status": "PASS" if not failed_checks else "FAIL",
        "checks_total": len(checks),
        "checks_passed": len(checks) - len(failed_checks),
        "checks_failed": len(failed_checks),
        "batches": batch_sets,
        "metrics": {
            "bronze_records": bronze_total,
            "silver_records": silver_total,
            "quarantine_records": quarantine_total,
            "duplicates_removed": duplicates_removed,
            "gold_snapshot_records": gold_snapshot,
            "physical_bronze_records": physical_bronze,
            "physical_silver_records": physical_silver,
            "physical_quarantine_records": physical_quarantine,
        },
        "checks": checks,
    }


def count_parquet(spark, path: Path) -> int:
    if not path.exists():
        return 0

    return spark.read.parquet(str(path)).count()


def collect_physical_state(
    spark,
    bronze_control: dict[str, Any],
) -> dict[str, Any]:
    bronze_batches = successful_batches_by_id(
        bronze_control
    )
    bronze_path_values = [
        batch.get("bronze_path")
        for batch in bronze_batches.values()
    ]
    missing_bronze_paths = [
        str(path_value or "<bronze_path ausente>")
        for path_value in bronze_path_values
        if (
            not path_value
            or not Path(path_value).exists()
        )
    ]
    existing_bronze_paths = [
        str(path_value)
        for path_value in bronze_path_values
        if path_value and Path(path_value).exists()
    ]

    if existing_bronze_paths:
        bronze_records = (
            spark.read
            .parquet(*existing_bronze_paths)
            .count()
        )
    else:
        bronze_records = 0

    return {
        "bronze_records": bronze_records,
        "silver_records": count_parquet(
            spark,
            SILVER_OUTPUT_PATH,
        ),
        "quarantine_records": count_parquet(
            spark,
            QUARANTINE_OUTPUT_PATH,
        ),
        "missing_bronze_paths": missing_bronze_paths,
        "missing_gold_outputs": [
            str(path)
            for path in GOLD_OUTPUT_PATHS
            if not path.exists()
        ],
    }


def append_reconciliation_run(
    report: dict[str, Any],
    path: Path = RECONCILIATION_RUNS_PATH,
) -> None:
    if path.exists():
        with path.open("r", encoding="utf-8") as source_file:
            history = json.load(source_file)
    else:
        history = {
            "version": 1,
            "runs": [],
        }

    history.setdefault("runs", []).append(report)
    path.parent.mkdir(parents=True, exist_ok=True)

    temporary_path = path.with_suffix(".tmp")

    with temporary_path.open("w", encoding="utf-8") as output_file:
        json.dump(
            history,
            output_file,
            ensure_ascii=False,
            indent=2,
        )

    os.replace(temporary_path, path)


def print_report(report: dict[str, Any]) -> None:
    approved = report["status"] == "PASS"

    print(
        "\nReconciliação do pipeline: "
        + ("APROVADA" if approved else "REPROVADA")
    )
    print(
        "Checks aprovados: "
        f"{report['checks_passed']}/{report['checks_total']}"
    )

    metrics = report["metrics"]
    print(f"Bronze: {metrics['bronze_records']}")
    print(f"Silver: {metrics['silver_records']}")
    print(
        "Quarentena: "
        f"{metrics['quarantine_records']}"
    )
    print(
        "Duplicidades removidas: "
        f"{metrics['duplicates_removed']}"
    )
    print(
        "Snapshot Gold: "
        f"{metrics['gold_snapshot_records']}"
    )

    if not approved:
        print("\nDivergências encontradas:")

        for check in report["checks"]:
            if check["status"] == "FAIL":
                print(
                    f"- {check['name']}: {check['message']} "
                    f"esperado={check['expected']!r}, "
                    f"atual={check['actual']!r}"
                )


def main() -> None:
    from pyspark.sql import SparkSession

    bronze_control = load_control(
        BRONZE_CONTROL_PATH
    )
    silver_control = load_control(
        SILVER_CONTROL_PATH
    )
    gold_control = load_control(
        GOLD_CONTROL_PATH
    )

    spark = (
        SparkSession.builder
        .appName("IT Incident Pipeline Reconciliation")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        physical_state = collect_physical_state(
            spark,
            bronze_control,
        )
    finally:
        spark.stop()

    report = build_reconciliation_report(
        bronze_control,
        silver_control,
        gold_control,
        physical_state,
    )

    append_reconciliation_run(report)
    print_report(report)
    print(
        "Relatório registrado em: "
        f"{RECONCILIATION_RUNS_PATH}"
    )

    if report["status"] != "PASS":
        sys.exit(1)


if __name__ == "__main__":
    main()
