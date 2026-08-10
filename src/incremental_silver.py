import json
import os
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.silver import (
    deduplicate,
    transform_records,
)


BRONZE_CONTROL_PATH = Path(
    "data/control/processed_batches.json"
)

SILVER_CONTROL_PATH = Path(
    "data/control/silver_batches.json"
)

SILVER_OUTPUT = Path(
    "data/silver/incremental_incidents"
)

SILVER_STAGING = Path(
    "data/silver/incremental_incidents_staging"
)

QUARANTINE_OUTPUT = Path(
    "data/quarantine/incidents"
)

QUARANTINE_STAGING = Path(
    "data/quarantine/incidents_staging"
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IT Incident Incremental Silver")
        .getOrCreate()
    )


def load_control(
    path: Path,
) -> dict[str, Any]:
    if not path.exists():
        return {
            "version": 1,
            "batches": [],
        }

    with path.open(
        "r",
        encoding="utf-8",
    ) as control_file:
        control = json.load(control_file)

    if "batches" not in control:
        raise ValueError(
            f"Controle inválido: {path}"
        )

    return control


def save_control(
    path: Path,
    control: dict[str, Any],
) -> None:
    path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path = path.with_suffix(
        ".tmp"
    )

    with temporary_path.open(
        "w",
        encoding="utf-8",
    ) as control_file:
        json.dump(
            control,
            control_file,
            ensure_ascii=False,
            indent=2,
        )

    os.replace(
        temporary_path,
        path,
    )


def get_successful_bronze_batches(
    control: dict[str, Any],
) -> list[dict[str, Any]]:
    batches_by_id = {}

    for batch in control["batches"]:
        if batch.get("status") != "SUCCESS":
            continue

        batch_id = batch["batch_id"]
        batches_by_id[batch_id] = batch

    return sorted(
        batches_by_id.values(),
        key=lambda batch: batch["processed_at"],
    )


def get_processed_silver_batches(
    control: dict[str, Any],
) -> set[str]:
    return {
        batch["batch_id"]
        for batch in control["batches"]
        if batch.get("status") == "SUCCESS"
    }


def remove_staging_directory(
    path: Path,
) -> None:
    if path.exists():
        shutil.rmtree(path)


def replace_output_directory(
    staging_path: Path,
    output_path: Path,
) -> None:
    backup_path = output_path.with_name(
        f"{output_path.name}_backup"
    )

    if backup_path.exists():
        shutil.rmtree(backup_path)

    if output_path.exists():
        output_path.rename(backup_path)

    try:
        staging_path.rename(output_path)

    except Exception:
        if (
            backup_path.exists()
            and not output_path.exists()
        ):
            backup_path.rename(output_path)

        raise

    if backup_path.exists():
        shutil.rmtree(backup_path)


def write_staging_outputs(
    spark: SparkSession,
    valid_df: DataFrame,
    invalid_df: DataFrame,
    valid_count: int,
    invalid_count: int,
) -> None:
    remove_staging_directory(
        SILVER_STAGING
    )

    remove_staging_directory(
        QUARANTINE_STAGING
    )

    (
        valid_df.write
        .mode("overwrite")
        .partitionBy(
            "opened_year",
            "opened_month",
        )
        .parquet(str(SILVER_STAGING))
    )

    (
        invalid_df.write
        .mode("overwrite")
        .parquet(str(QUARANTINE_STAGING))
    )

    written_valid_count = (
        spark.read
        .parquet(str(SILVER_STAGING))
        .count()
    )

    written_invalid_count = (
        spark.read
        .parquet(str(QUARANTINE_STAGING))
        .count()
    )

    if written_valid_count != valid_count:
        raise RuntimeError(
            "Quantidade válida gravada "
            "na Silver não confere."
        )

    if written_invalid_count != invalid_count:
        raise RuntimeError(
            "Quantidade gravada "
            "na quarentena não confere."
        )


def collect_batch_metrics(
    transformed_df: DataFrame,
) -> dict[str, dict[str, int]]:
    rows = (
        transformed_df
        .groupBy(
            "_batch_id",
            "dq_status",
        )
        .count()
        .collect()
    )

    metrics: dict[str, dict[str, int]] = {}

    for row in rows:
        batch_metrics = metrics.setdefault(
            row["_batch_id"],
            {
                "valid_records_received": 0,
                "invalid_records_received": 0,
            },
        )

        if row["dq_status"] == "VALID":
            batch_metrics[
                "valid_records_received"
            ] += row["count"]

        else:
            batch_metrics[
                "invalid_records_received"
            ] += row["count"]

    return metrics


def register_successful_batches(
    silver_control: dict[str, Any],
    pending_batches: list[dict[str, Any]],
    batch_metrics: dict[str, dict[str, int]],
    silver_count: int,
    quarantine_count: int,
) -> None:
    processed_at = datetime.now(
        timezone.utc
    ).isoformat()

    for batch in pending_batches:
        batch_id = batch["batch_id"]

        metrics = batch_metrics.get(
            batch_id,
            {
                "valid_records_received": 0,
                "invalid_records_received": 0,
            },
        )

        silver_control["batches"].append(
            {
                "batch_id": batch_id,
                "source_file": batch["source_file"],
                "file_hash": batch["file_hash"],
                "status": "SUCCESS",
                "records_received": batch[
                    "records_written"
                ],
                **metrics,
                "silver_total_records": silver_count,
                "quarantine_total_records": (
                    quarantine_count
                ),
                "processed_at": processed_at,
            }
        )

    save_control(
        SILVER_CONTROL_PATH,
        silver_control,
    )


def main() -> None:
    if not BRONZE_CONTROL_PATH.exists():
        raise FileNotFoundError(
            "Controle da Bronze não encontrado: "
            f"{BRONZE_CONTROL_PATH}"
        )

    bronze_control = load_control(
        BRONZE_CONTROL_PATH
    )

    silver_control = load_control(
        SILVER_CONTROL_PATH
    )

    bronze_batches = (
        get_successful_bronze_batches(
            bronze_control
        )
    )

    processed_batch_ids = (
        get_processed_silver_batches(
            silver_control
        )
    )

    pending_batches = []

    for batch in bronze_batches:
        if batch["batch_id"] in processed_batch_ids:
            print(
                "Batch já aplicado na Silver: "
                f"{batch['source_file']}"
            )
            continue

        pending_batches.append(batch)

    if not pending_batches:
        print(
            "Nenhum batch novo para aplicar "
            "na Silver."
        )
        return

    bronze_paths = [
        batch["bronze_path"]
        for batch in pending_batches
    ]

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    transformed_df = None
    merged_df = None

    try:
        new_bronze_df = (
            spark.read
            .parquet(*bronze_paths)
        )

        new_bronze_count = (
            new_bronze_df.count()
        )

        transformed_df = (
            transform_records(
                new_bronze_df
            )
            .cache()
        )

        transformed_count = (
            transformed_df.count()
        )

        if transformed_count != new_bronze_count:
            raise RuntimeError(
                "Quantidade transformada "
                "não confere com a Bronze."
            )

        batch_metrics = collect_batch_metrics(
            transformed_df
        )

        if SILVER_OUTPUT.exists():
            existing_silver_df = (
                spark.read
                .parquet(str(SILVER_OUTPUT))
            )

            existing_silver_count = (
                existing_silver_df.count()
            )

            combined_df = (
                existing_silver_df
                .unionByName(
                    transformed_df,
                    allowMissingColumns=True,
                )
            )

        else:
            existing_silver_count = 0
            combined_df = transformed_df

        combined_count = (
            existing_silver_count
            + transformed_count
        )

        merged_df = (
            deduplicate(combined_df)
            .cache()
        )

        merged_count = merged_df.count()

        duplicate_count = (
            combined_count - merged_count
        )

        valid_df = merged_df.filter(
            F.col("dq_status") == "VALID"
        )

        invalid_df = merged_df.filter(
            F.col("dq_status") == "INVALID"
        )

        valid_count = valid_df.count()
        invalid_count = invalid_df.count()

        if (
            valid_count + invalid_count
            != merged_count
        ):
            raise RuntimeError(
                "A soma entre Silver e quarentena "
                "não confere."
            )

        write_staging_outputs(
            spark=spark,
            valid_df=valid_df,
            invalid_df=invalid_df,
            valid_count=valid_count,
            invalid_count=invalid_count,
        )

        replace_output_directory(
            SILVER_STAGING,
            SILVER_OUTPUT,
        )

        replace_output_directory(
            QUARANTINE_STAGING,
            QUARANTINE_OUTPUT,
        )

        register_successful_batches(
            silver_control=silver_control,
            pending_batches=pending_batches,
            batch_metrics=batch_metrics,
            silver_count=valid_count,
            quarantine_count=invalid_count,
        )

        print(
            f"Batches novos aplicados: "
            f"{len(pending_batches)}"
        )

        print(
            f"Registros recebidos: "
            f"{new_bronze_count}"
        )

        print(
            f"Registros Silver anteriores: "
            f"{existing_silver_count}"
        )

        print(
            f"Duplicidades removidas: "
            f"{duplicate_count}"
        )

        print(
            f"Registros válidos atuais: "
            f"{valid_count}"
        )

        print(
            f"Registros em quarentena: "
            f"{invalid_count}"
        )

        print("Validação Silver incremental: OK")

    finally:
        if transformed_df is not None:
            transformed_df.unpersist()

        if merged_df is not None:
            merged_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()