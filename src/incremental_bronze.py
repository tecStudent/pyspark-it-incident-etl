import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


LANDING_DIR = Path("data/landing")
CONTROL_PATH = Path(
    "data/control/processed_batches.json"
)
BRONZE_ROOT = Path(
    "data/bronze/incremental_incidents"
)


RAW_SCHEMA = StructType([
    StructField("Número", StringType(), True),
    StructField("Prioridade", StringType(), True),
    StructField("Produto", StringType(), True),
    StructField("Categoria", StringType(), True),
    StructField("Subcategoria", StringType(), True),
    StructField("Grupo designado", StringType(), True),
    StructField("Item de configuração", StringType(), True),
    StructField("Aberto", StringType(), True),
    StructField("Resolvido", StringType(), True),
    StructField("Encerrado", StringType(), True),
    StructField("Duração", StringType(), True),
    StructField("Código de fechamento", StringType(), True),
    StructField("Descrição resumida", StringType(), True),
    StructField("Solução", StringType(), True),
    StructField("Aberto por", StringType(), True),
    StructField("Incidente Pai", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Entrou para KPI?", StringType(), True),
    StructField("KPI Violado?", StringType(), True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IT Incident Incremental Bronze")
        .getOrCreate()
    )


def calculate_file_hash(
    file_path: Path,
) -> str:
    sha256 = hashlib.sha256()

    with file_path.open("rb") as source_file:
        while chunk := source_file.read(1024 * 1024):
            sha256.update(chunk)

    return sha256.hexdigest()


def load_control() -> dict[str, Any]:
    if not CONTROL_PATH.exists():
        return {
            "version": 1,
            "batches": [],
        }

    with CONTROL_PATH.open(
        "r",
        encoding="utf-8",
    ) as control_file:
        control = json.load(control_file)

    if "batches" not in control:
        raise ValueError(
            "Arquivo de controle inválido: "
            "campo 'batches' ausente."
        )

    return control


def save_control(
    control: dict[str, Any],
) -> None:
    CONTROL_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    temporary_path = CONTROL_PATH.with_suffix(
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
        CONTROL_PATH,
    )


def read_batch(
    spark: SparkSession,
    file_path: Path,
) -> DataFrame:
    return (
        spark.read
        .schema(RAW_SCHEMA)
        .option("header", "true")
        .option("encoding", "UTF-8")
        .option("multiLine", "true")
        .option("quote", '"')
        .option("escape", '"')
        .option("mode", "FAILFAST")
        .option("enforceSchema", "false")
        .csv(str(file_path))
    )


def successful_hashes(
    control: dict[str, Any],
) -> set[str]:
    return {
        batch["file_hash"]
        for batch in control["batches"]
        if batch.get("status") == "SUCCESS"
    }


def register_batch(
    control: dict[str, Any],
    batch_record: dict[str, Any],
) -> None:
    control["batches"].append(batch_record)
    save_control(control)


def process_batch(
    spark: SparkSession,
    control: dict[str, Any],
    file_path: Path,
    file_hash: str,
) -> None:
    batch_id = file_hash[:16]

    output_path = (
        BRONZE_ROOT
        / f"batch_{batch_id}"
    )

    processed_at = datetime.now(
        timezone.utc
    ).isoformat()

    print(
        f"\nProcessando arquivo: {file_path.name}"
    )
    print(f"Batch ID: {batch_id}")
    print(f"SHA-256: {file_hash}")

    try:
        raw_df = read_batch(
            spark,
            file_path,
        )

        bronze_df = (
            raw_df
            .withColumn(
                "_batch_id",
                F.lit(batch_id),
            )
            .withColumn(
                "_source_file",
                F.lit(file_path.name),
            )
            .withColumn(
                "_source_hash",
                F.lit(file_hash),
            )
            .withColumn(
                "_ingested_at",
                F.current_timestamp(),
            )
        )

        source_count = bronze_df.count()

        if source_count == 0:
            raise ValueError(
                "O lote não possui registros."
            )

        (
            bronze_df.write
            .mode("overwrite")
            .parquet(str(output_path))
        )

        output_count = (
            spark.read
            .parquet(str(output_path))
            .count()
        )

        if source_count != output_count:
            raise RuntimeError(
                "Quantidade da origem e da "
                "Bronze incremental não confere."
            )

        register_batch(
            control,
            {
                "batch_id": batch_id,
                "source_file": file_path.name,
                "file_hash": file_hash,
                "status": "SUCCESS",
                "records_read": source_count,
                "records_written": output_count,
                "bronze_path": str(output_path),
                "processed_at": processed_at,
            },
        )

        print(
            f"Registros lidos: {source_count}"
        )
        print(
            f"Registros gravados: {output_count}"
        )
        print("Status do lote: SUCCESS")

    except Exception as error:
        register_batch(
            control,
            {
                "batch_id": batch_id,
                "source_file": file_path.name,
                "file_hash": file_hash,
                "status": "FAILED",
                "error": str(error),
                "processed_at": processed_at,
            },
        )

        print("Status do lote: FAILED")

        raise


def main() -> None:
    LANDING_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    landing_files = sorted(
        LANDING_DIR.glob("*.csv")
    )

    if not landing_files:
        print(
            "Nenhum arquivo CSV encontrado "
            "em data/landing."
        )
        return

    control = load_control()

    processed = successful_hashes(control)

    pending_files = []

    for file_path in landing_files:
        file_hash = calculate_file_hash(
            file_path
        )

        if file_hash in processed:
            print(
                f"Arquivo já processado: "
                f"{file_path.name}"
            )
            continue

        pending_files.append(
            (file_path, file_hash)
        )

    if not pending_files:
        print(
            "Nenhum lote novo para processar."
        )
        return

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        for file_path, file_hash in pending_files:
            process_batch(
                spark=spark,
                control=control,
                file_path=file_path,
                file_hash=file_hash,
            )

    finally:
        spark.stop()

    print(
        f"\nLotes novos processados: "
        f"{len(pending_files)}"
    )


if __name__ == "__main__":
    main()