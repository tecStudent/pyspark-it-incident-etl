import argparse
import json
import shutil
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.gold import (
    create_dashboard_summary,
    create_monthly_kpis,
    create_priority_summary,
    create_team_summary,
)
from src.incremental_bronze import (
    RAW_SCHEMA,
    calculate_file_hash,
)
from src.silver import deduplicate, transform_records


DEFAULT_INPUT = Path(
    "data/sample/incidents_incremental_test.csv"
)
DEFAULT_OUTPUT_ROOT = Path(
    "/tmp/pyspark-it-incident-e2e-smoke"
)

EXPECTED_METRICS = {
    "bronze_records": 3,
    "deduplicated_records": 2,
    "silver_records": 1,
    "quarantine_records": 1,
    "monthly_rows": 1,
    "priority_rows": 1,
    "team_rows": 1,
    "dashboard_rows": 1,
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IT Incident End-to-End Smoke Test")
        .getOrCreate()
    )


def validate_output_root(output_root: Path) -> Path:
    resolved = output_root.resolve()

    protected_paths = {
        Path("/").resolve(),
        Path("/tmp").resolve(),
        Path.cwd().resolve(),
    }

    if resolved in protected_paths:
        raise ValueError(
            "Diretório de saída muito amplo para o smoke test: "
            f"{resolved}"
        )

    return resolved


def reset_output_root(output_root: Path) -> None:
    resolved = validate_output_root(output_root)

    if resolved.exists():
        shutil.rmtree(resolved)

    resolved.mkdir(parents=True, exist_ok=True)


def write_parquet_and_count(
    spark: SparkSession,
    df: DataFrame,
    output_path: Path,
    partition_columns: tuple[str, ...] = (),
) -> int:
    writer = df.write.mode("overwrite")

    if partition_columns:
        writer = writer.partitionBy(
            *partition_columns
        )

    writer.parquet(str(output_path))

    return (
        spark.read
        .parquet(str(output_path))
        .count()
    )


def validate_metrics(
    actual: dict[str, int],
    expected: dict[str, int] = EXPECTED_METRICS,
) -> None:
    differences = []

    for metric_name, expected_value in expected.items():
        actual_value = actual.get(metric_name)

        if actual_value != expected_value:
            differences.append(
                f"{metric_name}: esperado={expected_value}, "
                f"atual={actual_value}"
            )

    if differences:
        raise AssertionError(
            "Métricas end-to-end divergentes:\n- "
            + "\n- ".join(differences)
        )


def assert_idempotent(
    first_run: dict[str, int],
    second_run: dict[str, int],
) -> None:
    if first_run != second_run:
        raise AssertionError(
            "A reexecução alterou as métricas do pipeline.\n"
            f"Primeira execução: {first_run}\n"
            f"Segunda execução: {second_run}"
        )


def read_source(
    spark: SparkSession,
    input_path: Path,
) -> DataFrame:
    if not input_path.exists():
        raise FileNotFoundError(
            f"Amostra do smoke test não encontrada: {input_path}"
        )

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
        .csv(str(input_path))
    )


def run_smoke_pipeline(
    spark: SparkSession,
    input_path: Path,
    output_root: Path,
) -> dict[str, int]:
    bronze_path = output_root / "bronze"
    silver_path = output_root / "silver"
    quarantine_path = output_root / "quarantine"
    gold_root = output_root / "gold"

    source_df = read_source(spark, input_path)
    source_hash = calculate_file_hash(input_path)

    bronze_df = (
        source_df
        .withColumn(
            "_batch_id",
            F.lit("e2e-smoke-batch"),
        )
        .withColumn(
            "_source_file",
            F.lit(input_path.name),
        )
        .withColumn(
            "_source_hash",
            F.lit(source_hash),
        )
        .withColumn(
            "_ingested_at",
            F.current_timestamp(),
        )
    )

    bronze_records = write_parquet_and_count(
        spark,
        bronze_df,
        bronze_path,
    )

    transformed_df = transform_records(
        spark.read.parquet(str(bronze_path))
    )

    deduplicated_df = deduplicate(
        transformed_df
    ).cache()

    try:
        deduplicated_records = (
            deduplicated_df.count()
        )

        valid_df = deduplicated_df.filter(
            F.col("dq_status") == "VALID"
        )

        invalid_df = deduplicated_df.filter(
            F.col("dq_status") == "INVALID"
        )

        silver_records = write_parquet_and_count(
            spark,
            valid_df,
            silver_path,
            ("opened_year", "opened_month"),
        )

        quarantine_records = write_parquet_and_count(
            spark,
            invalid_df,
            quarantine_path,
        )

        quarantine_record = (
            spark.read
            .parquet(str(quarantine_path))
            .select("incident_id", "dq_issues")
            .first()
        )

        if (
            quarantine_record is None
            or quarantine_record["incident_id"] != "BAD-ID"
            or "invalid_incident_id"
            not in quarantine_record["dq_issues"]
        ):
            raise AssertionError(
                "O registro inválido não foi encaminhado "
                "corretamente para a quarentena."
            )

    finally:
        deduplicated_df.unpersist()

    silver_df = (
        spark.read
        .parquet(str(silver_path))
        .cache()
    )

    try:
        latest_record = (
            silver_df
            .select(
                "incident_id",
                "short_description",
                "duration_seconds",
            )
            .first()
        )

        if (
            latest_record is None
            or latest_record["incident_id"] != "INC9999998"
            or latest_record["short_description"]
            != "Versão mais recente"
            or latest_record["duration_seconds"] != 7200
        ):
            raise AssertionError(
                "A deduplicação não preservou a versão mais recente."
            )

        gold_frames = {
            "monthly": create_monthly_kpis(silver_df),
            "priority": create_priority_summary(silver_df),
            "team": create_team_summary(silver_df),
            "dashboard": create_dashboard_summary(silver_df),
        }

        monthly_record = (
            gold_frames["monthly"]
            .select(
                "total_incidents",
                "kpi_incidents",
                "kpi_violations",
                "kpi_compliance_pct",
            )
            .first()
        )

        if (
            monthly_record is None
            or monthly_record["total_incidents"] != 1
            or monthly_record["kpi_incidents"] != 1
            or monthly_record["kpi_violations"] != 0
            or monthly_record["kpi_compliance_pct"] != 100.0
        ):
            raise AssertionError(
                "A agregação mensal Gold produziu valores inesperados."
            )

        gold_counts = {
            name: write_parquet_and_count(
                spark,
                gold_df,
                gold_root / name,
            )
            for name, gold_df in gold_frames.items()
        }

    finally:
        silver_df.unpersist()

    metrics = {
        "bronze_records": bronze_records,
        "deduplicated_records": deduplicated_records,
        "silver_records": silver_records,
        "quarantine_records": quarantine_records,
        "monthly_rows": gold_counts["monthly"],
        "priority_rows": gold_counts["priority"],
        "team_rows": gold_counts["team"],
        "dashboard_rows": gold_counts["dashboard"],
    }

    validate_metrics(metrics)

    return metrics


def main() -> None:
    parser = argparse.ArgumentParser(
        description=(
            "Executa um smoke test end-to-end com uma amostra "
            "versionada e saídas isoladas."
        )
    )

    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="Arquivo CSV usado como entrada do smoke test.",
    )

    parser.add_argument(
        "--output-root",
        type=Path,
        default=DEFAULT_OUTPUT_ROOT,
        help="Diretório isolado usado pelas camadas temporárias.",
    )

    parser.add_argument(
        "--keep-output",
        action="store_true",
        help="Mantém as saídas temporárias para inspeção local.",
    )

    args = parser.parse_args()
    output_root = validate_output_root(args.output_root)

    reset_output_root(output_root)

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        first_run = run_smoke_pipeline(
            spark,
            args.input,
            output_root,
        )

        second_run = run_smoke_pipeline(
            spark,
            args.input,
            output_root,
        )

        assert_idempotent(first_run, second_run)

        print("\nSmoke test end-to-end: APROVADO")
        print("Fluxo validado: CSV -> Bronze -> Silver -> Gold")
        print("Data Quality: 1 registro em quarentena")
        print("Deduplicação: versão mais recente preservada")
        print("Reexecução idempotente: APROVADA")
        print(
            json.dumps(
                second_run,
                ensure_ascii=False,
                indent=2,
            )
        )

    finally:
        spark.stop()

        if not args.keep_output and output_root.exists():
            shutil.rmtree(output_root)


if __name__ == "__main__":
    main()
