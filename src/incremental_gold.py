import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import SparkSession

from src.forecast_gold import (
    create_forecast_history,
    create_forecast_summary,
)
from src.gold import (
    create_dashboard_summary,
    create_monthly_kpis,
    create_priority_summary,
    create_team_summary,
    write_gold,
)
from src.operational_gold import (
    create_annual_ola_summary,
    create_daily_trends,
    create_operational_kpi_summary,
)
from src.recommendation_gold import create_recommendations
from src.risk_gold import create_risk_summary


SILVER_INPUT = Path(
    "data/silver/incremental_incidents"
)

SILVER_CONTROL_PATH = Path(
    "data/control/silver_batches.json"
)

GOLD_CONTROL_PATH = Path(
    "data/control/gold_batches.json"
)

MONTHLY_OUTPUT = (
    "data/gold/incremental/monthly_kpis"
)

PRIORITY_OUTPUT = (
    "data/gold/incremental/priority_summary"
)

TEAM_OUTPUT = (
    "data/gold/incremental/team_summary"
)

DASHBOARD_OUTPUT = (
    "data/gold/incremental/dashboard_summary"
)

DAILY_TRENDS_OUTPUT = (
    "data/gold/incremental/daily_trends"
)

OPERATIONAL_KPI_OUTPUT = (
    "data/gold/incremental/operational_kpi_summary"
)

ANNUAL_OLA_OUTPUT = (
    "data/gold/incremental/annual_ola_summary"
)

RISK_OUTPUT = (
    "data/gold/incremental/risk_summary"
)

FORECAST_HISTORY_OUTPUT = (
    "data/gold/incremental/forecast_history"
)

FORECAST_SUMMARY_OUTPUT = (
    "data/gold/incremental/forecast_summary"
)

RECOMMENDATIONS_OUTPUT = (
    "data/gold/incremental/recommendations"
)


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IT Incident Incremental Gold")
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


def successful_silver_batches(
    control: dict[str, Any],
) -> list[dict[str, Any]]:
    batches_by_id = {}

    for batch in control["batches"]:
        if batch.get("status") != "SUCCESS":
            continue

        batches_by_id[
            batch["batch_id"]
        ] = batch

    return sorted(
        batches_by_id.values(),
        key=lambda batch: batch["processed_at"],
    )


def processed_gold_batch_ids(
    control: dict[str, Any],
) -> set[str]:
    return {
        batch["batch_id"]
        for batch in control["batches"]
        if batch.get("status") == "SUCCESS"
    }


def register_gold_batches(
    gold_control: dict[str, Any],
    pending_batches: list[dict[str, Any]],
    silver_count: int,
) -> None:
    processed_at = datetime.now(
        timezone.utc
    ).isoformat()

    for batch in pending_batches:
        gold_control["batches"].append(
            {
                "batch_id": batch["batch_id"],
                "source_file": batch["source_file"],
                "file_hash": batch["file_hash"],
                "status": "SUCCESS",
                "gold_snapshot_records": (
                    silver_count
                ),
                "processed_at": processed_at,
            }
        )

    save_control(
        GOLD_CONTROL_PATH,
        gold_control,
    )


def main() -> None:
    if not SILVER_CONTROL_PATH.exists():
        raise FileNotFoundError(
            "Controle da Silver não encontrado: "
            f"{SILVER_CONTROL_PATH}"
        )

    if not SILVER_INPUT.exists():
        raise FileNotFoundError(
            "Silver incremental não encontrada: "
            f"{SILVER_INPUT}"
        )

    silver_control = load_control(
        SILVER_CONTROL_PATH
    )

    gold_control = load_control(
        GOLD_CONTROL_PATH
    )

    silver_batches = (
        successful_silver_batches(
            silver_control
        )
    )

    processed_batch_ids = (
        processed_gold_batch_ids(
            gold_control
        )
    )

    pending_batches = []

    for batch in silver_batches:
        if batch["batch_id"] in processed_batch_ids:
            print(
                "Batch já aplicado na Gold: "
                f"{batch['source_file']}"
            )
            continue

        pending_batches.append(batch)

    if not pending_batches:
        print(
            "Nenhum batch novo para aplicar "
            "na Gold."
        )
        return

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    silver_df = None

    try:
        silver_df = (
            spark.read
            .parquet(str(SILVER_INPUT))
            .cache()
        )

        silver_count = silver_df.count()

        print(
            "Registros da Silver incremental: "
            f"{silver_count}"
        )

        print(
            f"Batches novos para a Gold: "
            f"{len(pending_batches)}"
        )

        monthly_df = create_monthly_kpis(
            silver_df
        )

        priority_df = create_priority_summary(
            silver_df
        )

        team_df = create_team_summary(
            silver_df
        )

        dashboard_df = (
            create_dashboard_summary(
                silver_df
            )
        )

        daily_trends_df = create_daily_trends(
            silver_df
        )

        operational_kpi_df = (
            create_operational_kpi_summary(
                silver_df
            )
        )

        annual_ola_df = create_annual_ola_summary(
            silver_df
        )

        risk_df = create_risk_summary(silver_df)

        forecast_history_df = create_forecast_history(
            silver_df
        )

        forecast_summary_df = create_forecast_summary(
            silver_df
        )

        recommendations_df = create_recommendations(
            risk_df,
            annual_ola_df,
            forecast_summary_df,
            forecast_history_df,
        )

        write_gold(
            spark,
            monthly_df,
            MONTHLY_OUTPUT,
            "incremental_monthly_kpis",
        )

        write_gold(
            spark,
            priority_df,
            PRIORITY_OUTPUT,
            "incremental_priority_summary",
        )

        write_gold(
            spark,
            team_df,
            TEAM_OUTPUT,
            "incremental_team_summary",
        )

        write_gold(
            spark,
            dashboard_df,
            DASHBOARD_OUTPUT,
            "incremental_dashboard_summary",
        )

        write_gold(
            spark,
            daily_trends_df,
            DAILY_TRENDS_OUTPUT,
            "incremental_daily_trends",
        )

        write_gold(
            spark,
            operational_kpi_df,
            OPERATIONAL_KPI_OUTPUT,
            "incremental_operational_kpi_summary",
        )

        write_gold(
            spark,
            annual_ola_df,
            ANNUAL_OLA_OUTPUT,
            "incremental_annual_ola_summary",
        )

        write_gold(
            spark,
            risk_df,
            RISK_OUTPUT,
            "incremental_risk_summary",
        )

        write_gold(
            spark,
            forecast_history_df,
            FORECAST_HISTORY_OUTPUT,
            "incremental_forecast_history",
        )

        write_gold(
            spark,
            forecast_summary_df,
            FORECAST_SUMMARY_OUTPUT,
            "incremental_forecast_summary",
        )

        write_gold(
            spark,
            recommendations_df,
            RECOMMENDATIONS_OUTPUT,
            "incremental_recommendations",
        )

        register_gold_batches(
            gold_control=gold_control,
            pending_batches=pending_batches,
            silver_count=silver_count,
        )

        print(
            "Validação Gold incremental: OK"
        )

    finally:
        if silver_df is not None:
            silver_df.unpersist()

        spark.stop()


if __name__ == "__main__":
    main()
