import json
import math
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.dashboard_manifest import generate_dashboard_manifest
from src.forecast_gold import FORECAST_METHOD, FORECAST_METHOD_VERSION
from src.recommendation_gold import RECOMMENDATION_RULES_VERSION
from src.risk_gold import (
    AVG_DURATION_WEIGHT,
    KPI_VIOLATION_RATE_WEIGHT,
    RISK_METHODOLOGY_NAME,
    RISK_METHODOLOGY_VERSION,
    VOLUME_WEIGHT,
)


OUTPUT_DIR = Path("docs/data")
SCHEMA_VERSION = "1.0"

MONTH_NAMES = {
    1: "Janeiro",
    2: "Fevereiro",
    3: "Março",
    4: "Abril",
    5: "Maio",
    6: "Junho",
    7: "Julho",
    8: "Agosto",
    9: "Setembro",
    10: "Outubro",
    11: "Novembro",
    12: "Dezembro",
}

TABLES = {
    "monthly_kpis": {
        "source": "data/gold/monthly_kpis",
        "target": "monthly_kpis.json",
    },
    "priority_summary": {
        "source": "data/gold/priority_summary",
        "target": "priority_summary.json",
    },
    "team_summary": {
        "source": "data/gold/team_summary",
        "target": "team_summary.json",
    },
    "dashboard_summary": {
        "source": "data/gold/dashboard_summary",
        "target": "dashboard_summary.json",
    },
}

OPERATIONAL_TABLES = {
    "daily_trends": "data/gold/daily_trends",
    "risk_summary": "data/gold/risk_summary",
    "forecast_history": "data/gold/forecast_history",
    "forecast_summary": "data/gold/forecast_summary",
    "recommendations": "data/gold/recommendations",
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Dashboard Data Export")
        .getOrCreate()
    )


def generated_at_utc() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def json_compatible(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")

    if isinstance(value, date):
        return value.isoformat()

    if isinstance(value, float) and not math.isfinite(value):
        return None

    if isinstance(value, dict):
        return {
            key: json_compatible(item)
            for key, item in value.items()
        }

    if isinstance(value, (list, tuple)):
        return [json_compatible(item) for item in value]

    return value


def write_json(
    output_path: Path,
    payload: Any,
) -> None:
    output_path.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as json_file:
        json.dump(
            json_compatible(payload),
            json_file,
            ensure_ascii=False,
            indent=2,
            allow_nan=False,
        )


def base_payload(generated_at: str) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "mock": False,
    }


def sort_dataframe(
    table_name: str,
    df: DataFrame,
) -> DataFrame:
    if table_name == "monthly_kpis":
        return df.orderBy(
            "opened_year",
            "opened_month",
        )

    if table_name == "priority_summary":
        return df.orderBy("priority_code")

    if table_name == "team_summary":
        return df.orderBy(
            F.desc("total_incidents")
        )

    if table_name == "dashboard_summary":
        return df.orderBy(
            "opened_year",
            "opened_month",
            "priority_code",
            "assigned_group",
        )

    return df


def dataframe_records(df: DataFrame) -> list[dict[str, Any]]:
    return [
        json_compatible(row.asDict(recursive=True))
        for row in df.collect()
    ]


def export_table(
    spark: SparkSession,
    table_name: str,
    source: str,
    target: str,
) -> None:
    df = spark.read.parquet(source)
    df = sort_dataframe(table_name, df)
    records = dataframe_records(df)
    output_path = OUTPUT_DIR / target

    write_json(output_path, records)

    print(
        f"{table_name}: "
        f"{len(records)} registros -> "
        f"{output_path}"
    )


def distinct_strings(
    df: DataFrame,
    column_name: str,
) -> list[str]:
    value = F.trim(
        F.col(column_name).cast("string")
    )

    rows = (
        df
        .select(value.alias("value"))
        .filter(
            F.col("value").isNotNull()
            & (F.col("value") != "")
        )
        .distinct()
        .orderBy("value")
        .collect()
    )

    return [row["value"] for row in rows]


def create_filter_options_payload(
    dashboard_df: DataFrame,
    daily_trends_df: DataFrame,
    generated_at: str,
) -> dict[str, Any]:
    years = [
        row["opened_year"]
        for row in (
            dashboard_df
            .select("opened_year")
            .filter(F.col("opened_year").isNotNull())
            .distinct()
            .orderBy("opened_year")
            .collect()
        )
    ]

    month_numbers = [
        row["opened_month"]
        for row in (
            dashboard_df
            .select("opened_month")
            .filter(
                F.col("opened_month").between(1, 12)
            )
            .distinct()
            .orderBy("opened_month")
            .collect()
        )
    ]

    priorities = dataframe_records(
        dashboard_df
        .select(
            F.col("priority_code").alias("code"),
            F.col("priority_name").alias("name"),
        )
        .filter(
            F.col("code").isNotNull()
            & F.col("name").isNotNull()
        )
        .distinct()
        .orderBy("code")
    )

    payload = base_payload(generated_at)
    payload.update(
        {
            "years": years,
            "months": [
                {
                    "number": number,
                    "name": MONTH_NAMES[number],
                }
                for number in month_numbers
            ],
            "priorities": priorities,
            "products": distinct_strings(
                daily_trends_df,
                "product",
            ),
            "categories": distinct_strings(
                daily_trends_df,
                "category",
            ),
            "teams": distinct_strings(
                daily_trends_df,
                "assigned_group",
            ),
        }
    )

    return payload


def create_daily_trends_payload(
    daily_trends_df: DataFrame,
    generated_at: str,
) -> dict[str, Any]:
    records = dataframe_records(
        daily_trends_df
        .select(
            F.date_format("date", "yyyy-MM-dd").alias("date"),
            "priority_code",
            "priority_name",
            "product",
            "category",
            "assigned_group",
            "total_incidents",
            "kpi_incidents",
            "kpi_violations",
            "avg_duration_seconds",
            "p95_duration_seconds",
        )
        .orderBy(
            "date",
            "priority_code",
            "product",
            "category",
            "assigned_group",
        )
    )

    payload = base_payload(generated_at)
    payload["records"] = records
    return payload


def create_risk_summary_payload(
    risk_df: DataFrame,
    generated_at: str,
) -> dict[str, Any]:
    items = dataframe_records(
        risk_df
        .select(
            "dimension_type",
            "dimension_value",
            "is_unknown",
            "volume",
            "kpi_violation_rate_pct",
            "avg_duration_seconds",
            "risk_score",
            "rank",
        )
        .orderBy(
            "dimension_type",
            F.col("is_unknown").asc(),
            F.col("rank").asc_nulls_last(),
            "dimension_value",
        )
    )

    payload = base_payload(generated_at)
    payload.update(
        {
            "methodology": {
                "name": RISK_METHODOLOGY_NAME,
                "version": RISK_METHODOLOGY_VERSION,
                "score_scale": "0-100",
                "weights": {
                    "volume": VOLUME_WEIGHT,
                    "kpi_violation_rate": (
                        KPI_VIOLATION_RATE_WEIGHT
                    ),
                    "avg_duration": AVG_DURATION_WEIGHT,
                },
            },
            "items": items,
        }
    )

    return payload


def parse_priority_codes(value: Any) -> list[int]:
    if value is None:
        return []

    if isinstance(value, str):
        values = value.split(",")
    else:
        values = value

    return [int(item) for item in values]


def create_forecast_summary_payload(
    forecast_history_df: DataFrame,
    forecast_summary_df: DataFrame,
    generated_at: str,
) -> dict[str, Any]:
    history = dataframe_records(
        forecast_history_df
        .select(
            F.date_format("date", "yyyy-MM-dd").alias("date"),
            "actual_incidents",
        )
        .orderBy("date")
    )

    ordered_forecast = forecast_summary_df.orderBy(
        "horizon_day"
    )
    first_row = ordered_forecast.first()

    forecast = dataframe_records(
        ordered_forecast
        .select(
            F.date_format(
                "forecast_date",
                "yyyy-MM-dd",
            ).alias("date"),
            "predicted_incidents",
            "lower_bound",
            "upper_bound",
        )
    )

    payload = base_payload(generated_at)

    if first_row is None:
        payload.update(
            {
                "method": FORECAST_METHOD,
                "method_version": FORECAST_METHOD_VERSION,
                "scope": {
                    "description": None,
                    "priority_codes": [],
                    "filters_applied": [],
                },
                "forecast_d1": None,
                "forecast_d7": None,
                "risk_range": None,
                "history": history,
                "forecast": [],
            }
        )
        return payload

    payload.update(
        {
            "method": first_row["method"],
            "method_version": first_row["method_version"],
            "scope": {
                "description": first_row[
                    "scope_description"
                ],
                "priority_codes": parse_priority_codes(
                    first_row["scope_priority_codes"]
                ),
                "filters_applied": [],
            },
            "forecast_d1": first_row["forecast_d1"],
            "forecast_d7": first_row["forecast_d7"],
            "risk_range": first_row["risk_range"],
            "history": history,
            "forecast": forecast,
        }
    )

    return payload


def create_recommendations_payload(
    recommendations_df: DataFrame,
    generated_at: str,
) -> dict[str, Any]:
    severity_order = (
        F.when(F.col("severity") == "CRITICAL", 1)
        .when(F.col("severity") == "HIGH", 2)
        .when(F.col("severity") == "MEDIUM", 3)
        .otherwise(4)
    )

    items = dataframe_records(
        recommendations_df
        .select(
            "recommendation_id",
            "dimension_type",
            "target",
            "severity",
            "title",
            "recommendation",
            "evidence",
            "metric_name",
            "metric_value",
            "metric_unit",
        )
        .orderBy(
            severity_order,
            "recommendation_id",
        )
    )

    payload = base_payload(generated_at)
    payload.update(
        {
            "rules_version": RECOMMENDATION_RULES_VERSION,
            "items": items,
        }
    )

    return payload


def export_payload(
    name: str,
    payload: dict[str, Any],
    item_key: str | None = None,
) -> None:
    output_path = OUTPUT_DIR / f"{name}.json"
    write_json(output_path, payload)

    item_count = (
        len(payload[item_key])
        if item_key is not None
        else "metadados"
    )

    print(
        f"{name}: {item_count} -> {output_path}"
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    generated_at = generated_at_utc()

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    try:
        for table_name, config in TABLES.items():
            export_table(
                spark=spark,
                table_name=table_name,
                source=config["source"],
                target=config["target"],
            )

        dashboard_df = spark.read.parquet(
            TABLES["dashboard_summary"]["source"]
        )
        daily_trends_df = spark.read.parquet(
            OPERATIONAL_TABLES["daily_trends"]
        )
        risk_df = spark.read.parquet(
            OPERATIONAL_TABLES["risk_summary"]
        )
        forecast_history_df = spark.read.parquet(
            OPERATIONAL_TABLES["forecast_history"]
        )
        forecast_summary_df = spark.read.parquet(
            OPERATIONAL_TABLES["forecast_summary"]
        )
        recommendations_df = spark.read.parquet(
            OPERATIONAL_TABLES["recommendations"]
        )

        export_payload(
            "filter_options",
            create_filter_options_payload(
                dashboard_df,
                daily_trends_df,
                generated_at,
            ),
        )
        export_payload(
            "daily_trends",
            create_daily_trends_payload(
                daily_trends_df,
                generated_at,
            ),
            "records",
        )
        export_payload(
            "risk_summary",
            create_risk_summary_payload(
                risk_df,
                generated_at,
            ),
            "items",
        )
        export_payload(
            "forecast_summary",
            create_forecast_summary_payload(
                forecast_history_df,
                forecast_summary_df,
                generated_at,
            ),
            "forecast",
        )
        export_payload(
            "recommendations",
            create_recommendations_payload(
                recommendations_df,
                generated_at,
            ),
            "items",
        )

        manifest_payload = generate_dashboard_manifest(
            generated_at=generated_at,
        )
        validated_contracts = [
            file_entry["name"]
            for file_entry in manifest_payload["files"]
        ]
        print(
            "\nContratos JSON validados: "
            + ", ".join(validated_contracts)
        )
        print(
            "Manifesto publicado: "
            f"{OUTPUT_DIR / 'manifest.json'} "
            f"({manifest_payload['status']})"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
