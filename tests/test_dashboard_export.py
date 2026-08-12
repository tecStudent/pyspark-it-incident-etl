import json
from datetime import date

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from src.export_dashboard import (
    create_daily_trends_payload,
    create_filter_options_payload,
    create_forecast_summary_payload,
    create_recommendations_payload,
    create_risk_summary_payload,
    write_json,
)


GENERATED_AT = "2026-08-12T12:00:00Z"

DASHBOARD_SCHEMA = StructType([
    StructField("opened_year", IntegerType(), True),
    StructField("opened_month", IntegerType(), True),
    StructField("priority_code", IntegerType(), True),
    StructField("priority_name", StringType(), True),
    StructField("assigned_group", StringType(), True),
])

DAILY_SCHEMA = StructType([
    StructField("date", DateType(), True),
    StructField("priority_code", IntegerType(), True),
    StructField("priority_name", StringType(), True),
    StructField("product", StringType(), True),
    StructField("category", StringType(), True),
    StructField("assigned_group", StringType(), True),
    StructField("total_incidents", LongType(), False),
    StructField("kpi_incidents", LongType(), False),
    StructField("kpi_violations", LongType(), False),
    StructField("avg_duration_seconds", DoubleType(), True),
    StructField("p95_duration_seconds", LongType(), True),
])

RISK_SCHEMA = StructType([
    StructField("dimension_type", StringType(), False),
    StructField("dimension_value", StringType(), True),
    StructField("is_unknown", BooleanType(), False),
    StructField("volume", LongType(), False),
    StructField("kpi_violation_rate_pct", DoubleType(), True),
    StructField("avg_duration_seconds", DoubleType(), True),
    StructField("risk_score", DoubleType(), False),
    StructField("rank", IntegerType(), True),
])

FORECAST_HISTORY_SCHEMA = StructType([
    StructField("date", DateType(), False),
    StructField("actual_incidents", LongType(), False),
])

FORECAST_SCHEMA = StructType([
    StructField("forecast_date", DateType(), False),
    StructField("horizon_day", IntegerType(), False),
    StructField("predicted_incidents", LongType(), False),
    StructField("lower_bound", LongType(), False),
    StructField("upper_bound", LongType(), False),
    StructField("forecast_d1", LongType(), False),
    StructField("forecast_d7", LongType(), False),
    StructField("risk_range", LongType(), False),
    StructField("method", StringType(), False),
    StructField("method_version", StringType(), False),
    StructField("scope_description", StringType(), False),
    StructField("scope_priority_codes", StringType(), False),
])

RECOMMENDATION_SCHEMA = StructType([
    StructField("recommendation_id", StringType(), False),
    StructField("dimension_type", StringType(), False),
    StructField("target", StringType(), True),
    StructField("severity", StringType(), False),
    StructField("title", StringType(), False),
    StructField("recommendation", StringType(), False),
    StructField("evidence", StringType(), False),
    StructField("metric_name", StringType(), False),
    StructField("metric_value", DoubleType(), True),
    StructField("metric_unit", StringType(), False),
])


def assert_production_metadata(payload):
    assert payload["schema_version"] == "1.0"
    assert payload["generated_at"] == GENERATED_AT
    assert payload["mock"] is False


def test_write_json_converts_dates_and_non_finite_numbers(
    tmp_path,
):
    output_path = tmp_path / "payload.json"

    write_json(
        output_path,
        {
            "date": date(2025, 1, 2),
            "invalid_number": float("nan"),
        },
    )

    payload = json.loads(
        output_path.read_text(encoding="utf-8")
    )

    assert payload == {
        "date": "2025-01-02",
        "invalid_number": None,
    }


def test_filter_options_are_distinct_sorted_and_ignore_blanks(
    spark,
):
    dashboard_df = spark.createDataFrame(
        [
            (2025, 2, 3, "Média", "Team02"),
            (2024, 1, 2, "Alta", "Team01"),
            (2025, 1, 2, "Alta", "Team01"),
        ],
        DASHBOARD_SCHEMA,
    )
    daily_df = spark.createDataFrame(
        [
            (date(2025, 1, 1), 2, "Alta", "B", "Cat02",
             "Team02", 1, 1, 0, 10.0, 10),
            (date(2025, 1, 2), 3, "Média", "A", "Cat01",
             "Team01", 1, 1, 0, 10.0, 10),
            (date(2025, 1, 3), 3, "Média", " ", None,
             None, 1, 1, 0, 10.0, 10),
        ],
        DAILY_SCHEMA,
    )

    payload = create_filter_options_payload(
        dashboard_df,
        daily_df,
        GENERATED_AT,
    )

    assert_production_metadata(payload)
    assert payload["years"] == [2024, 2025]
    assert payload["months"] == [
        {"number": 1, "name": "Janeiro"},
        {"number": 2, "name": "Fevereiro"},
    ]
    assert payload["priorities"] == [
        {"code": 2, "name": "Alta"},
        {"code": 3, "name": "Média"},
    ]
    assert payload["products"] == ["A", "B"]
    assert payload["categories"] == ["Cat01", "Cat02"]
    assert payload["teams"] == ["Team01", "Team02"]


def test_daily_trends_follow_contract_and_iso_dates(
    spark,
):
    daily_df = spark.createDataFrame(
        [
            (date(2025, 1, 2), 2, "Alta", "Mail", "Access",
             "Team01", 8, 5, 1, 120.5, 300),
        ],
        DAILY_SCHEMA,
    )

    payload = create_daily_trends_payload(
        daily_df,
        GENERATED_AT,
    )
    record = payload["records"][0]

    assert_production_metadata(payload)
    assert record["date"] == "2025-01-02"
    assert record["total_incidents"] == 8
    assert set(record) == {
        "date",
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
    }


def test_risk_summary_includes_methodology_and_unknown_items(
    spark,
):
    risk_df = spark.createDataFrame(
        [
            ("product", "Mail", False, 100, 10.0, 300.0, 80.0, 1),
            ("product", None, True, 5, None, None, 2.0, None),
        ],
        RISK_SCHEMA,
    )

    payload = create_risk_summary_payload(
        risk_df,
        GENERATED_AT,
    )

    assert_production_metadata(payload)
    assert payload["methodology"]["weights"] == {
        "volume": 0.45,
        "kpi_violation_rate": 0.35,
        "avg_duration": 0.2,
    }
    assert payload["items"][1]["is_unknown"] is True
    assert payload["items"][1]["rank"] is None


def test_forecast_summary_combines_history_and_forecast(
    spark,
):
    history_df = spark.createDataFrame(
        [
            (date(2025, 12, 30), 70),
            (date(2025, 12, 31), 80),
        ],
        FORECAST_HISTORY_SCHEMA,
    )
    forecast_df = spark.createDataFrame(
        [
            (
                date(2026, 1, 1), 1, 90, 75, 105, 90, 650, 15,
                "weighted_explainable_baseline", "1.0",
                "Incidentes P1, P2 e P3", "1,2,3",
            ),
            (
                date(2026, 1, 2), 2, 95, 80, 110, 90, 650, 15,
                "weighted_explainable_baseline", "1.0",
                "Incidentes P1, P2 e P3", "1,2,3",
            ),
        ],
        FORECAST_SCHEMA,
    )

    payload = create_forecast_summary_payload(
        history_df,
        forecast_df,
        GENERATED_AT,
    )

    assert_production_metadata(payload)
    assert payload["scope"]["priority_codes"] == [1, 2, 3]
    assert payload["forecast_d1"] == 90
    assert payload["forecast_d7"] == 650
    assert payload["history"][0] == {
        "date": "2025-12-30",
        "actual_incidents": 70,
    }
    assert payload["forecast"][0]["date"] == "2026-01-01"


def test_empty_forecast_preserves_valid_contract(
    spark,
):
    history_df = spark.createDataFrame(
        [],
        FORECAST_HISTORY_SCHEMA,
    )
    forecast_df = spark.createDataFrame(
        [],
        FORECAST_SCHEMA,
    )

    payload = create_forecast_summary_payload(
        history_df,
        forecast_df,
        GENERATED_AT,
    )

    assert payload["forecast_d1"] is None
    assert payload["history"] == []
    assert payload["forecast"] == []
    assert payload["scope"]["priority_codes"] == []


def test_recommendations_follow_severity_order_and_contract(
    spark,
):
    recommendations_df = spark.createDataFrame(
        [
            (
                "REC-002", "product", "Mail", "MEDIUM", "Title 2",
                "Action 2", "Evidence 2", "volume", 100.0,
                "incidents",
            ),
            (
                "REC-001", "category", "Access", "CRITICAL", "Title 1",
                "Action 1", "Evidence 1", "risk_score", 90.0,
                "score",
            ),
        ],
        RECOMMENDATION_SCHEMA,
    )

    payload = create_recommendations_payload(
        recommendations_df,
        GENERATED_AT,
    )

    assert_production_metadata(payload)
    assert payload["rules_version"] == "1.0"
    assert [item["recommendation_id"] for item in payload["items"]] == [
        "REC-001",
        "REC-002",
    ]
    assert set(payload["items"][0]) == {
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
    }
