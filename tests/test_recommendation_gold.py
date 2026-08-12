from datetime import date, timedelta

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

from src.recommendation_gold import (
    OUTPUT_COLUMNS,
    create_compliance_recommendations,
    create_forecast_recommendations,
    create_risk_score_recommendations,
    create_violation_rate_recommendations,
    create_volume_concentration_recommendations,
)


RISK_SCHEMA = StructType([
    StructField("dimension_type", StringType(), False),
    StructField("dimension_value", StringType(), True),
    StructField("is_unknown", BooleanType(), False),
    StructField("volume", LongType(), False),
    StructField("evaluated_kpi_incidents", LongType(), False),
    StructField("kpi_violations", LongType(), False),
    StructField("kpi_violation_rate_pct", DoubleType(), True),
    StructField("avg_duration_seconds", DoubleType(), True),
    StructField("risk_score", DoubleType(), False),
    StructField("rank", IntegerType(), True),
])

ANNUAL_OLA_SCHEMA = StructType([
    StructField("opened_year", IntegerType(), False),
    StructField("priority_code", IntegerType(), False),
    StructField("priority_name", StringType(), False),
    StructField("calculated_compliance_pct", DoubleType(), True),
])

FORECAST_SUMMARY_SCHEMA = StructType([
    StructField("horizon_day", IntegerType(), False),
    StructField("forecast_d7", LongType(), False),
])

FORECAST_HISTORY_SCHEMA = StructType([
    StructField("date", DateType(), False),
    StructField("history_end_date", DateType(), False),
    StructField("actual_incidents", LongType(), False),
])


def risk_row(
    dimension_type="assigned_group",
    dimension_value="Team01",
    is_unknown=False,
    volume=100,
    evaluated=20,
    violations=5,
    violation_rate=25.0,
    duration=3600.0,
    risk_score=60.0,
    rank=1,
):
    return (
        dimension_type,
        dimension_value,
        is_unknown,
        volume,
        evaluated,
        violations,
        violation_rate,
        duration,
        risk_score,
        rank,
    )


def test_risk_recommendation_has_stable_id_and_contract(
    spark,
):
    risk_df = spark.createDataFrame(
        [risk_row(risk_score=80.0)],
        RISK_SCHEMA,
    )

    first = create_risk_score_recommendations(
        risk_df
    ).collect()[0]
    second = create_risk_score_recommendations(
        risk_df
    ).collect()[0]

    assert first.recommendation_id == second.recommendation_id
    assert first.recommendation_id.startswith("REC-")
    assert first.severity == "CRITICAL"
    assert first.rule_id == "RISK_SCORE_HIGH"
    assert first.asDict().keys() == set(OUTPUT_COLUMNS)


def test_violation_rule_requires_minimum_sample_and_keeps_top_three(
    spark,
):
    risk_df = spark.createDataFrame(
        [
            risk_row("product", "Small", volume=10, evaluated=4,
                     violations=4, violation_rate=100.0),
            risk_row("product", "A", evaluated=10, violations=8,
                     violation_rate=80.0),
            risk_row("product", "B", evaluated=10, violations=7,
                     violation_rate=70.0),
            risk_row("product", "C", evaluated=10, violations=6,
                     violation_rate=60.0),
            risk_row("product", "D", evaluated=10, violations=5,
                     violation_rate=50.0),
        ],
        RISK_SCHEMA,
    )

    rows = create_violation_rate_recommendations(
        risk_df
    ).collect()

    assert {row.target for row in rows} == {"A", "B", "C"}
    assert all(row.severity == "CRITICAL" for row in rows)


def test_volume_concentration_uses_volume_instead_of_risk_rank(
    spark,
):
    risk_df = spark.createDataFrame(
        [
            risk_row("product", "HighestRisk", volume=1200,
                     risk_score=90.0, rank=1),
            risk_row("product", "HighestVolume", volume=2500,
                     risk_score=60.0, rank=2),
            risk_row("category", "TooSmall", volume=999,
                     risk_score=90.0, rank=1),
        ],
        RISK_SCHEMA,
    )

    rows = create_volume_concentration_recommendations(
        risk_df
    ).collect()

    assert len(rows) == 1
    assert rows[0].target == "HighestVolume"
    assert rows[0].metric_value == 2500.0


def test_annual_compliance_recommendations_classify_severity(
    spark,
):
    annual_df = spark.createDataFrame(
        [
            (2025, 2, "2 - Alta", 70.0),
            (2025, 3, "3 - Média", 85.0),
            (2024, 2, "2 - Alta", 95.0),
        ],
        ANNUAL_OLA_SCHEMA,
    )

    rows = create_compliance_recommendations(
        annual_df
    ).collect()
    severity_by_target = {
        row.target: row.severity
        for row in rows
    }

    assert severity_by_target == {
        "2025 | 2 - Alta": "CRITICAL",
        "2025 | 3 - Média": "HIGH",
    }


def test_forecast_recommendation_compares_next_week_with_recent_week(
    spark,
):
    history_end = date(2025, 12, 31)
    history = [
        (
            history_end - timedelta(days=offset),
            history_end,
            10,
        )
        for offset in range(7)
    ]

    forecast_df = spark.createDataFrame(
        [(1, 140)],
        FORECAST_SUMMARY_SCHEMA,
    )
    history_df = spark.createDataFrame(
        history,
        FORECAST_HISTORY_SCHEMA,
    )

    row = create_forecast_recommendations(
        forecast_df,
        history_df,
    ).collect()[0]

    assert row.target == "next_7_days"
    assert row.severity == "HIGH"
    assert row.metric_name == "forecast_growth_pct"
    assert row.metric_value == 100.0
