from pyspark.sql.types import StringType, StructField, StructType

from src.kpi_rules import (
    KPI_RULE_VERSION,
    add_kpi_audit_columns,
)
from src.silver import transform_records


AUDIT_SCHEMA = """
    case_id string,
    priority_code int,
    parent_incident_id string,
    status string,
    duration_seconds long,
    entered_kpi boolean,
    kpi_violated boolean
"""


TRANSFORM_INPUT_SCHEMA = StructType([
    StructField("Número", StringType(), True),
    StructField("Prioridade", StringType(), True),
    StructField("Grupo designado", StringType(), True),
    StructField("Aberto", StringType(), True),
    StructField("Resolvido", StringType(), True),
    StructField("Encerrado", StringType(), True),
    StructField("Duração", StringType(), True),
    StructField("Incidente Pai", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Entrou para KPI?", StringType(), True),
    StructField("KPI Violado?", StringType(), True),
])


def create_audit_result(
    spark,
    data,
):
    df = spark.createDataFrame(
        data,
        schema=AUDIT_SCHEMA,
    )

    return {
        row.case_id: row
        for row in (
            add_kpi_audit_columns(df)
            .collect()
        )
    }


def test_kpi_limits_and_boundary(spark):
    rows = create_audit_result(
        spark,
        [
            (
                "p1_at_limit",
                1,
                None,
                "Encerrado",
                14400,
                True,
                False,
            ),
            (
                "p2_over_limit",
                2,
                None,
                "Encerrado",
                14401,
                True,
                True,
            ),
            (
                "p3_at_limit",
                3,
                None,
                "Encerrado",
                43200,
                True,
                False,
            ),
            (
                "p3_over_limit",
                3,
                None,
                "Encerrado",
                43201,
                True,
                True,
            ),
        ],
    )

    assert (
        rows["p1_at_limit"]
        .kpi_limit_seconds
        == 14400
    )

    assert (
        rows["p1_at_limit"]
        .kpi_violated_by_rule
        is False
    )

    assert (
        rows["p2_over_limit"]
        .kpi_limit_seconds
        == 14400
    )

    assert (
        rows["p2_over_limit"]
        .kpi_violated_by_rule
        is True
    )

    assert (
        rows["p3_at_limit"]
        .kpi_limit_seconds
        == 43200
    )

    assert (
        rows["p3_at_limit"]
        .kpi_violated_by_rule
        is False
    )

    assert (
        rows["p3_over_limit"]
        .kpi_violated_by_rule
        is True
    )


def test_kpi_exclusion_rules(spark):
    rows = create_audit_result(
        spark,
        [
            (
                "priority_4",
                4,
                None,
                "Encerrado",
                90000,
                False,
                None,
            ),
            (
                "priority_5",
                5,
                None,
                "Encerrado",
                350000,
                False,
                None,
            ),
            (
                "parent_incident",
                2,
                "INC1234567",
                "Encerrado",
                20000,
                False,
                None,
            ),
            (
                "without_intervention",
                2,
                None,
                "  sem   intervenção  ",
                20000,
                False,
                None,
            ),
        ],
    )

    assert (
        rows["priority_4"]
        .kpi_limit_seconds
        == 86400
    )
    assert (
        rows["priority_4"]
        .kpi_eligible_by_rule
        is False
    )
    assert (
        rows["priority_4"]
        .kpi_violated_by_rule
        is None
    )
    assert (
        rows["priority_4"]
        .kpi_rule_reason
        == "priority_not_eligible"
    )

    assert (
        rows["priority_5"]
        .kpi_limit_seconds
        == 345600
    )
    assert (
        rows["priority_5"]
        .kpi_eligible_by_rule
        is False
    )

    assert (
        rows["parent_incident"]
        .kpi_rule_reason
        == "parent_incident"
    )
    assert (
        rows["parent_incident"]
        .kpi_eligible_by_rule
        is False
    )

    assert (
        rows["without_intervention"]
        .kpi_rule_reason
        == "status_without_intervention"
    )
    assert (
        rows["without_intervention"]
        .kpi_eligible_by_rule
        is False
    )


def test_missing_and_invalid_values(spark):
    rows = create_audit_result(
        spark,
        [
            (
                "missing_priority",
                None,
                None,
                "Encerrado",
                100,
                None,
                None,
            ),
            (
                "invalid_priority",
                9,
                None,
                "Encerrado",
                100,
                None,
                None,
            ),
            (
                "missing_duration",
                2,
                None,
                "Encerrado",
                None,
                True,
                None,
            ),
        ],
    )

    assert (
        rows["missing_priority"]
        .kpi_eligible_by_rule
        is None
    )
    assert (
        rows["missing_priority"]
        .kpi_rule_reason
        == "invalid_priority"
    )

    assert (
        rows["invalid_priority"]
        .kpi_limit_seconds
        is None
    )
    assert (
        rows["invalid_priority"]
        .kpi_eligible_by_rule
        is None
    )

    assert (
        rows["missing_duration"]
        .kpi_eligible_by_rule
        is True
    )
    assert (
        rows["missing_duration"]
        .kpi_violated_by_rule
        is None
    )
    assert (
        rows["missing_duration"]
        .kpi_rule_reason
        == "missing_duration"
    )


def test_source_flags_are_preserved_and_compared(
    spark,
):
    rows = create_audit_result(
        spark,
        [
            (
                "matches",
                3,
                None,
                "Encerrado",
                1000,
                True,
                False,
            ),
            (
                "diverges",
                2,
                None,
                "Encerrado",
                14401,
                False,
                False,
            ),
            (
                "source_null",
                2,
                None,
                "Encerrado",
                1000,
                None,
                None,
            ),
        ],
    )

    assert rows["matches"].entered_kpi is True
    assert rows["matches"].kpi_violated is False
    assert (
        rows["matches"]
        .entered_kpi_rule_matches_source
        is True
    )
    assert (
        rows["matches"]
        .kpi_violated_rule_matches_source
        is True
    )

    assert rows["diverges"].entered_kpi is False
    assert rows["diverges"].kpi_violated is False
    assert (
        rows["diverges"]
        .entered_kpi_rule_matches_source
        is False
    )
    assert (
        rows["diverges"]
        .kpi_violated_rule_matches_source
        is False
    )

    assert (
        rows["source_null"]
        .entered_kpi_rule_matches_source
        is None
    )
    assert (
        rows["source_null"]
        .kpi_violated_rule_matches_source
        is None
    )


def test_transform_records_adds_kpi_audit_columns(
    spark,
):
    data = [
        (
            "INC1234567",
            "2 - Alta",
            "Team01",
            "2025-01-01 10:00:00",
            "2025-01-01 14:00:01",
            "2025-01-01 14:00:01",
            "14401",
            None,
            "Encerrado",
            "SIM",
            "NAO",
        )
    ]

    bronze_df = spark.createDataFrame(
        data,
        schema=TRANSFORM_INPUT_SCHEMA,
    )

    row = transform_records(
        bronze_df
    ).first()

    assert row.kpi_rule_version == KPI_RULE_VERSION
    assert row.kpi_limit_seconds == 14400
    assert row.kpi_eligible_by_rule is True
    assert row.kpi_violated_by_rule is True
    assert row.entered_kpi is True
    assert row.kpi_violated is False
    assert (
        row.entered_kpi_rule_matches_source
        is True
    )
    assert (
        row.kpi_violated_rule_matches_source
        is False
    )
