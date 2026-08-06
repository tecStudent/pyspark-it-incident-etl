from datetime import datetime

from src.silver import (
    add_data_quality,
    clean_strings,
    deduplicate,
    transform_types,
)


def test_clean_and_transform_types(spark):
    data = [
        (
            "3 - Média",
            "2025-01-01 10:00:00",
            "2025-01-01 10:30:00",
            "2025-01-01 11:00:00",
            "3600",
            "SIM",
            "NAO",
            "   ",
        )
    ]

    columns = [
        "priority",
        "opened_at",
        "resolved_at",
        "closed_at",
        "duration_seconds",
        "entered_kpi",
        "kpi_violated",
        "product",
    ]

    df = spark.createDataFrame(data, columns)

    df = clean_strings(df)
    df = transform_types(df)

    row = df.first()

    assert row.priority_code == 3
    assert row.priority_name == "Média"
    assert row.duration_seconds == 3600

    assert row.entered_kpi is True
    assert row.kpi_violated is False

    assert row.product is None

    assert isinstance(row.opened_at, datetime)


def test_kpi_not_applicable_becomes_null(spark):
    data = [
        (
            "4 - Baixa",
            "2025-01-01 10:00:00",
            None,
            "2025-01-01 11:00:00",
            "3600",
            "NAO",
            "N/A",
        )
    ]

    schema = """
        priority string,
        opened_at string,
        resolved_at string,
        closed_at string,
        duration_seconds string,
        entered_kpi string,
        kpi_violated string
    """

    df = spark.createDataFrame(
        data,
        schema=schema,
    )

    df = transform_types(df)

    row = df.first()

    assert row.entered_kpi is False
    assert row.kpi_violated is None


def test_data_quality_valid_record(spark):
    data = [
        (
            "INC1234567",
            3,
            "Team14",
            datetime(2025, 1, 1, 10, 0, 0),
            datetime(2025, 1, 1, 11, 0, 0),
            3600,
        )
    ]

    columns = [
        "incident_id",
        "priority_code",
        "assigned_group",
        "opened_at",
        "closed_at",
        "duration_seconds",
    ]

    df = spark.createDataFrame(data, columns)

    result = add_data_quality(df).first()

    assert result.dq_status == "VALID"
    assert result.dq_issues == ""


def test_data_quality_invalid_record(spark):
    data = [
        (
            "INVALID",
            9,
            None,
            datetime(2025, 1, 2, 10, 0, 0),
            datetime(2025, 1, 1, 10, 0, 0),
            -100,
        )
    ]

    schema = """
        incident_id string,
        priority_code int,
        assigned_group string,
        opened_at timestamp,
        closed_at timestamp,
        duration_seconds long
    """

    df = spark.createDataFrame(
        data,
        schema=schema,
    )

    result = add_data_quality(df).first()

    assert result.dq_status == "INVALID"

    assert "invalid_incident_id" in result.dq_issues
    assert "invalid_priority" in result.dq_issues
    assert "missing_assigned_group" in result.dq_issues
    assert "negative_duration" in result.dq_issues
    assert "closed_before_opened" in result.dq_issues


def test_deduplication_keeps_latest_incident(spark):
    data = [
        (
            "INC1234567",
            datetime(2025, 1, 1, 11, 0, 0),
            datetime(2025, 1, 1, 12, 0, 0),
            "old",
        ),
        (
            "INC1234567",
            datetime(2025, 1, 2, 11, 0, 0),
            datetime(2025, 1, 2, 12, 0, 0),
            "new",
        ),
    ]

    columns = [
        "incident_id",
        "closed_at",
        "_ingested_at",
        "version",
    ]

    df = spark.createDataFrame(data, columns)

    result = deduplicate(df)

    assert result.count() == 1

    row = result.first()

    assert row.version == "new"