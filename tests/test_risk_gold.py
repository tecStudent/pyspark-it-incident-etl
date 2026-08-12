from src.risk_gold import (
    AVG_DURATION_WEIGHT,
    KPI_VIOLATION_RATE_WEIGHT,
    RISK_DIMENSIONS,
    VOLUME_WEIGHT,
    create_risk_summary,
)


RISK_INPUT_SCHEMA = """
    incident_id string,
    priority_name string,
    product string,
    category string,
    assigned_group string,
    duration_seconds long,
    kpi_eligible_by_rule boolean,
    kpi_violated_by_rule boolean
"""


def sample_risk_input(spark):
    return spark.createDataFrame(
        [
            (
                "INC001",
                "Alta",
                "Hosting",
                "Availability",
                "Team01",
                100,
                True,
                True,
            ),
            (
                "INC002",
                "Alta",
                "Hosting",
                "Availability",
                "Team01",
                50,
                True,
                False,
            ),
            (
                "INC003",
                "Média",
                "Email",
                "Performance",
                "Team02",
                200,
                True,
                True,
            ),
            (
                "INC004",
                "Baixa",
                None,
                None,
                "Team03",
                400,
                False,
                None,
            ),
        ],
        schema=RISK_INPUT_SCHEMA,
    )


def result_by_dimension(spark):
    return {
        (
            row.dimension_type,
            row.dimension_value,
        ): row
        for row in create_risk_summary(
            sample_risk_input(spark)
        ).collect()
    }


def test_risk_summary_contains_all_dimensions(spark):
    result = result_by_dimension(spark)

    expected_dimensions = {
        dimension_type
        for dimension_type, _ in RISK_DIMENSIONS
    }

    actual_dimensions = {
        dimension_type
        for dimension_type, _ in result
    }

    assert actual_dimensions == expected_dimensions


def test_risk_metrics_use_calculated_kpi(spark):
    result = result_by_dimension(spark)
    hosting = result[("product", "Hosting")]

    assert hosting.volume == 2
    assert hosting.evaluated_kpi_incidents == 2
    assert hosting.kpi_violations == 1
    assert hosting.kpi_violation_rate_pct == 50.0
    assert hosting.avg_duration_seconds == 75.0


def test_weighted_score_and_rank_are_deterministic(spark):
    result = result_by_dimension(spark)
    hosting = result[("product", "Hosting")]
    email = result[("product", "Email")]

    assert hosting.volume_normalized == 1.0
    assert hosting.violation_rate_normalized == 0.5
    assert hosting.avg_duration_normalized == 0.375
    assert hosting.risk_score == 70.0
    assert hosting.rank == 2

    assert email.volume_normalized == 0.5
    assert email.violation_rate_normalized == 1.0
    assert email.avg_duration_normalized == 1.0
    assert email.risk_score == 77.5
    assert email.rank == 1


def test_unknown_dimension_is_preserved_without_rank(spark):
    result = result_by_dimension(spark)
    unknown = result[("product", None)]

    assert unknown.is_unknown is True
    assert unknown.volume == 1
    assert unknown.evaluated_kpi_incidents == 0
    assert unknown.kpi_violation_rate_pct is None
    assert unknown.violation_rate_normalized == 0.0
    assert unknown.rank is None


def test_scores_and_weights_follow_contract(spark):
    rows = create_risk_summary(
        sample_risk_input(spark)
    ).collect()

    assert VOLUME_WEIGHT == 0.45
    assert KPI_VIOLATION_RATE_WEIGHT == 0.35
    assert AVG_DURATION_WEIGHT == 0.20
    assert (
        VOLUME_WEIGHT
        + KPI_VIOLATION_RATE_WEIGHT
        + AVG_DURATION_WEIGHT
        == 1.0
    )

    for row in rows:
        assert 0.0 <= row.risk_score <= 100.0

    for dimension_type, _ in RISK_DIMENSIONS:
        ranks = sorted(
            row.rank
            for row in rows
            if (
                row.dimension_type == dimension_type
                and not row.is_unknown
            )
        )

        assert ranks == list(
            range(1, len(ranks) + 1)
        )
