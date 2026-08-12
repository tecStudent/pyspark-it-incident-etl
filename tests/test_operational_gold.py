from datetime import datetime

from src.operational_gold import (
    add_annual_attainment,
    create_annual_ola_summary,
    create_daily_trends,
    create_operational_kpi_summary,
)


SILVER_SCHEMA = """
    incident_id string,
    opened_at timestamp,
    opened_year int,
    opened_month int,
    priority_code int,
    priority_name string,
    assigned_group string,
    product string,
    category string,
    duration_seconds long,
    entered_kpi boolean,
    kpi_violated boolean,
    kpi_eligible_by_rule boolean,
    kpi_violated_by_rule boolean,
    entered_kpi_rule_matches_source boolean,
    kpi_violated_rule_matches_source boolean
"""


def sample_silver_df(spark):
    return spark.createDataFrame(
        [
            (
                "INC001",
                datetime(2025, 1, 10, 10, 0),
                2025,
                1,
                2,
                "Alta",
                "Team01",
                "Hosting",
                "Availability",
                20000,
                True,
                False,
                True,
                True,
                True,
                False,
            ),
            (
                "INC002",
                datetime(2025, 1, 10, 11, 0),
                2025,
                1,
                2,
                "Alta",
                "Team01",
                "Hosting",
                "Availability",
                10000,
                True,
                False,
                True,
                False,
                True,
                True,
            ),
            (
                "INC003",
                datetime(2025, 1, 11, 12, 0),
                2025,
                1,
                3,
                "Média",
                "Team02",
                "Email",
                "Performance",
                None,
                True,
                None,
                True,
                None,
                True,
                None,
            ),
            (
                "INC004",
                datetime(2025, 1, 11, 13, 0),
                2025,
                1,
                4,
                "Baixa",
                "Team03",
                "Email",
                "Request",
                5000,
                False,
                False,
                False,
                None,
                True,
                None,
            ),
        ],
        schema=SILVER_SCHEMA,
    )


def test_daily_trends_use_calculated_kpi_rules(spark):
    rows = (
        create_daily_trends(sample_silver_df(spark))
        .collect()
    )

    p2 = next(
        row
        for row in rows
        if row.priority_code == 2
    )

    assert p2.date.isoformat() == "2025-01-10"
    assert p2.total_incidents == 2
    assert p2.kpi_incidents == 2
    assert p2.evaluated_kpi_incidents == 2
    assert p2.kpi_violations == 1
    assert p2.kpi_compliant == 1
    assert p2.kpi_compliance_pct == 50.0
    assert p2.avg_duration_seconds == 15000.0
    assert p2.p95_duration_seconds == 20000


def test_operational_summary_exposes_source_mismatch(spark):
    rows = (
        create_operational_kpi_summary(
            sample_silver_df(spark)
        )
        .collect()
    )

    p2 = next(
        row
        for row in rows
        if row.priority_code == 2
    )

    assert p2.source_kpi_incidents == 2
    assert p2.source_kpi_violations == 0
    assert p2.calculated_kpi_incidents == 2
    assert p2.calculated_kpi_violations == 1
    assert p2.calculated_kpi_compliant == 1
    assert p2.calculated_compliance_pct == 50.0
    assert p2.eligibility_comparisons == 2
    assert p2.eligibility_mismatches == 0
    assert p2.eligibility_mismatch_pct == 0.0
    assert p2.violation_comparisons == 2
    assert p2.violation_mismatches == 1
    assert p2.violation_mismatch_pct == 50.0


def test_not_evaluable_incident_is_not_compliant(spark):
    rows = (
        create_operational_kpi_summary(
            sample_silver_df(spark)
        )
        .collect()
    )

    p3 = next(
        row
        for row in rows
        if row.priority_code == 3
    )

    assert p3.calculated_kpi_incidents == 1
    assert p3.evaluated_kpi_incidents == 0
    assert p3.not_evaluable_kpi_incidents == 1
    assert p3.calculated_kpi_compliant == 0
    assert p3.calculated_compliance_pct is None
    assert p3.violation_comparisons == 0
    assert p3.violation_mismatch_pct is None


def test_annual_ola_summary_uses_only_p2_and_p3(spark):
    rows = {
        row.priority_code: row
        for row in create_annual_ola_summary(
            sample_silver_df(spark)
        ).collect()
    }

    assert set(rows) == {2, 3}
    assert rows[2].treated_volume == 2
    assert rows[2].calculated_kpi_violations == 1
    assert rows[2].violation_target_attainment_pct == 150
    assert rows[2].volume_target_attainment_pct == 150
    assert rows[3].treated_volume == 1
    assert rows[3].evaluated_kpi_incidents == 0
    assert rows[3].calculated_compliance_pct is None


def test_annual_attainment_boundaries(spark):
    result = {
        row.case_id: row
        for row in add_annual_attainment(
            spark.createDataFrame(
                [
                    ("p2_start_125", 2, 31, 4585),
                    ("p2_above_target", 2, 54, 6337),
                    ("p3_score_100", 3, 263, 22524),
                    ("p3_start_125", 3, 201, 19489),
                ],
                """
                    case_id string,
                    priority_code int,
                    calculated_kpi_violations long,
                    treated_volume long
                """,
            )
        ).collect()
    }

    assert (
        result["p2_start_125"]
        .violation_target_attainment_pct
        == 125
    )
    assert (
        result["p2_start_125"]
        .volume_target_attainment_pct
        == 125
    )
    assert (
        result["p2_above_target"]
        .violation_target_attainment_pct
        == 0
    )
    assert (
        result["p2_above_target"]
        .volume_target_attainment_pct
        == 0
    )
    assert (
        result["p3_score_100"]
        .violation_target_attainment_pct
        == 100
    )
    assert (
        result["p3_score_100"]
        .volume_target_attainment_pct
        == 100
    )
    assert (
        result["p3_start_125"]
        .violation_target_attainment_pct
        == 125
    )
    assert (
        result["p3_start_125"]
        .volume_target_attainment_pct
        == 125
    )
