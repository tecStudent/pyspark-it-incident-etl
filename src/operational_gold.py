from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def conditional_count(condition: Column) -> Column:
    return F.sum(
        F.when(condition, 1).otherwise(0)
    )


def add_percentage(
    df: DataFrame,
    output_column: str,
    numerator_column: str,
    denominator_column: str,
) -> DataFrame:
    return df.withColumn(
        output_column,
        F.when(
            F.col(denominator_column) > 0,
            F.round(
                (
                    F.col(numerator_column)
                    / F.col(denominator_column)
                )
                * 100,
                2,
            ),
        ),
    )


def add_operational_rates(df: DataFrame) -> DataFrame:
    result = add_percentage(
        df,
        "calculated_compliance_pct",
        "calculated_kpi_compliant",
        "evaluated_kpi_incidents",
    )

    result = add_percentage(
        result,
        "eligibility_mismatch_pct",
        "eligibility_mismatches",
        "eligibility_comparisons",
    )

    return add_percentage(
        result,
        "violation_mismatch_pct",
        "violation_mismatches",
        "violation_comparisons",
    )


def operational_aggregations() -> list[Column]:
    return [
        F.count("*").alias("total_incidents"),
        conditional_count(
            F.col("entered_kpi") == True
        ).alias("source_kpi_incidents"),
        conditional_count(
            F.col("kpi_violated") == True
        ).alias("source_kpi_violations"),
        conditional_count(
            F.col("kpi_eligible_by_rule") == True
        ).alias("calculated_kpi_incidents"),
        conditional_count(
            (
                F.col("kpi_eligible_by_rule") == True
            )
            & F.col(
                "kpi_violated_by_rule"
            ).isNotNull()
        ).alias("evaluated_kpi_incidents"),
        conditional_count(
            F.col("kpi_violated_by_rule") == True
        ).alias("calculated_kpi_violations"),
        conditional_count(
            F.col("kpi_violated_by_rule") == False
        ).alias("calculated_kpi_compliant"),
        conditional_count(
            (
                F.col("kpi_eligible_by_rule") == True
            )
            & F.col(
                "kpi_violated_by_rule"
            ).isNull()
        ).alias("not_evaluable_kpi_incidents"),
        conditional_count(
            F.col(
                "entered_kpi_rule_matches_source"
            ).isNotNull()
        ).alias("eligibility_comparisons"),
        conditional_count(
            F.col(
                "entered_kpi_rule_matches_source"
            ) == False
        ).alias("eligibility_mismatches"),
        conditional_count(
            F.col(
                "kpi_violated_rule_matches_source"
            ).isNotNull()
        ).alias("violation_comparisons"),
        conditional_count(
            F.col(
                "kpi_violated_rule_matches_source"
            ) == False
        ).alias("violation_mismatches"),
        F.round(
            F.avg("duration_seconds"),
            2,
        ).alias("avg_duration_seconds"),
        F.percentile_approx(
            "duration_seconds",
            0.95,
        ).alias("p95_duration_seconds"),
    ]


def create_operational_kpi_summary(
    df: DataFrame,
) -> DataFrame:
    result = (
        df
        .groupBy(
            "opened_year",
            "opened_month",
            "priority_code",
            "priority_name",
            "assigned_group",
        )
        .agg(*operational_aggregations())
    )

    return add_operational_rates(result)


def create_daily_trends(df: DataFrame) -> DataFrame:
    result = (
        df
        .withColumn(
            "date",
            F.to_date("opened_at"),
        )
        .groupBy(
            "date",
            "priority_code",
            "priority_name",
            "product",
            "category",
            "assigned_group",
        )
        .agg(
            F.count("*").alias("total_incidents"),
            conditional_count(
                F.col("kpi_eligible_by_rule") == True
            ).alias("kpi_incidents"),
            conditional_count(
                (
                    F.col("kpi_eligible_by_rule") == True
                )
                & F.col(
                    "kpi_violated_by_rule"
                ).isNotNull()
            ).alias("evaluated_kpi_incidents"),
            conditional_count(
                F.col("kpi_violated_by_rule") == True
            ).alias("kpi_violations"),
            conditional_count(
                F.col("kpi_violated_by_rule") == False
            ).alias("kpi_compliant"),
            F.round(
                F.avg("duration_seconds"),
                2,
            ).alias("avg_duration_seconds"),
            F.percentile_approx(
                "duration_seconds",
                0.95,
            ).alias("p95_duration_seconds"),
        )
    )

    return add_percentage(
        result,
        "kpi_compliance_pct",
        "kpi_compliant",
        "evaluated_kpi_incidents",
    )


def attainment_score(
    metric: Column,
    first_limit: int,
    score_125_limit: int,
    score_100_limit: int,
    score_75_limit: int,
    score_50_limit: int,
) -> Column:
    return (
        F.when(metric < first_limit, 150)
        .when(metric <= score_125_limit, 125)
        .when(metric <= score_100_limit, 100)
        .when(metric <= score_75_limit, 75)
        .when(metric <= score_50_limit, 50)
        .otherwise(0)
    )


def add_annual_attainment(df: DataFrame) -> DataFrame:
    priority = F.col("priority_code")
    violations = F.col("calculated_kpi_violations")
    volume = F.col("treated_volume")

    violation_attainment = (
        F.when(
            priority == 2,
            attainment_score(
                violations,
                31,
                35,
                39,
                45,
                53,
            ),
        )
        .when(
            priority == 3,
            attainment_score(
                violations,
                201,
                230,
                263,
                290,
                320,
            ),
        )
    )

    volume_attainment = (
        F.when(
            priority == 2,
            attainment_score(
                volume,
                4585,
                5388,
                6168,
                6252,
                6336,
            ),
        )
        .when(
            priority == 3,
            attainment_score(
                volume,
                19489,
                22116,
                22524,
                23892,
                24276,
            ),
        )
    )

    return (
        df
        .withColumn(
            "violation_target_attainment_pct",
            violation_attainment,
        )
        .withColumn(
            "volume_target_attainment_pct",
            volume_attainment,
        )
    )


def create_annual_ola_summary(
    df: DataFrame,
) -> DataFrame:
    result = (
        df
        .filter(F.col("priority_code").isin(2, 3))
        .groupBy(
            "opened_year",
            "priority_code",
            "priority_name",
        )
        .agg(
            F.count("*").alias("treated_volume"),
            conditional_count(
                F.col("kpi_eligible_by_rule") == True
            ).alias("calculated_kpi_incidents"),
            conditional_count(
                (
                    F.col("kpi_eligible_by_rule") == True
                )
                & F.col(
                    "kpi_violated_by_rule"
                ).isNotNull()
            ).alias("evaluated_kpi_incidents"),
            conditional_count(
                F.col("kpi_violated_by_rule") == True
            ).alias("calculated_kpi_violations"),
            conditional_count(
                F.col("kpi_violated_by_rule") == False
            ).alias("calculated_kpi_compliant"),
        )
    )

    result = add_percentage(
        result,
        "calculated_compliance_pct",
        "calculated_kpi_compliant",
        "evaluated_kpi_incidents",
    )

    return add_annual_attainment(result)
