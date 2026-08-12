from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


KPI_RULE_VERSION = "1.0"

KPI_ELIGIBLE_PRIORITIES = (
    1,
    2,
    3,
)

KPI_LIMITS_SECONDS = {
    1: 4 * 60 * 60,
    2: 4 * 60 * 60,
    3: 12 * 60 * 60,
    4: 24 * 60 * 60,
    5: 96 * 60 * 60,
}

ACCENTED_CHARACTERS = (
    "ÁÀÃÂÄÉÈÊËÍÌÎÏÓÒÕÔÖÚÙÛÜÇ"
)

ASCII_CHARACTERS = (
    "AAAAAEEEEIIIIOOOOOUUUUC"
)


def normalize_status(column: Column) -> Column:
    return F.regexp_replace(
        F.translate(
            F.upper(
                F.trim(column)
            ),
            ACCENTED_CHARACTERS,
            ASCII_CHARACTERS,
        ),
        r"\s+",
        " ",
    )


def create_kpi_limit_expression() -> Column:
    expression = None

    for priority_code, limit_seconds in (
        KPI_LIMITS_SECONDS.items()
    ):
        if expression is None:
            expression = F.when(
                F.col("priority_code")
                == priority_code,
                F.lit(limit_seconds),
            )
        else:
            expression = expression.when(
                F.col("priority_code")
                == priority_code,
                F.lit(limit_seconds),
            )

    return expression.cast("long")


def add_kpi_audit_columns(
    df: DataFrame,
) -> DataFrame:
    priority = F.col("priority_code")
    parent_incident = F.col(
        "parent_incident_id"
    )
    status = F.col("status")
    duration = F.col("duration_seconds")

    invalid_priority = (
        priority.isNull()
        | ~priority.between(1, 5)
    )

    priority_not_eligible = (
        priority.between(4, 5)
    )

    parent_incident_present = (
        parent_incident.isNotNull()
        & (
            F.length(
                F.trim(parent_incident)
            ) > 0
        )
    )

    status_without_intervention = (
        normalize_status(status)
        == "SEM INTERVENCAO"
    )

    eligible_by_rule = (
        F.when(
            invalid_priority,
            F.lit(None).cast("boolean"),
        )
        .when(
            priority_not_eligible,
            F.lit(False),
        )
        .when(
            parent_incident_present,
            F.lit(False),
        )
        .when(
            status_without_intervention,
            F.lit(False),
        )
        .otherwise(F.lit(True))
    )

    rule_reason = (
        F.when(
            invalid_priority,
            F.lit("invalid_priority"),
        )
        .when(
            priority_not_eligible,
            F.lit("priority_not_eligible"),
        )
        .when(
            parent_incident_present,
            F.lit("parent_incident"),
        )
        .when(
            status_without_intervention,
            F.lit(
                "status_without_intervention"
            ),
        )
        .when(
            duration.isNull(),
            F.lit("missing_duration"),
        )
        .otherwise(F.lit("eligible"))
    )

    result = (
        df
        .withColumn(
            "kpi_rule_version",
            F.lit(KPI_RULE_VERSION),
        )
        .withColumn(
            "kpi_limit_seconds",
            create_kpi_limit_expression(),
        )
        .withColumn(
            "kpi_eligible_by_rule",
            eligible_by_rule,
        )
        .withColumn(
            "kpi_violated_by_rule",
            F.when(
                (
                    F.col(
                        "kpi_eligible_by_rule"
                    )
                    == F.lit(True)
                )
                & duration.isNotNull()
                & (duration >= 0),
                duration
                > F.col("kpi_limit_seconds"),
            ).otherwise(
                F.lit(None).cast("boolean")
            ),
        )
        .withColumn(
            "kpi_rule_reason",
            rule_reason,
        )
        .withColumn(
            "entered_kpi_rule_matches_source",
            F.when(
                F.col("entered_kpi").isNull()
                | F.col(
                    "kpi_eligible_by_rule"
                ).isNull(),
                F.lit(None).cast("boolean"),
            ).otherwise(
                F.col("entered_kpi")
                == F.col(
                    "kpi_eligible_by_rule"
                )
            ),
        )
        .withColumn(
            "kpi_violated_rule_matches_source",
            F.when(
                F.col("kpi_violated").isNull()
                | F.col(
                    "kpi_violated_by_rule"
                ).isNull(),
                F.lit(None).cast("boolean"),
            ).otherwise(
                F.col("kpi_violated")
                == F.col(
                    "kpi_violated_by_rule"
                )
            ),
        )
    )

    return result

