from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


RISK_METHODOLOGY_NAME = "weighted_operational_risk"
RISK_METHODOLOGY_VERSION = "1.0"

VOLUME_WEIGHT = 0.45
KPI_VIOLATION_RATE_WEIGHT = 0.35
AVG_DURATION_WEIGHT = 0.20

RISK_DIMENSIONS = (
    ("priority", "priority_name"),
    ("product", "product"),
    ("category", "category"),
    ("assigned_group", "assigned_group"),
)


def conditional_count(condition: Column) -> Column:
    return F.sum(
        F.when(condition, 1).otherwise(0)
    )


def normalize_dimension_value(
    column_name: str,
) -> Column:
    value = F.trim(
        F.col(column_name).cast("string")
    )

    return F.when(
        value == "",
        F.lit(None).cast("string"),
    ).otherwise(value)


def aggregate_dimension(
    df: DataFrame,
    dimension_type: str,
    column_name: str,
) -> DataFrame:
    result = (
        df
        .withColumn(
            "_dimension_value",
            normalize_dimension_value(
                column_name
            ),
        )
        .groupBy("_dimension_value")
        .agg(
            F.count("*").alias("volume"),
            conditional_count(
                (
                    F.col("kpi_eligible_by_rule")
                    == True
                )
                & F.col(
                    "kpi_violated_by_rule"
                ).isNotNull()
            ).alias("evaluated_kpi_incidents"),
            conditional_count(
                F.col("kpi_violated_by_rule")
                == True
            ).alias("kpi_violations"),
            F.round(
                F.avg("duration_seconds"),
                2,
            ).alias("avg_duration_seconds"),
        )
        .withColumn(
            "dimension_type",
            F.lit(dimension_type),
        )
        .withColumnRenamed(
            "_dimension_value",
            "dimension_value",
        )
        .withColumn(
            "is_unknown",
            F.col("dimension_value").isNull(),
        )
        .withColumn(
            "kpi_violation_rate_pct",
            F.when(
                F.col("evaluated_kpi_incidents")
                > 0,
                F.round(
                    (
                        F.col("kpi_violations")
                        / F.col(
                            "evaluated_kpi_incidents"
                        )
                    )
                    * 100,
                    2,
                ),
            ),
        )
        .select(
            "dimension_type",
            "dimension_value",
            "is_unknown",
            "volume",
            "evaluated_kpi_incidents",
            "kpi_violations",
            "kpi_violation_rate_pct",
            "avg_duration_seconds",
        )
    )

    return result


def normalized_component(
    value_column: str,
    maximum_column: str,
) -> Column:
    return F.when(
        F.col(maximum_column) > 0,
        F.least(
            F.coalesce(
                F.col(value_column).cast("double"),
                F.lit(0.0),
            )
            / F.col(maximum_column),
            F.lit(1.0),
        ),
    ).otherwise(F.lit(0.0))


def add_risk_score(df: DataFrame) -> DataFrame:
    dimension_window = Window.partitionBy(
        "dimension_type"
    )

    known_item = ~F.col("is_unknown")

    result = (
        df
        .withColumn(
            "_max_volume",
            F.max(
                F.when(
                    known_item,
                    F.col("volume"),
                )
            ).over(dimension_window),
        )
        .withColumn(
            "_max_violation_rate",
            F.max(
                F.when(
                    known_item,
                    F.col(
                        "kpi_violation_rate_pct"
                    ),
                )
            ).over(dimension_window),
        )
        .withColumn(
            "_max_avg_duration",
            F.max(
                F.when(
                    known_item,
                    F.col("avg_duration_seconds"),
                )
            ).over(dimension_window),
        )
        .withColumn(
            "volume_normalized",
            F.round(
                normalized_component(
                    "volume",
                    "_max_volume",
                ),
                4,
            ),
        )
        .withColumn(
            "violation_rate_normalized",
            F.round(
                normalized_component(
                    "kpi_violation_rate_pct",
                    "_max_violation_rate",
                ),
                4,
            ),
        )
        .withColumn(
            "avg_duration_normalized",
            F.round(
                normalized_component(
                    "avg_duration_seconds",
                    "_max_avg_duration",
                ),
                4,
            ),
        )
        .withColumn(
            "risk_score",
            F.round(
                (
                    F.col("volume_normalized")
                    * F.lit(VOLUME_WEIGHT)
                    + F.col(
                        "violation_rate_normalized"
                    )
                    * F.lit(
                        KPI_VIOLATION_RATE_WEIGHT
                    )
                    + F.col(
                        "avg_duration_normalized"
                    )
                    * F.lit(AVG_DURATION_WEIGHT)
                )
                * 100,
                2,
            ),
        )
        .drop(
            "_max_volume",
            "_max_violation_rate",
            "_max_avg_duration",
        )
    )

    return result


def add_dimension_rank(df: DataFrame) -> DataFrame:
    ranking_window = (
        Window
        .partitionBy("dimension_type")
        .orderBy(
            F.col("is_unknown").asc(),
            F.col("risk_score").desc(),
            F.col("volume").desc(),
            F.col("dimension_value").asc(),
        )
    )

    return df.withColumn(
        "rank",
        F.when(
            ~F.col("is_unknown"),
            F.row_number().over(ranking_window),
        ).cast("int"),
    )


def create_risk_summary(df: DataFrame) -> DataFrame:
    summaries = [
        aggregate_dimension(
            df,
            dimension_type,
            column_name,
        )
        for dimension_type, column_name
        in RISK_DIMENSIONS
    ]

    combined = summaries[0]

    for summary in summaries[1:]:
        combined = combined.unionByName(summary)

    return (
        combined
        .transform(add_risk_score)
        .transform(add_dimension_rank)
        .orderBy(
            "dimension_type",
            F.col("is_unknown").asc(),
            "rank",
        )
    )
