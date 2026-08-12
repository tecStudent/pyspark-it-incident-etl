from pyspark.sql import DataFrame
from pyspark.sql import functions as F


FORECAST_METHOD = "weighted_explainable_baseline"
FORECAST_METHOD_VERSION = "1.0"

HISTORY_DAYS = 28
RECENT_DAYS = 7
FORECAST_DAYS = 7

SAME_WEEKDAY_WEIGHT = 0.60
RECENT_AVERAGE_WEIGHT = 0.40

SCOPE_DESCRIPTION = (
    "Incidentes válidos P1, P2 e P3 consolidados"
)
SCOPE_PRIORITY_CODES = "1,2,3"


def scoped_incidents(df: DataFrame) -> DataFrame:
    return df.filter(
        F.col("priority_code").isin(1, 2, 3)
        & F.col("opened_at").isNotNull()
    )


def history_bounds(df: DataFrame) -> DataFrame:
    return (
        scoped_incidents(df)
        .agg(
            F.max(
                F.to_date("opened_at")
            ).alias("history_end_date")
        )
        .withColumn(
            "history_start_date",
            F.date_sub(
                "history_end_date",
                HISTORY_DAYS - 1,
            ),
        )
    )


def create_forecast_history(
    df: DataFrame,
) -> DataFrame:
    bounds = history_bounds(df)

    calendar = (
        bounds
        .filter(F.col("history_end_date").isNotNull())
        .select(
            "history_start_date",
            "history_end_date",
            F.explode(
                F.sequence(
                    "history_start_date",
                    "history_end_date",
                )
            ).alias("date"),
        )
    )

    daily_counts = (
        scoped_incidents(df)
        .withColumn(
            "date",
            F.to_date("opened_at"),
        )
        .groupBy("date")
        .agg(
            F.count("*").alias(
                "actual_incidents"
            )
        )
    )

    return (
        calendar
        .join(
            daily_counts,
            on="date",
            how="left",
        )
        .fillna(0, subset=["actual_incidents"])
        .withColumn(
            "actual_incidents",
            F.col("actual_incidents").cast("long"),
        )
        .withColumn(
            "day_of_week",
            F.dayofweek("date"),
        )
        .withColumn(
            "history_days",
            F.lit(HISTORY_DAYS),
        )
        .withColumn(
            "scope_description",
            F.lit(SCOPE_DESCRIPTION),
        )
        .withColumn(
            "scope_priority_codes",
            F.lit(SCOPE_PRIORITY_CODES),
        )
        .orderBy("date")
    )


def create_forecast_summary(
    df: DataFrame,
) -> DataFrame:
    history = create_forecast_history(df)

    statistics = history.agg(
        F.max("history_start_date").alias(
            "history_start_date"
        ),
        F.max("history_end_date").alias(
            "history_end_date"
        ),
        F.avg(
            F.when(
                F.datediff(
                    F.col("history_end_date"),
                    F.col("date"),
                )
                < RECENT_DAYS,
                F.col("actual_incidents"),
            )
        ).alias("recent_average"),
        F.stddev_pop("actual_incidents").alias(
            "history_stddev"
        ),
    )

    weekday_statistics = (
        history
        .groupBy("day_of_week")
        .agg(
            F.avg("actual_incidents").alias(
                "same_weekday_average"
            )
        )
    )

    horizon = (
        statistics
        .filter(F.col("history_end_date").isNotNull())
        .select(
            "history_start_date",
            "history_end_date",
            "recent_average",
            "history_stddev",
            F.explode(
                F.sequence(
                    F.lit(1),
                    F.lit(FORECAST_DAYS),
                )
            ).alias("horizon_day"),
        )
        .withColumn(
            "forecast_date",
            F.date_add(
                "history_end_date",
                F.col("horizon_day"),
            ),
        )
        .withColumn(
            "day_of_week",
            F.dayofweek("forecast_date"),
        )
        .join(
            weekday_statistics,
            on="day_of_week",
            how="left",
        )
    )

    predicted = F.greatest(
        F.round(
            F.coalesce(
                F.col("same_weekday_average"),
                F.col("recent_average"),
                F.lit(0.0),
            )
            * F.lit(SAME_WEEKDAY_WEIGHT)
            + F.coalesce(
                F.col("recent_average"),
                F.lit(0.0),
            )
            * F.lit(RECENT_AVERAGE_WEIGHT),
            0,
        ),
        F.lit(0),
    ).cast("long")

    risk_range = F.greatest(
        F.round(
            F.coalesce(
                F.col("history_stddev"),
                F.lit(0.0),
            ),
            0,
        ),
        F.lit(0),
    ).cast("long")

    forecast_rows = (
        horizon
        .withColumn(
            "predicted_incidents",
            predicted,
        )
        .withColumn("risk_range", risk_range)
        .withColumn(
            "lower_bound",
            F.greatest(
                F.col("predicted_incidents")
                - F.col("risk_range"),
                F.lit(0),
            ).cast("long"),
        )
        .withColumn(
            "upper_bound",
            (
                F.col("predicted_incidents")
                + F.col("risk_range")
            ).cast("long"),
        )
        .withColumn(
            "method",
            F.lit(FORECAST_METHOD),
        )
        .withColumn(
            "method_version",
            F.lit(FORECAST_METHOD_VERSION),
        )
        .withColumn(
            "history_days",
            F.lit(HISTORY_DAYS),
        )
        .withColumn(
            "recent_days",
            F.lit(RECENT_DAYS),
        )
        .withColumn(
            "same_weekday_weight",
            F.lit(SAME_WEEKDAY_WEIGHT),
        )
        .withColumn(
            "recent_average_weight",
            F.lit(RECENT_AVERAGE_WEIGHT),
        )
        .withColumn(
            "scope_description",
            F.lit(SCOPE_DESCRIPTION),
        )
        .withColumn(
            "scope_priority_codes",
            F.lit(SCOPE_PRIORITY_CODES),
        )
    )

    forecast_totals = forecast_rows.agg(
        F.max(
            F.when(
                F.col("horizon_day") == 1,
                F.col("predicted_incidents"),
            )
        ).alias("forecast_d1"),
        F.sum("predicted_incidents").alias(
            "forecast_d7"
        ),
    )

    return (
        forecast_rows
        .crossJoin(forecast_totals)
        .select(
            "forecast_date",
            "horizon_day",
            "predicted_incidents",
            "lower_bound",
            "upper_bound",
            "forecast_d1",
            "forecast_d7",
            "risk_range",
            "history_start_date",
            "history_end_date",
            "method",
            "method_version",
            "history_days",
            "recent_days",
            "same_weekday_weight",
            "recent_average_weight",
            "scope_description",
            "scope_priority_codes",
        )
        .orderBy("horizon_day")
    )
