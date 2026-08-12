from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from src.operational_gold import (
    create_annual_ola_summary,
    create_daily_trends,
    create_operational_kpi_summary,
)


INPUT_PATH = "data/silver/incidents"

MONTHLY_OUTPUT = "data/gold/monthly_kpis"
PRIORITY_OUTPUT = "data/gold/priority_summary"
TEAM_OUTPUT = "data/gold/team_summary"
DASHBOARD_OUTPUT = "data/gold/dashboard_summary"
DAILY_TRENDS_OUTPUT = "data/gold/daily_trends"
OPERATIONAL_KPI_OUTPUT = "data/gold/operational_kpi_summary"
ANNUAL_OLA_OUTPUT = "data/gold/annual_ola_summary"


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IT Incident Gold Aggregations")
        .getOrCreate()
    )


def conditional_count(condition):
    return F.sum(
        F.when(condition, 1).otherwise(0)
    )


def add_compliance_rate(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "kpi_compliance_pct",
        F.when(
            F.col("kpi_incidents") > 0,
            F.round(
                (
                    (
                        F.col("kpi_incidents")
                        - F.col("kpi_violations")
                    )
                    / F.col("kpi_incidents")
                )
                * 100,
                2,
            ),
        ),
    )


def create_monthly_kpis(df: DataFrame) -> DataFrame:
    result = (
        df
        .groupBy(
            "opened_year",
            "opened_month",
        )
        .agg(
            F.count("*").alias("total_incidents"),

            conditional_count(
                F.col("opened_by") == "Monitoramento"
            ).alias("monitoring_incidents"),

            conditional_count(
                F.col("opened_by") == "Manual"
            ).alias("manual_incidents"),

            conditional_count(
                F.col("entered_kpi") == True
            ).alias("kpi_incidents"),

            conditional_count(
                F.col("kpi_violated") == True
            ).alias("kpi_violations"),

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

    return add_compliance_rate(result)


def create_priority_summary(df: DataFrame) -> DataFrame:
    result = (
        df
        .groupBy(
            "priority_code",
            "priority_name",
        )
        .agg(
            F.count("*").alias("total_incidents"),

            conditional_count(
                F.col("entered_kpi") == True
            ).alias("kpi_incidents"),

            conditional_count(
                F.col("kpi_violated") == True
            ).alias("kpi_violations"),

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

    return add_compliance_rate(result)


def create_team_summary(df: DataFrame) -> DataFrame:
    result = (
        df
        .groupBy("assigned_group")
        .agg(
            F.count("*").alias("total_incidents"),

            conditional_count(
                F.col("entered_kpi") == True
            ).alias("kpi_incidents"),

            conditional_count(
                F.col("kpi_violated") == True
            ).alias("kpi_violations"),

            F.round(
                F.avg("duration_seconds"),
                2,
            ).alias("avg_duration_seconds"),
        )
    )

    return add_compliance_rate(result)

def create_dashboard_summary(df: DataFrame) -> DataFrame:
    return (
        df
        .groupBy(
            "opened_year",
            "opened_month",
            "priority_code",
            "priority_name",
            "assigned_group",
        )
        .agg(
            F.count("*").alias("total_incidents"),

            conditional_count(
                F.col("entered_kpi") == True
            ).alias("kpi_incidents"),

            conditional_count(
                F.col("kpi_violated") == True
            ).alias("kpi_violations"),

            conditional_count(
                F.col("opened_by") == "Monitoramento"
            ).alias("monitoring_incidents"),

            conditional_count(
                F.col("opened_by") == "Manual"
            ).alias("manual_incidents"),
        )
    )

def write_gold(
    spark: SparkSession,
    df: DataFrame,
    output_path: str,
    table_name: str,
) -> None:
    df.write.mode("overwrite").parquet(output_path)

    output_count = (
        spark.read
        .parquet(output_path)
        .count()
    )

    print(
        f"{table_name}: {output_count} registros gravados"
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        silver_df = (
            spark.read
            .parquet(INPUT_PATH)
            .filter(F.col("dq_status") == "VALID")
        )

        print(
            f"Registros Silver válidos: {silver_df.count()}"
        )

        monthly_df = create_monthly_kpis(silver_df)
        priority_df = create_priority_summary(silver_df)
        team_df = create_team_summary(silver_df)
        dashboard_df = create_dashboard_summary(silver_df)
        daily_trends_df = create_daily_trends(silver_df)
        operational_kpi_df = (
            create_operational_kpi_summary(
                silver_df
            )
        )
        annual_ola_df = create_annual_ola_summary(
            silver_df
        )

        write_gold(
            spark,
            monthly_df,
            MONTHLY_OUTPUT,
            "monthly_kpis",
        )

        write_gold(
            spark,
            priority_df,
            PRIORITY_OUTPUT,
            "priority_summary",
        )

        write_gold(
            spark,
            team_df,
            TEAM_OUTPUT,
            "team_summary",
        )
        write_gold(
            spark,
            dashboard_df,
            DASHBOARD_OUTPUT,
            "dashboard_summary",
        )

        write_gold(
            spark,
            daily_trends_df,
            DAILY_TRENDS_OUTPUT,
            "daily_trends",
        )

        write_gold(
            spark,
            operational_kpi_df,
            OPERATIONAL_KPI_OUTPUT,
            "operational_kpi_summary",
        )

        write_gold(
            spark,
            annual_ola_df,
            ANNUAL_OLA_OUTPUT,
            "annual_ola_summary",
        )

        print("\nResumo mensal:")

        (
            monthly_df
            .orderBy(
                F.desc("opened_year"),
                F.desc("opened_month"),
            )
            .show(12, truncate=False)
        )

        print("\nResumo por prioridade:")

        (
            priority_df
            .orderBy("priority_code")
            .show(truncate=False)
        )

        print("\nTop 10 equipes por volume:")

        (
            team_df
            .orderBy(
                F.desc("total_incidents")
            )
            .show(10, truncate=False)
        )

        print("\nResumo anual de OLA:")

        (
            annual_ola_df
            .orderBy(
                "opened_year",
                "priority_code",
            )
            .show(truncate=False)
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
