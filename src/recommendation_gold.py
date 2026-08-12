from functools import reduce

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


RECOMMENDATION_RULES_VERSION = "1.0"

HIGH_RISK_SCORE = 50.0
CRITICAL_RISK_SCORE = 75.0

HIGH_VIOLATION_RATE_PCT = 20.0
CRITICAL_VIOLATION_RATE_PCT = 50.0
MINIMUM_EVALUATED_INCIDENTS = 5

CONCENTRATION_MINIMUM_VOLUME = 1000
COMPLIANCE_WARNING_PCT = 90.0
COMPLIANCE_CRITICAL_PCT = 75.0
FORECAST_GROWTH_WARNING_PCT = 10.0


OUTPUT_COLUMNS = [
    "recommendation_id",
    "rules_version",
    "rule_id",
    "recommendation_type",
    "dimension_type",
    "target",
    "severity",
    "title",
    "recommendation",
    "evidence",
    "metric_name",
    "metric_value",
    "metric_unit",
]


def stable_recommendation_id(
    rule_id,
    dimension_type,
    target,
):
    digest = F.sha2(
        F.concat_ws(
            "|",
            rule_id,
            dimension_type,
            F.coalesce(target, F.lit("UNKNOWN")),
        ),
        256,
    )

    return F.concat(
        F.lit("REC-"),
        F.upper(F.substring(digest, 1, 12)),
    )


def finalize_recommendation(
    df: DataFrame,
) -> DataFrame:
    return (
        df
        .withColumn(
            "rules_version",
            F.lit(RECOMMENDATION_RULES_VERSION),
        )
        .withColumn(
            "recommendation_id",
            stable_recommendation_id(
                F.col("rule_id"),
                F.col("dimension_type"),
                F.col("target"),
            ),
        )
        .select(*OUTPUT_COLUMNS)
    )


def create_risk_score_recommendations(
    risk_df: DataFrame,
) -> DataFrame:
    return finalize_recommendation(
        risk_df
        .filter(
            (~F.col("is_unknown"))
            & (F.col("rank") <= 3)
            & (F.col("risk_score") >= HIGH_RISK_SCORE)
        )
        .select(
            F.lit("RISK_SCORE_HIGH").alias("rule_id"),
            F.lit("RISK_MITIGATION").alias(
                "recommendation_type"
            ),
            F.col("dimension_type"),
            F.col("dimension_value").alias("target"),
            F.when(
                F.col("risk_score")
                >= CRITICAL_RISK_SCORE,
                F.lit("CRITICAL"),
            ).otherwise(F.lit("HIGH")).alias("severity"),
            F.lit(
                "Investigar dimensão com risco elevado"
            ).alias("title"),
            F.lit(
                "Revisar causas recorrentes, capacidade operacional "
                "e ações preventivas para reduzir o risco."
            ).alias("recommendation"),
            F.concat(
                F.lit("Posição "),
                F.col("rank").cast("string"),
                F.lit(" no ranking de "),
                F.col("dimension_type"),
                F.lit(", com score de risco "),
                F.format_number("risk_score", 2),
                F.lit("."),
            ).alias("evidence"),
            F.lit("risk_score").alias("metric_name"),
            F.col("risk_score").cast("double").alias(
                "metric_value"
            ),
            F.lit("score").alias("metric_unit"),
        )
    )


def create_violation_rate_recommendations(
    risk_df: DataFrame,
) -> DataFrame:
    violation_window = (
        Window
        .partitionBy("dimension_type")
        .orderBy(
            F.col("kpi_violation_rate_pct").desc(),
            F.col("evaluated_kpi_incidents").desc(),
            F.col("dimension_value").asc(),
        )
    )

    return finalize_recommendation(
        risk_df
        .filter(
            (~F.col("is_unknown"))
            & (
                F.col("evaluated_kpi_incidents")
                >= MINIMUM_EVALUATED_INCIDENTS
            )
            & (
                F.col("kpi_violation_rate_pct")
                >= HIGH_VIOLATION_RATE_PCT
            )
        )
        .withColumn(
            "_violation_rank",
            F.row_number().over(violation_window),
        )
        .filter(F.col("_violation_rank") <= 3)
        .select(
            F.lit("KPI_VIOLATION_RATE_HIGH").alias(
                "rule_id"
            ),
            F.lit("KPI_COMPLIANCE").alias(
                "recommendation_type"
            ),
            F.col("dimension_type"),
            F.col("dimension_value").alias("target"),
            F.when(
                F.col("kpi_violation_rate_pct")
                >= CRITICAL_VIOLATION_RATE_PCT,
                F.lit("CRITICAL"),
            ).otherwise(F.lit("HIGH")).alias("severity"),
            F.lit(
                "Reduzir violações de KPI"
            ).alias("title"),
            F.lit(
                "Analisar os incidentes violados, identificar causas "
                "comuns e definir um plano de correção."
            ).alias("recommendation"),
            F.concat(
                F.lit("Taxa calculada de violação de "),
                F.format_number(
                    "kpi_violation_rate_pct",
                    2,
                ),
                F.lit("% em "),
                F.col("evaluated_kpi_incidents").cast("string"),
                F.lit(" incidentes avaliados."),
            ).alias("evidence"),
            F.lit("kpi_violation_rate_pct").alias(
                "metric_name"
            ),
            F.col("kpi_violation_rate_pct")
            .cast("double")
            .alias("metric_value"),
            F.lit("percent").alias("metric_unit"),
        )
    )


def create_volume_concentration_recommendations(
    risk_df: DataFrame,
) -> DataFrame:
    volume_window = (
        Window
        .partitionBy("dimension_type")
        .orderBy(
            F.col("volume").desc(),
            F.col("dimension_value").asc(),
        )
    )

    return finalize_recommendation(
        risk_df
        .filter(
            (~F.col("is_unknown"))
            & F.col("dimension_type").isin(
                "product",
                "category",
                "assigned_group",
            )
            & (
                F.col("volume")
                >= CONCENTRATION_MINIMUM_VOLUME
            )
        )
        .withColumn(
            "_volume_rank",
            F.row_number().over(volume_window),
        )
        .filter(F.col("_volume_rank") == 1)
        .select(
            F.lit("VOLUME_CONCENTRATION").alias("rule_id"),
            F.lit("CAPACITY_REVIEW").alias(
                "recommendation_type"
            ),
            F.col("dimension_type"),
            F.col("dimension_value").alias("target"),
            F.lit("MEDIUM").alias("severity"),
            F.lit(
                "Avaliar concentração de volume"
            ).alias("title"),
            F.lit(
                "Revisar capacidade, distribuição da demanda e "
                "oportunidades de automação preventiva."
            ).alias("recommendation"),
            F.concat(
                F.lit("Maior volume da dimensão, com "),
                F.col("volume").cast("string"),
                F.lit(" incidentes."),
            ).alias("evidence"),
            F.lit("volume").alias("metric_name"),
            F.col("volume").cast("double").alias(
                "metric_value"
            ),
            F.lit("incidents").alias("metric_unit"),
        )
    )


def create_compliance_recommendations(
    annual_ola_df: DataFrame,
) -> DataFrame:
    target = F.concat_ws(
        " | ",
        F.col("opened_year").cast("string"),
        F.col("priority_name"),
    )

    return finalize_recommendation(
        annual_ola_df
        .filter(
            F.col("calculated_compliance_pct").isNotNull()
            & (
                F.col("calculated_compliance_pct")
                < COMPLIANCE_WARNING_PCT
            )
        )
        .select(
            F.lit("ANNUAL_COMPLIANCE_LOW").alias("rule_id"),
            F.lit("OLA_COMPLIANCE").alias(
                "recommendation_type"
            ),
            F.lit("annual_priority").alias(
                "dimension_type"
            ),
            target.alias("target"),
            F.when(
                F.col("calculated_compliance_pct")
                < COMPLIANCE_CRITICAL_PCT,
                F.lit("CRITICAL"),
            ).otherwise(F.lit("HIGH")).alias("severity"),
            F.lit(
                "Recuperar compliance anual de OLA"
            ).alias("title"),
            F.lit(
                "Priorizar análise das violações e acompanhar um plano "
                "de recuperação para a prioridade."
            ).alias("recommendation"),
            F.concat(
                F.lit("Compliance calculado de "),
                F.format_number(
                    "calculated_compliance_pct",
                    2,
                ),
                F.lit("% no ano."),
            ).alias("evidence"),
            F.lit("calculated_compliance_pct").alias(
                "metric_name"
            ),
            F.col("calculated_compliance_pct")
            .cast("double")
            .alias("metric_value"),
            F.lit("percent").alias("metric_unit"),
        )
    )


def create_forecast_recommendations(
    forecast_summary_df: DataFrame,
    forecast_history_df: DataFrame,
) -> DataFrame:
    recent_volume = (
        forecast_history_df
        .filter(
            F.datediff(
                F.col("history_end_date"),
                F.col("date"),
            )
            < 7
        )
        .agg(
            F.sum("actual_incidents").alias(
                "recent_7d_incidents"
            )
        )
    )

    forecast = (
        forecast_summary_df
        .filter(F.col("horizon_day") == 1)
        .select("forecast_d7")
        .crossJoin(recent_volume)
        .withColumn(
            "forecast_growth_pct",
            F.when(
                F.col("recent_7d_incidents") > 0,
                F.round(
                    (
                        (
                            F.col("forecast_d7")
                            - F.col("recent_7d_incidents")
                        )
                        / F.col("recent_7d_incidents")
                    )
                    * 100,
                    2,
                ),
            ),
        )
    )

    return finalize_recommendation(
        forecast
        .filter(
            F.col("forecast_growth_pct")
            >= FORECAST_GROWTH_WARNING_PCT
        )
        .select(
            F.lit("FORECAST_D7_GROWTH").alias("rule_id"),
            F.lit("FORECAST_CAPACITY").alias(
                "recommendation_type"
            ),
            F.lit("forecast").alias("dimension_type"),
            F.lit("next_7_days").alias("target"),
            F.when(
                F.col("forecast_growth_pct") >= 25,
                F.lit("HIGH"),
            ).otherwise(F.lit("MEDIUM")).alias("severity"),
            F.lit(
                "Preparar capacidade para aumento previsto"
            ).alias("title"),
            F.lit(
                "Revisar escala e capacidade operacional para os "
                "próximos sete dias."
            ).alias("recommendation"),
            F.concat(
                F.lit("Previsão D+7 de "),
                F.col("forecast_d7").cast("string"),
                F.lit(" incidentes, "),
                F.format_number("forecast_growth_pct", 2),
                F.lit("% acima dos sete dias recentes."),
            ).alias("evidence"),
            F.lit("forecast_growth_pct").alias(
                "metric_name"
            ),
            F.col("forecast_growth_pct")
            .cast("double")
            .alias("metric_value"),
            F.lit("percent").alias("metric_unit"),
        )
    )


def create_recommendations(
    risk_df: DataFrame,
    annual_ola_df: DataFrame,
    forecast_summary_df: DataFrame,
    forecast_history_df: DataFrame,
) -> DataFrame:
    recommendation_sets = [
        create_risk_score_recommendations(risk_df),
        create_violation_rate_recommendations(risk_df),
        create_volume_concentration_recommendations(risk_df),
        create_compliance_recommendations(annual_ola_df),
        create_forecast_recommendations(
            forecast_summary_df,
            forecast_history_df,
        ),
    ]

    result = reduce(
        lambda left, right: left.unionByName(right),
        recommendation_sets,
    )

    severity_order = (
        F.when(F.col("severity") == "CRITICAL", 1)
        .when(F.col("severity") == "HIGH", 2)
        .when(F.col("severity") == "MEDIUM", 3)
        .otherwise(4)
    )

    return (
        result
        .dropDuplicates(["recommendation_id"])
        .orderBy(
            severity_order,
            "rule_id",
            "dimension_type",
            "target",
        )
    )
