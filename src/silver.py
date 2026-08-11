from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window


INPUT_PATH = "data/bronze/incidents"
OUTPUT_PATH = "data/silver/incidents"


COLUMN_MAPPING = {
    "Número": "incident_id",
    "Prioridade": "priority",
    "Produto": "product",
    "Categoria": "category",
    "Subcategoria": "subcategory",
    "Grupo designado": "assigned_group",
    "Item de configuração": "configuration_item",
    "Aberto": "opened_at",
    "Resolvido": "resolved_at",
    "Encerrado": "closed_at",
    "Duração": "duration_seconds",
    "Código de fechamento": "close_code",
    "Descrição resumida": "short_description",
    "Solução": "solution",
    "Aberto por": "opened_by",
    "Incidente Pai": "parent_incident_id",
    "Status": "status",
    "Entrou para KPI?": "entered_kpi",
    "KPI Violado?": "kpi_violated",
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IT Incident Silver Transformation")
        .getOrCreate()
    )


def rename_columns(df: DataFrame) -> DataFrame:
    for original, new_name in COLUMN_MAPPING.items():
        df = df.withColumnRenamed(original, new_name)

    return df


def clean_strings(df: DataFrame) -> DataFrame:
    string_columns = [
        column
        for column in df.columns
        if not column.startswith("_")
    ]

    for column in string_columns:
        df = df.withColumn(
            column,
            F.when(
                F.trim(F.col(column)) == "",
                F.lit(None),
            ).otherwise(
                F.trim(F.col(column))
            ),
        )

    return df


def transform_types(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn(
            "priority_code",
            F.regexp_extract("priority", r"^(\d+)", 1).cast("int"),
        )
        .withColumn(
            "priority_name",
            F.trim(
                F.regexp_replace(
                    "priority",
                    r"^\d+\s*-\s*",
                    "",
                )
            ),
        )
        .withColumn(
            "opened_at",
            F.to_timestamp("opened_at"),
        )
        .withColumn(
            "resolved_at",
            F.to_timestamp("resolved_at"),
        )
        .withColumn(
            "closed_at",
            F.to_timestamp("closed_at"),
        )
        .withColumn(
            "duration_seconds",
            F.col("duration_seconds")
            .cast("double")
            .cast("long"),
        )
        .withColumn(
            "entered_kpi",
            F.when(
                F.upper("entered_kpi") == "SIM",
                F.lit(True),
            )
            .when(
                F.upper("entered_kpi") == "NAO",
                F.lit(False),
            )
            .otherwise(
                F.lit(None).cast("boolean")
            ),
        )
        .withColumn(
            "kpi_violated",
            F.when(
                F.upper("kpi_violated") == "SIM",
                F.lit(True),
            )
            .when(
                F.upper("kpi_violated") == "NAO",
                F.lit(False),
            )
            .otherwise(
                F.lit(None).cast("boolean")
            ),
        )
        .withColumn(
            "opened_year",
            F.year("opened_at"),
        )
        .withColumn(
            "opened_month",
            F.month("opened_at"),
        )
    )


def add_data_quality(df: DataFrame) -> DataFrame:
    dq_issues = F.concat_ws(
        "|",

        F.when(
            F.col("incident_id").isNull(),
            "missing_incident_id",
        ),

        F.when(
            ~F.col("incident_id").rlike(r"^INC\d{7}$"),
            "invalid_incident_id",
        ),

        F.when(
            ~F.col("priority_code").between(1, 5),
            "invalid_priority",
        ),

        F.when(
            F.col("assigned_group").isNull(),
            "missing_assigned_group",
        ),

        F.when(
            F.col("opened_at").isNull(),
            "invalid_opened_at",
        ),

        F.when(
            F.col("closed_at").isNull(),
            "invalid_closed_at",
        ),

        F.when(
            F.col("duration_seconds").isNull(),
            "invalid_duration",
        ),

        F.when(
            F.col("duration_seconds") < 0,
            "negative_duration",
        ),

        F.when(
            F.col("closed_at") < F.col("opened_at"),
            "closed_before_opened",
        ),
    )

    return (
        df
        .withColumn("dq_issues", dq_issues)
        .withColumn(
            "dq_status",
            F.when(
                F.length("dq_issues") == 0,
                "VALID",
            ).otherwise("INVALID"),
        )
    )


def deduplicate(df: DataFrame) -> DataFrame:
    window = (
        Window
        .partitionBy("incident_id")
        .orderBy(
            F.col("closed_at").desc_nulls_last(),
            F.col("_ingested_at").desc_nulls_last(),
        )
    )

    return (
        df
        .withColumn(
            "_row_number",
            F.row_number().over(window),
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop("_row_number")
    )

def transform_records(
    bronze_df: DataFrame,
) -> DataFrame:
    silver_df = rename_columns(bronze_df)
    silver_df = clean_strings(silver_df)
    silver_df = transform_types(silver_df)
    silver_df = add_data_quality(silver_df)

    return silver_df

def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        bronze_df = spark.read.parquet(INPUT_PATH)

        bronze_count = bronze_df.count()

        silver_df = transform_records(bronze_df)
        silver_df = deduplicate(silver_df)

        silver_count = silver_df.count()

        invalid_count = (
            silver_df
            .filter(F.col("dq_status") == "INVALID")
            .count()
        )

        duplicate_count = bronze_count - silver_count

        print(f"Registros Bronze: {bronze_count}")
        print(f"Registros Silver: {silver_count}")
        print(f"Duplicidades removidas: {duplicate_count}")
        print(f"Registros inválidos: {invalid_count}")

        (
            silver_df.write
            .mode("overwrite")
            .partitionBy(
                "opened_year",
                "opened_month",
            )
            .parquet(OUTPUT_PATH)
        )

        output_count = spark.read.parquet(OUTPUT_PATH).count()

        if silver_count != output_count:
            raise RuntimeError(
                "Quantidade gravada na Silver não confere."
            )

        print("Validação Silver: OK")

        silver_df.select(
            "incident_id",
            "priority_code",
            "priority_name",
            "opened_at",
            "duration_seconds",
            "entered_kpi",
            "kpi_violated",
            "dq_status",
        ).show(10, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()