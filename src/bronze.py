from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


INPUT_PATH = "data/raw/incidents.csv"
OUTPUT_PATH = "data/bronze/incidents"


RAW_SCHEMA = StructType([
    StructField("Número", StringType(), True),
    StructField("Prioridade", StringType(), True),
    StructField("Produto", StringType(), True),
    StructField("Categoria", StringType(), True),
    StructField("Subcategoria", StringType(), True),
    StructField("Grupo designado", StringType(), True),
    StructField("Item de configuração", StringType(), True),
    StructField("Aberto", StringType(), True),
    StructField("Resolvido", StringType(), True),
    StructField("Encerrado", StringType(), True),
    StructField("Duração", StringType(), True),
    StructField("Código de fechamento", StringType(), True),
    StructField("Descrição resumida", StringType(), True),
    StructField("Solução", StringType(), True),
    StructField("Aberto por", StringType(), True),
    StructField("Incidente Pai", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Entrou para KPI?", StringType(), True),
    StructField("KPI Violado?", StringType(), True),
])


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("IT Incident Bronze Ingestion")
        .getOrCreate()
    )


def main() -> None:
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw_df = (
            spark.read
            .schema(RAW_SCHEMA)
            .option("header", "true")
            .option("encoding", "UTF-8")
            .option("multiLine", "true")
            .option("quote", '"')
            .option("escape", '"')
            .option("mode", "FAILFAST")
            .option("enforceSchema", "false")
            .csv(INPUT_PATH)
        )

        bronze_df = (
            raw_df
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.input_file_name())
        )

        source_count = bronze_df.count()

        print(f"Registros lidos da origem: {source_count}")

        bronze_df.write.mode("overwrite").parquet(OUTPUT_PATH)

        output_count = spark.read.parquet(OUTPUT_PATH).count()

        print(f"Registros gravados na Bronze: {output_count}")

        if source_count != output_count:
            raise RuntimeError(
                "Quantidade de registros da origem e da Bronze não confere."
            )

        print("Validação de quantidade: OK")

        bronze_df.select(
            "Número",
            "Prioridade",
            "Grupo designado",
            "Aberto",
            "Status",
        ).show(5, truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()