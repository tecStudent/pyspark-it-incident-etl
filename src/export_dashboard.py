import json
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


OUTPUT_DIR = Path("docs/data")


TABLES = {
    "monthly_kpis": {
        "source": "data/gold/monthly_kpis",
        "target": "monthly_kpis.json",
    },
    "priority_summary": {
        "source": "data/gold/priority_summary",
        "target": "priority_summary.json",
    },
    "team_summary": {
        "source": "data/gold/team_summary",
        "target": "team_summary.json",
    },
    "dashboard_summary": {
        "source": "data/gold/dashboard_summary",
        "target": "dashboard_summary.json",
    },
}


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("Dashboard Data Export")
        .getOrCreate()
    )


def sort_dataframe(
    table_name: str,
    df: DataFrame,
) -> DataFrame:

    if table_name == "monthly_kpis":
        return df.orderBy(
            "opened_year",
            "opened_month",
        )

    if table_name == "priority_summary":
        return df.orderBy(
            "priority_code"
        )

    if table_name == "team_summary":
        return df.orderBy(
            F.desc("total_incidents")
        )
    
    if table_name == "dashboard_summary":
        return df.orderBy(
            "opened_year",
            "opened_month",
            "priority_code",
            "assigned_group",
        )
    return df


def export_table(
    spark: SparkSession,
    table_name: str,
    source: str,
    target: str,
) -> None:

    df = spark.read.parquet(source)

    df = sort_dataframe(
        table_name,
        df,
    )

    records = [
        row.asDict(recursive=True)
        for row in df.collect()
    ]

    output_path = OUTPUT_DIR / target

    with output_path.open(
        "w",
        encoding="utf-8",
    ) as json_file:

        json.dump(
            records,
            json_file,
            ensure_ascii=False,
            indent=2,
        )

    print(
        f"{table_name}: "
        f"{len(records)} registros -> "
        f"{output_path}"
    )


def main() -> None:

    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True,
    )

    try:
        for table_name, config in TABLES.items():

            export_table(
                spark=spark,
                table_name=table_name,
                source=config["source"],
                target=config["target"],
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()