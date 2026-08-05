from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("PySpark IT Incident ETL")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("ERROR")

data = [
    ("Docker", "OK"),
    ("PySpark", "OK"),
    ("Java", "OK"),
]

df = spark.createDataFrame(data, ["componente", "status"])

print(f"\nSpark version: {spark.version}")
df.show()

spark.stop()