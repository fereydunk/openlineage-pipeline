"""The pipeline's end consumer: Apache Spark (OSS) reads the Tableflow-produced
Iceberg table straight from AWS Glue + S3 — no Confluent dependency at query time.

Run:  source .env && bash run_spark.sh reader/spark_read.py
"""
import os

from pyspark.sql import SparkSession

BUCKET = os.environ["AWS_S3_BUCKET"]
PREFIX = os.environ.get("ICEBERG_S3_PREFIX", "warehouse")
GLUE_DB = os.environ.get("AWS_GLUE_DATABASE", "olpipe")
# Tableflow names the Iceberg table after the topic with '-' -> '_'
TABLE = os.environ.get("ENRICHED_TOPIC", "orders-enriched").replace("-", "_")
FQN = f"glue.{GLUE_DB}.{TABLE}"


def main():
    spark = (
        SparkSession.builder.appName("olpipe-spark-reader")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.glue.catalog-impl",
                "org.apache.iceberg.aws.glue.GlueCatalog")
        .config("spark.sql.catalog.glue.warehouse", f"s3://{BUCKET}/{PREFIX}")
        .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.glue.glue.skip-name-validation", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"\n=== Spark reading Iceberg table {FQN} ===\n")
    df = spark.table(FQN)
    print(f"row count: {df.count()}\n")
    df.show(50, truncate=False)

    # A real query: spend by risk tier — proving Spark genuinely processes the data.
    print("\n=== spend by RISK_TIER ===\n")
    df.createOrReplaceTempView("orders")
    spark.sql("""
        SELECT RISK_TIER,
               COUNT(*)            AS orders,
               ROUND(SUM(AMOUNT_USD), 2) AS total_usd
        FROM orders
        GROUP BY RISK_TIER
        ORDER BY total_usd DESC
    """).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
