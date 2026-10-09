"""Optional no-JVM sanity read of the Tableflow Iceberg table via pyiceberg
(lighter than Spark; handy to confirm the table exists before running Spark).

Run:  source .env && python3 reader/quick_read.py
"""
import os

from pyiceberg.catalog import load_catalog

BUCKET = os.environ["AWS_S3_BUCKET"]
PREFIX = os.environ.get("ICEBERG_S3_PREFIX", "warehouse")
GLUE_DB = os.environ.get("AWS_GLUE_DATABASE", "olpipe")
REGION = os.environ.get("AWS_REGION", "us-east-2")
TABLE = os.environ.get("ENRICHED_TOPIC", "orders-enriched").replace("-", "_")


def main():
    catalog = load_catalog("glue", **{
        "type": "glue",
        "warehouse": f"s3://{BUCKET}/{PREFIX}",
        "s3.region": REGION,
        "glue.region": REGION,
    })
    tbl = catalog.load_table(f"{GLUE_DB}.{TABLE}")
    arrow = tbl.scan().to_arrow()
    print(f"\n{GLUE_DB}.{TABLE}: {arrow.num_rows} rows\n")
    print(arrow.to_pandas().to_string(index=False))


if __name__ == "__main__":
    main()
