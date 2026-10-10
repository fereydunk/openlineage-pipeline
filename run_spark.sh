#!/usr/bin/env bash
# Submit a Spark job that reads the Tableflow Iceberg table from AWS Glue + S3.
# Downloads the Iceberg Spark runtime + AWS bundle JARs on first run, pins Java 21,
# and runs spark-submit locally. Spark is the pipeline's end consumer.
#
# Usage:  source .env && bash run_spark.sh reader/spark_read.py
set -euo pipefail
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

if [ -f "${SCRIPT_DIR}/.env" ]; then set -a; source "${SCRIPT_DIR}/.env"; set +a; fi

if [ -z "${JAVA_HOME:-}" ]; then
  export JAVA_HOME="/opt/homebrew/opt/openjdk@21"
  export PATH="${JAVA_HOME}/bin:${PATH}"
fi

ICEBERG_VERSION="1.10.1"
SPARK_SCALA="4.0_2.13"
JAR_CACHE="${SCRIPT_DIR}/.spark_jars"; mkdir -p "${JAR_CACHE}"
ICEBERG_SPARK_JAR="${JAR_CACHE}/iceberg-spark-runtime-${SPARK_SCALA}-${ICEBERG_VERSION}.jar"
ICEBERG_AWS_JAR="${JAR_CACHE}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"
MAVEN="https://repo1.maven.org/maven2"

dl() { [ -f "$2" ] && return; echo "[run_spark] downloading $(basename "$2")..."; curl -fL "$1" -o "$2"; }
dl "${MAVEN}/org/apache/iceberg/iceberg-spark-runtime-${SPARK_SCALA}/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_SCALA}-${ICEBERG_VERSION}.jar" "${ICEBERG_SPARK_JAR}"
dl "${MAVEN}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" "${ICEBERG_AWS_JAR}"

JOB="${1:-${SCRIPT_DIR}/reader/spark_read.py}"; shift 2>/dev/null || true
echo "[run_spark] job: ${JOB}  (Glue catalog + S3)"

# Prefer the venv's spark-submit (installed with the pyspark pip package); fall
# back to one on PATH. Pin the driver/worker python to the same interpreter.
if [ -x "${SCRIPT_DIR}/.venv/bin/spark-submit" ]; then
  SPARK_SUBMIT="${SCRIPT_DIR}/.venv/bin/spark-submit"
  export PYSPARK_PYTHON="${SCRIPT_DIR}/.venv/bin/python"
  export PYSPARK_DRIVER_PYTHON="${SCRIPT_DIR}/.venv/bin/python"
else
  SPARK_SUBMIT="spark-submit"
fi

"${SPARK_SUBMIT}" \
  --master "local[*]" \
  --jars "${ICEBERG_SPARK_JAR},${ICEBERG_AWS_JAR}" \
  --conf "spark.sql.shuffle.partitions=4" \
  --conf "spark.driver.memory=2g" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.EnvironmentVariableCredentialsProvider" \
  "${JOB}" "$@"
