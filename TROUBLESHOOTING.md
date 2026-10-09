# Troubleshooting

## Docker daemon not running
`make up` needs Docker/OrbStack. Start it, then `docker ps` should work.

## DB2 takes forever to start / `make seed` hangs on "waiting for DB2"
First boot of the Db2 community image is genuinely slow (~3–5 min, longer under amd64 emulation on Apple
Silicon). Watch `docker compose logs -f db2` until you see **"Setup has completed."**. `make seed` polls the
container healthcheck and proceeds once it's `healthy`.

## DB2 on Apple Silicon
The image is amd64 (`platform: linux/amd64` in compose) and runs under emulation — slower but works. If it
refuses to start, ensure Rosetta/emulation is enabled in Docker Desktop/OrbStack settings.

## Connect worker won't start or can't reach Confluent Cloud
- The worker needs `KAFKA_API_KEY/SECRET` and `SR_URL/SR_API_KEY/SR_API_SECRET` in `.env` — run
  `make discover` first.
- Its internal topics (`_olpipe-connect-*`) are created on the CC cluster with RF 3; the API key must allow
  topic creation. Check `docker compose logs -f connect`.
- First build downloads the JDBC connector + Db2 driver (`make up --build`); re-run if the download flaked.

## `make connector` → connector FAILED / "No suitable driver"
The Db2 jcc driver jar must be on the JDBC connector's classpath — it's baked in by `connect/Dockerfile`.
If you changed versions, rebuild: `docker compose build connect && make up`. Check the task trace in
`make status`.

## No rows in `db2-orders`
- Confirm the connector is RUNNING: `make status`.
- Confirm DB2 has rows: `docker exec olpipe-db2 bash -lc "su - db2inst1 -c 'db2 connect to TESTDB && db2 \"select count(*) from ORDERS\"'"`.
- `mode=incrementing` only emits rows with an `ID` greater than the last seen — `make produce-more` adds new rows.

## Flink statement fails with "Table ... does not exist"
Pass `--database <CLUSTER_ID>` (the script does). Unqualified table names resolve against that cluster's
topics only when the database flag is set. Also make sure `db2-orders` exists and has data first.

## Flink "Different number of columns"
Writing a Kafka topic exposes an implicit `key BYTES` column. This repo uses `CREATE TABLE AS SELECT`
(CTAS), which defines the target schema from the projection and avoids the mismatch. If you switch to a
bare `INSERT INTO`, you must list the value columns explicitly.

## `make aws-setup` / `make tableflow` — AWS errors
- Export fresh AWS credentials first (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`);
  SSO/STS tokens expire and `source .env` won't refresh them. `aws sts get-caller-identity` should succeed.
- `make tableflow` also needs `CLOUD_API_KEY/SECRET` (a **Cloud resource-management** key) in `.env` —
  `make discover` mints it.
- If Tableflow `AssumeRole` fails, the IAM trust policy may not have propagated — re-run `make tableflow`
  (it sleeps for propagation, but AWS can lag).

## Tableflow: "Table not found" when reading / no `metadata.json` in S3
Tableflow flushes to S3 on a size/time threshold — with few rows it can take minutes. Produce more:
`make produce-more` (repeat), then check:
`aws s3 ls s3://$AWS_S3_BUCKET/warehouse/ --recursive | grep metadata.json`.
Until the first flush, Spark/pyiceberg will say the table doesn't exist.

## Glue table not auto-registered after a flush
Occasionally Tableflow writes S3 metadata but doesn't register the Glue table. Register it by pointing Glue
at the latest `metadata.json`:
```bash
aws glue create-table --database-name $AWS_GLUE_DATABASE --table-input "{
  \"Name\": \"orders_enriched\", \"TableType\": \"ICEBERG\",
  \"Parameters\": {\"table_type\": \"ICEBERG\", \"metadata_location\": \"s3://$AWS_S3_BUCKET/<path>/metadata.json\"}}"
```

## Spark: hangs on startup / Java errors
Force Java 21: `export JAVA_HOME=/opt/homebrew/opt/openjdk@21` (or `brew install openjdk@21`). `run_spark.sh`
sets it if unset. The Iceberg JARs are auto-downloaded to `.spark_jars/` on first run.

## Spark: S3 access denied / credentials
Spark reads AWS creds from the environment (`EnvironmentVariableCredentialsProvider`). `source .env` does not
re-export expired SSO tokens — refresh them in the shell before `make read`.

## Backtick-quote the `$$` columns in Spark
Tableflow's `$$partition` / `$$offset` columns must be backtick-quoted in Spark SQL if you select them.
