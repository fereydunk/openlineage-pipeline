# Troubleshooting

## Docker daemon not running
`make up` needs Docker/OrbStack. Start it, then `docker ps` should work.

## DB2 takes forever to start / `make seed` hangs on "waiting for DB2"
First boot of the Db2 community image is genuinely slow (~3–5 min, longer under amd64 emulation on Apple
Silicon). Watch `docker compose logs -f db2` until you see **"Setup has completed."**. `make seed` polls the
container healthcheck and proceeds once it's `healthy`.

## DB2 on Apple Silicon
The image is amd64 (`platform: linux/amd64` in compose) and runs under emulation — slower but works. If it
refuses to start, ensure Rosetta/emulation is enabled in Docker Desktop/OrbStack settings (colima:
`--vm-type vz --vz-rosetta`).

## DB2 shows "unhealthy" but the pipeline works
The healthcheck can read `unhealthy` even while Db2 is serving fine. Always drive Db2 with
`docker exec -u db2inst1 olpipe-db2 bash -lc '. ~/sqllib/db2profile; db2 connect to TESTDB; …'` —
**never** `su - db2inst1` (it loses the instance attachment / db2 isn't on the PATH). The compose
healthcheck and the `seed`/`produce-more` Make targets source `db2profile` for this reason.

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
- Confirm DB2 has rows: `docker exec -u db2inst1 olpipe-db2 bash -lc '. ~/sqllib/db2profile; db2 connect to TESTDB; db2 "select count(*) from ORDERS"'`.
- `mode=incrementing` only emits rows with an `ID` greater than the last seen — `make produce-more` adds new rows.
- The connector's committed offset is the real proof it's producing: `curl -s localhost:8083/connectors/db2-orders-source/offsets` — an advancing `incrementing` value means rows are reaching `db2-orders`.

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
- **"Unable to assume the IAM role"** — the trust policy must allow **both** `sts:AssumeRole` (gated by the
  `sts:ExternalId` condition) **and** `sts:TagSession` (no condition). Confluent assumes the role *with
  session tags*, so a policy missing `sts:TagSession` fails even when the principal + ExternalId are
  correct. `infra/02_setup_tableflow.py` writes both. After fixing the policy, **disable then re-enable**
  the Tableflow topic (a suspended BYOS topic won't retry on its own) and allow ~1–3 min for cross-account
  IAM propagation.

## Tableflow: "Table not found" when reading / no `metadata.json` in S3
Tableflow flushes to S3 on a size/time threshold — with few rows it can take minutes. Produce more:
`make produce-more` (repeat), then check (Tableflow writes under a hashed prefix, not `/warehouse/`):
`aws s3 ls s3://$AWS_S3_BUCKET/ --recursive | grep metadata.json`.
Until the first flush, Spark/pyiceberg will say the table doesn't exist.

## Glue table name / not auto-registered after a flush
The Glue/Iceberg table keeps the topic name **verbatim**: `orders-enriched` (hyphen), **not**
`orders_enriched`. In Spark it must be back-tick-quoted with `glue.skip-name-validation=true` (the reader
does both). If Tableflow writes S3 metadata but doesn't register the Glue table, register it against the
latest `metadata.json`:
```bash
aws glue create-table --database-name $AWS_GLUE_DATABASE --table-input "{
  \"Name\": \"orders-enriched\", \"TableType\": \"ICEBERG\",
  \"Parameters\": {\"table_type\": \"ICEBERG\", \"metadata_location\": \"s3://$AWS_S3_BUCKET/<hashed-path>/metadata/<n>.metadata.json\"}}"
```

## Spark: hangs on startup / Java errors
Force Java 21: `export JAVA_HOME=/opt/homebrew/opt/openjdk@21` (or `brew install openjdk@21`). `run_spark.sh`
sets it if unset. The Iceberg JARs are auto-downloaded to `.spark_jars/` on first run.

## Spark: S3 access denied / credentials
Spark reads AWS creds from the environment (`EnvironmentVariableCredentialsProvider`). `source .env` does not
re-export expired SSO tokens — refresh them in the shell before `make read`.

## Backtick-quote the `$$` columns in Spark
Tableflow's `$$partition` / `$$offset` columns must be backtick-quoted in Spark SQL if you select them.

## "My new rows aren't showing up in the topic" (but they are)
Three macOS/CLI traps make flowing data look missing:
- There's **no `timeout`** command on stock macOS — a `timeout … confluent kafka topic consume …` is a
  silent no-op. Background the consumer and `kill` it instead.
- The consume flag is **`--schema-registry-endpoint`** (plus `--schema-registry-api-key/-secret`), **not**
  `--sr-endpoint`.
- `confluent kafka topic consume` reads from **latest** by default, so it misses rows produced before it
  attached. Pass `--from-beginning`, or start the consumer *before* inserting.

## Flink statement is RUNNING but `orders-enriched` isn't growing
A statement can read `RUNNING` with null latest-offsets. Verify by **data**, not status: insert rows and
confirm they reach `orders-enriched` (consume `--from-beginning` and grep for the new ORDER_IDs). A stale
Schema Registry subject can also make the CTAS FAIL with *"subject orders-enriched-value doesn't match"* —
hard-delete the subject and recreate the statement.

## Marquez UI at :3000 shows nothing / `curl :5002` returns 403
On macOS, **Control Center / AirPlay Receiver (AirTunes) squats on port 5000** and returns 403 — which is
why the Marquez API is mapped to host **5002** (`docker-compose.marquez.yml`), not the usual 5000. The
emitter and `render_graph.py` default to `http://localhost:5002`. The UI on `:3000` reaches the API over
the Docker network, so it's unaffected.

## `make marquez-ui`: "Compiled with problems" or "Element type is invalid … Sidenav"
These are `marquez-web` build issues, fixed by pinning the right version:
- **"Compiled with problems"** is the webpack dev-server overlay blocking on a harmless warning. The patch
  sets `devServer.client.overlay.warnings=false`.
- **"Element type is invalid … Check the render method of `Sidenav`"** means you built `marquez-web` from
  `main` (React 19, broken). `scripts/marquez_ui_patched.sh` pins **tag `0.51.1`** (React 18), which matches
  the stock image. If you hit this, delete `~/marquez-ui-patch` and re-run `make marquez-ui`.
- After a rebuild, **hard-refresh** the browser (Cmd+Shift+R) — it caches the old bundle.
- The patched UI is a webpack dev server, not a container — it must stay running. `make marquez-ui`
  restarts it. For a no-Node alternative, `make lineage-view` renders `lineage/graph.html`.

## Marquez graph is empty
Run `make lineage` first (it emits the events), and make sure Marquez is up (`make marquez-up`). Check the
API directly: `curl -s http://localhost:5002/api/v1/jobs | python3 -m json.tool`.
