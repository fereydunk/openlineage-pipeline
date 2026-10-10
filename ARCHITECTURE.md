# Architecture

The pipeline spans three domains — your laptop (Docker), Confluent Cloud, and AWS — with Apache Spark
as the open-source end consumer.

```
┌─ Laptop (Docker) ─────────────────┐        ┌─ Confluent Cloud ──────────────────────────┐
│  IBM Db2  (ORDERS table)          │        │  Kafka: db2-orders ──► Flink SQL ──► orders- │
│     ▲ polls                       │        │                        (CTAS, RISK_TIER)  enriched
│  Kafka Connect (self-managed)     │ SASL   │                                   │          │
│   JDBC Source ───────────────────────────► │  (Connect worker's internal topics live here)│
└───────────────────────────────────┘  SSL   │                                   │ Tableflow │
                                              └───────────────────────────────────┼──────────┘
                                                                                  ▼ (BYOS)
                                       ┌─ AWS ──────────────────────────────────────────────┐
                                       │  S3  s3://olpipe-tableflow-<acct>/warehouse/ (Iceberg)│
                                       │  Glue  olpipe.orders-enriched  (Iceberg catalog)     │
                                       └───────────────────────────────────────────┬─────────┘
                                                                                    ▼
                                                         Apache Spark (OSS, local) reads Glue + S3
                                                         → SELECT / aggregate  (the end consumer)
```

## Stage by stage

**1. Db2 → Kafka (self-managed Connect + JDBC Source).**
Db2 runs in Docker (`icr.io/db2_community/db2`, amd64 under emulation on Apple Silicon). A self-managed
`cp-kafka-connect` worker — built with the Confluent **JDBC** connector and the **IBM Db2 jcc** driver —
connects to **Confluent Cloud** (SASL_SSL) and runs the `JdbcSourceConnector` in `mode=incrementing` on
the `ID` column, polling the `ORDERS` table every 2s. A `RegexRouter` SMT writes to exactly `db2-orders`.
Values are Avro (worker-level `AvroConverter` + Schema Registry); keys are strings. The worker's own
internal topics (`_olpipe-connect-*`) live on the CC cluster (RF 3).

> Why self-managed, not a CC managed connector: Db2 is local, so a cloud-hosted connector can't reach it.
> Why JDBC polling, not Debezium CDC: avoids the finicky Db2 ASN capture-agent setup.

**2. Flink SQL (Confluent Cloud).**
`CREATE TABLE \`orders-enriched\` AS SELECT … CASE … AS RISK_TIER FROM \`db2-orders\`` runs on the Flink
compute pool. CTAS creates the target topic *and* its schema from the projection — which sidesteps the
implicit `key BYTES` column that trips a bare `INSERT INTO` ("different number of columns"). `--database
<CLUSTER_ID>` makes unqualified names resolve to this cluster's topics; `--wait` surfaces failures.

**3. Tableflow → Iceberg (Confluent Cloud + AWS, BYOS).**
Tableflow materializes `orders-enriched` as an **Apache Iceberg** table in **your** S3 bucket, catalogued
in **AWS Glue** (customer-managed storage). Wiring (in `infra/02_setup_tableflow.py`):
1. A Confluent **provider integration** (`POST /pim/v1/integrations`) returns a Confluent IAM principal +
   external id.
2. Your IAM role's **trust policy** is updated to let that principal assume it. It needs **two** statements:
   `sts:AssumeRole` gated by the `sts:ExternalId` condition **and** `sts:TagSession` (no condition) —
   Confluent assumes the role *with session tags*, so a policy missing `sts:TagSession` fails with the
   misleading *"Unable to assume the IAM role"*.
3. **Enable Tableflow** on the topic via the CLI: `confluent tableflow topic enable orders-enriched
   --storage-type BYOS --provider-integration <cspi> --bucket-name <bucket> --table-formats ICEBERG`.
   (The old REST `storage.kind=CustomerManaged` discriminator is no longer accepted — use the CLI.)
4. A **Glue catalog integration** (`POST /tableflow/v1/catalog-integrations`, `kind=AwsGlue`, custom
   database) registers the Iceberg table in Glue. The table keeps the topic name **verbatim** —
   `orders-enriched` (hyphen), not `orders_enriched`. It also carries Kafka metadata columns
   (`$$topic`, `$$offset`, …) alongside the real ones.

**4. Spark (open source) — the end consumer.**
`reader/spark_read.py` builds a SparkSession with the Iceberg extensions and a `glue` catalog
(`SparkCatalog` → `GlueCatalog`, warehouse `s3://…`, `S3FileIO`, `glue.skip-name-validation=true`), then
`` spark.table("glue.olpipe.`orders-enriched`") `` (the hyphen requires back-tick quoting) → `.show()` + a
`GROUP BY RISK_TIER` aggregation. **No Confluent dependency at query time** — pure Glue + S3. `run_spark.sh`
downloads the Iceberg Spark runtime + AWS bundle JARs, pins Java 21, and uses the venv's `spark-submit`
(from the `pyspark` pip package).

## Ownership / trust boundaries
- **Confluent Cloud** holds the Kafka topics, runs Flink, and operates Tableflow — but the Iceberg data
  lands in **your** S3 and is catalogued in **your** Glue (BYOS). Confluent assumes a role you control.
- **AWS credentials** stay on your laptop / in your shell (Spark reads via the env-var credential provider;
  Tableflow uses the AssumeRole grant).
- **Db2 credentials** are local-only (Docker + `.env`); they never leave the Connect worker.

## Schema handling
- Db2 column names are UPPERCASE → Avro field names UPPERCASE → Flink columns UPPERCASE → Iceberg columns
  (Tableflow lowercases them in the Glue table: `order_id`, `risk_tier`, …). Spark SQL is case-insensitive
  on resolution, so the reader's `RISK_TIER` / `AMOUNT_USD` references work.
- Tableflow adds operational columns (`$$topic`, `$$partition`, `$$offset`, …); these must be
  back-tick-quoted in Spark SQL if referenced.

## Phase 2 — OpenLineage → Marquez (built)
The full chain is emitted as OpenLineage and rendered in a local **Marquez**.

**Marquez** runs from `docker-compose.marquez.yml` (Postgres + marquez-api + marquez-web). The API's host
port is **5002**, not the usual 5000 — on macOS, Control Center / AirPlay Receiver squats on :5000. The UI
(`:3000`) reaches the API over the Docker network, so it's unaffected.

**Emission** (`lineage/emit_to_marquez.py`, `openlineage-python` event_v2, HTTP transport): one OpenLineage
job per hop, each with its input/output datasets —

| Job (namespace) | input → output |
|---|---|
| `db2-orders-source` (`kafka-connect://env-m2qxq`) | `db2://…/TESTDB.DB2INST1.ORDERS` → `kafka://…/db2-orders` |
| `olpipe-enrich-orders` (`flink://env-m2qxq`) | `db2-orders` → `orders-enriched` |
| `orders-enriched-tableflow` (`tableflow://env-m2qxq`) | `orders-enriched` → `glue://us-east-2/olpipe.orders-enriched` |
| `olpipe-spark-reader` (`spark://olpipe-local`) | `glue://…/olpipe.orders-enriched` → (read) |

Marquez stitches these into one DAG purely by matching `(namespace, name)` across job inputs/outputs — so
the four independent events form the connected DB2→Spark graph, **including the two external ends** (Db2,
Spark) that the auto-discovery bridge drops. Conventions match the sibling `openlineage-confluent` project.

**Typed labels** in the Marquez UI come from a small patch to `marquez-web`
(`marquez-ui/typed-labels.patch`): it replaces the generic `JOB`/`DATASET` node text with a
`nodeTypeLabel(namespace)` (Kafka Topic / Flink / Kafka Connect / Tableflow / Iceberg / DB2 Table / Spark).
`scripts/marquez_ui_patched.sh` clones marquez-web **pinned to `0.51.1`** (matches the stock image; `main`
is on React 19 and crashes), applies the patch, and runs its dev server on `:3000`. `make lineage-view`
offers a no-Node standalone equivalent (`lineage/render_graph.py` → `lineage/graph.html`).

**Optional live bridge:** the `openlineage-confluent` bridge can also run against this env to auto-refresh
the Connect→Flink→Tableflow middle from Confluent's APIs (needs a Cloud key with MetricsViewer +
Connect/cluster admin). Not wired here.
