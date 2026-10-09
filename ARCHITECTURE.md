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
                                       │  Glue  olpipe.orders_enriched  (Iceberg catalog)     │
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
2. Your IAM role's **trust policy** is updated to let that principal `AssumeRole` (with the ExternalId).
3. **Enable Tableflow** on the topic (`POST /tableflow/v1/tableflow-topics`, `storage.kind=CustomerManaged`,
   `table_formats=["ICEBERG"]`).
4. A **Glue catalog integration** (`POST /tableflow/v1/catalog-integrations`, `kind=AwsGlue`) registers the
   Iceberg table in Glue. Table name = topic with `-`→`_` (`orders_enriched`).

**4. Spark (open source) — the end consumer.**
`reader/spark_read.py` builds a SparkSession with the Iceberg extensions and a `glue` catalog
(`SparkCatalog` → `GlueCatalog`, warehouse `s3://…`, `S3FileIO`), then `spark.table("glue.olpipe.orders_enriched")`
→ `.show()` + a `GROUP BY RISK_TIER` aggregation. **No Confluent dependency at query time** — pure
Glue + S3. `run_spark.sh` downloads the Iceberg Spark runtime + AWS bundle JARs and pins Java 21.

## Ownership / trust boundaries
- **Confluent Cloud** holds the Kafka topics, runs Flink, and operates Tableflow — but the Iceberg data
  lands in **your** S3 and is catalogued in **your** Glue (BYOS). Confluent assumes a role you control.
- **AWS credentials** stay on your laptop / in your shell (Spark reads via the env-var credential provider;
  Tableflow uses the AssumeRole grant).
- **Db2 credentials** are local-only (Docker + `.env`); they never leave the Connect worker.

## Schema handling
- Db2 column names are UPPERCASE → Avro field names UPPERCASE → Flink columns UPPERCASE → Iceberg columns
  UPPERCASE. Spark references them as-is.
- Tableflow adds operational columns (`$$partition`, `$$offset`) and the SR `schema_id`; these must be
  backtick-quoted in Spark SQL if referenced.

## Phase 2 — OpenLineage (not built yet)
The repo is named for lineage. Later, run the `openlineage-confluent` bridge against this env: it emits
OpenLineage for the Connect → Flink → Tableflow middle automatically. Add two custom edges at its seams
(`confluent/models.py::LineageEdge`, `client.py::build_graph`, `mapping/mapper.py`, `emitter/emitter.py`)
for the Db2 source and the Spark/Iceberg sink, so the full DB2 → … → Spark chain renders in Marquez.
