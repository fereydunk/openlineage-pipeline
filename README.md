# openlineage-pipeline

A concrete, runnable end-to-end data pipeline across the Confluent + open-source lakehouse stack:

```
IBM Db2 (Docker)
   │  Confluent JDBC Source connector  (self-managed Kafka Connect, Docker)
   ▼
Kafka topic  db2-orders            ── Confluent Cloud
   │  Flink SQL  (CREATE TABLE AS SELECT — enrich with a RISK_TIER)
   ▼
Kafka topic  orders-enriched       ── Confluent Cloud
   │  Tableflow  (BYOS → Apache Iceberg on S3, catalogued in AWS Glue)
   ▼
Iceberg table  olpipe.orders_enriched   ── S3 + Glue
   │  Apache Spark (OSS)  — reads straight from Glue + S3, no Confluent dependency
   ▼
spark.sql(...)  → rows + "spend by risk tier" aggregation   (the end consumer)
```

**Pipeline-first** demo: the goal is getting data to flow end to end. Db2 and the Connect worker run
locally in Docker; Kafka, Flink and Tableflow run on Confluent Cloud; the Iceberg table lands in your
AWS S3 + Glue; and **Spark** (open source) is the final consumer that queries it. OpenLineage lineage
emission across the whole chain is a planned **phase 2** (reusing the sibling `openlineage-confluent`
bridge) — this repo is structured for it but doesn't build it yet.

## Prerequisites

- **Docker / OrbStack** running (for Db2 + the Connect worker).
- **`confluent` CLI** logged in (`confluent login --save`).
- **Python 3.11+** (a `.venv` is created by `make venv`).
- **Java 21** at `/opt/homebrew/opt/openjdk@21` (`brew install openjdk@21`) — for the Spark reader.
- **AWS credentials** for the account behind your Confluent Cloud cluster (S3 + Glue + IAM), exported
  in your shell (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`). Needed from
  `make aws-setup` onward.
- A Confluent Cloud env with a **Flink compute pool** and **Tableflow** available (defaults target the
  shared `env-m2qxq` / `lkc-w71g5v5` in us-east-2, pool `lfcp-6kgnyv6`).

## Quick start

```bash
bash startup.sh        # preflight + prints the ordered steps

make venv              # Python deps
make discover          # resolve SR endpoint + mint CC API keys into .env
make up                # build & start DB2 + Connect (DB2 ~3-5 min first boot)
make seed              # create the ORDERS table + sample rows in DB2
make topics            # create db2-orders on Confluent Cloud
make connector         # deploy the JDBC source  (DB2 -> db2-orders)
make flink             # Flink statement         (db2-orders -> orders-enriched)
make aws-setup         # create S3 + Glue + IAM; paste the printed AWS_* into .env
make tableflow         # enable Tableflow (Iceberg) on orders-enriched
make produce-more      # add rows to trigger the first Tableflow S3 flush
make read              # Spark reads the Iceberg table + aggregates
```

`make status` shows connector/Flink/Tableflow state at any point. `make down` stops the containers;
`make clean` also deletes the Flink statement and connector (CC topics, Tableflow and AWS resources are
left for you to remove explicitly).

## What you should observe

| Stage | Check | Expect |
|---|---|---|
| Db2 | `docker exec olpipe-db2 … db2 "select * from ORDERS"` | the seeded rows |
| JDBC → Kafka | `confluent kafka topic consume db2-orders --from-beginning` | Avro order records |
| Flink | `confluent flink statement list` | `olpipe-enrich-orders` RUNNING; `orders-enriched` fills |
| Tableflow | `aws s3 ls s3://$AWS_S3_BUCKET/ --recursive \| grep metadata.json` | an Iceberg commit |
| Spark | `make read` | rows + a "spend by RISK_TIER" aggregation from Iceberg |

## Confluent Cloud / AWS targets

- **CC**: env `env-m2qxq` (DEVTEST), cluster `lkc-w71g5v5` (us-east-2), Flink pool `lfcp-6kgnyv6`,
  SR `lsrc-jwp0w`. Reuses the shared demo env — all IDs live in `.env`.
- **AWS**: account `586051073099`, region `us-east-2`, S3 `olpipe-tableflow-586051073099`, Glue DB
  `olpipe`, IAM role `olpipe-tableflow-confluent-role`.
- **Topics**: `db2-orders` (raw) → `orders-enriched` (Flink out → Tableflow→Iceberg).

## Repository layout

```
docker-compose.yml        DB2 + self-managed Connect (wired to Confluent Cloud)
connect/
  Dockerfile              cp-kafka-connect + JDBC connector + Db2 jcc driver
  jdbc-db2-source.json    JDBC source config (DB2 -> db2-orders, RegexRouter to the topic)
db2/init.sql              ORDERS table + seed rows
flink/enrich-orders.sql   CTAS: db2-orders -> orders-enriched (+ RISK_TIER)
infra/
  01_create_aws_resources.py   S3 + Glue + IAM role (idempotent)
  02_setup_tableflow.py        provider integration + enable Tableflow + Glue catalog
reader/
  spark_read.py           the end consumer: Spark reads the Iceberg table + aggregates
  quick_read.py           optional no-JVM pyiceberg sanity read
run_spark.sh              downloads Iceberg JARs + Java 21 + spark-submit
scripts/00-03             discover env · create topic · deploy connector · create Flink statement
Makefile · startup.sh · .env.example · requirements.txt
```

## Documentation
- [ARCHITECTURE.md](ARCHITECTURE.md) — the end-to-end flow, trust/ownership boundaries, schema handling.
- [TROUBLESHOOTING.md](TROUBLESHOOTING.md) — common failure points (DB2 boot, Connect→CC, Tableflow flush, Spark/AWS creds).

## Scope / future
- **Phase 2 — OpenLineage**: point the `openlineage-confluent` bridge at this env to emit lineage for the
  Connect → Flink → Tableflow middle automatically, and add two custom edges (Db2 source, Spark/Iceberg
  sink) so the whole DB2→Spark chain renders in Marquez.
- A web wizard (the sibling repos' stdlib-HTTP + cards pattern) could wrap these steps later; for now the
  Makefile is the driver.
