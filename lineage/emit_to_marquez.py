"""Phase 2 — emit OpenLineage events for the full pipeline to Marquez.

Reuses the conventions proven in the sibling `openlineage-confluent` project:
  * openlineage-python event_v2 API, HTTP transport → Marquez /api/v1/lineage
  * dataset namespaces  kafka://<bootstrap> and glue://<region>
  * job namespaces      kafka-connect://<env>, flink://<env>, tableflow://<env>
  * deterministic run ids, SchemaDatasetFacet + JobTypeJobFacet on every node
Marquez stitches the DAG purely by matching (namespace, name) across job
inputs/outputs — so the four jobs below link into one DB2 → Spark graph.

Unlike the auto-discovery bridge (which only sees the Kafka→Flink→Tableflow
middle), this hand-built graph includes BOTH external ends: the DB2 source
table and the Spark reader.

Run:  source .env && .venv/bin/python lineage/emit_to_marquez.py
Env:  MARQUEZ_URL (default http://localhost:5002)  — host port 5002, see
      docker-compose.marquez.yml for why it isn't the usual 5000 on macOS.
"""
from __future__ import annotations

import hashlib
import os
import uuid
from datetime import datetime, timedelta, timezone

from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import (
    InputDataset,
    Job,
    OutputDataset,
    Run,
    RunEvent,
    RunState,
)
from openlineage.client.facet_v2 import job_type_job, schema_dataset
from openlineage.client.transport.http import HttpConfig, HttpTransport

# ── config (env with sensible defaults for this pipeline) ─────────────────────
MARQUEZ_URL = os.environ.get("MARQUEZ_URL", "http://localhost:5002")
PRODUCER = "https://github.com/fereydunk/openlineage-pipeline/0.2.0"

ENV_ID = os.environ.get("ENV_ID", "env-m2qxq")
REGION = os.environ.get("AWS_REGION", "us-east-2")
BOOTSTRAP = os.environ.get(
    "KAFKA_BOOTSTRAP", "pkc-k6yr2m.us-east-2.aws.confluent.cloud:9092"
)
RAW_TOPIC = os.environ.get("DB2_TOPIC", "db2-orders")
ENR_TOPIC = os.environ.get("ENRICHED_TOPIC", "orders-enriched")
GLUE_DB = os.environ.get("AWS_GLUE_DATABASE", "olpipe")
DB2_DB = os.environ.get("DB2_DATABASE", "TESTDB")

# namespaces
KAFKA_NS = f"kafka://{BOOTSTRAP}"
GLUE_NS = f"glue://{REGION}"
DB2_NS = "db2://olpipe-db2:50000"
CONNECT_NS = f"kafka-connect://{ENV_ID}"
FLINK_NS = f"flink://{ENV_ID}"
TABLEFLOW_NS = f"tableflow://{ENV_ID}"
SPARK_NS = "spark://olpipe-local"


def schema(*fields: tuple[str, str]) -> schema_dataset.SchemaDatasetFacet:
    return schema_dataset.SchemaDatasetFacet(
        fields=[schema_dataset.SchemaDatasetFacetFields(name=n, type=t) for n, t in fields]
    )


# ── the four datasets of the chain ────────────────────────────────────────────
DS_DB2 = dict(
    namespace=DB2_NS,
    name=f"{DB2_DB}.DB2INST1.ORDERS",
    facets={"schema": schema(
        ("ID", "INTEGER"), ("ORDER_ID", "VARCHAR"), ("CUSTOMER", "VARCHAR"),
        ("ITEM", "VARCHAR"), ("QUANTITY", "INTEGER"), ("AMOUNT_USD", "DECIMAL"),
        ("ORDER_TS", "TIMESTAMP"),
    )},
)
DS_RAW = dict(
    namespace=KAFKA_NS,
    name=RAW_TOPIC,
    facets={"schema": schema(
        ("ORDER_ID", "string"), ("CUSTOMER", "string"), ("ITEM", "string"),
        ("QUANTITY", "int"), ("AMOUNT_USD", "bytes/decimal"), ("ID", "int"),
        ("ORDER_TS", "long/timestamp-millis"),
    )},
)
DS_ENR = dict(
    namespace=KAFKA_NS,
    name=ENR_TOPIC,
    facets={"schema": schema(
        ("ORDER_ID", "string"), ("CUSTOMER", "string"), ("ITEM", "string"),
        ("QUANTITY", "int"), ("AMOUNT_USD", "bytes/decimal"), ("RISK_TIER", "string"),
    )},
)
DS_ICEBERG = dict(
    namespace=GLUE_NS,
    name=f"{GLUE_DB}.{ENR_TOPIC}",
    facets={"schema": schema(
        ("order_id", "string"), ("customer", "string"), ("item", "string"),
        ("quantity", "int"), ("amount_usd", "decimal(10,2)"), ("risk_tier", "string"),
    )},
)

# ── the four jobs: (name, namespace, jobType, inputs, outputs) ─────────────────
JOBS = [
    ("db2-orders-source", CONNECT_NS,
     ("STREAMING", "KAFKA_CONNECT", "SOURCE_CONNECTOR"), [DS_DB2], [DS_RAW]),
    ("olpipe-enrich-orders", FLINK_NS,
     ("STREAMING", "FLINK", "QUERY"), [DS_RAW], [DS_ENR]),
    ("orders-enriched-tableflow", TABLEFLOW_NS,
     ("BATCH", "TABLEFLOW", "TABLE_SYNC"), [DS_ENR], [DS_ICEBERG]),
    ("olpipe-spark-reader", SPARK_NS,
     ("BATCH", "SPARK", "JOB"), [DS_ICEBERG], []),
]


def stable_run_id(job_ns: str, job_name: str) -> str:
    h = hashlib.sha256(f"{job_ns}:{job_name}:v2".encode()).digest()
    return str(uuid.UUID(bytes=h[:16], version=4))


def main() -> None:
    client = OpenLineageClient(transport=HttpTransport(HttpConfig(url=MARQUEZ_URL)))
    now = datetime.now(timezone.utc)
    emitted = 0

    for name, ns, (proc, integ, jtype), inputs, outputs in JOBS:
        rid = stable_run_id(ns, name)
        run = Run(runId=rid)
        job = Job(namespace=ns, name=name, facets={
            "jobType": job_type_job.JobTypeJobFacet(
                processingType=proc, integration=integ, jobType=jtype),
        })
        ins = [InputDataset(**d) for d in inputs]
        outs = [OutputDataset(**d) for d in outputs]

        start = RunEvent(
            eventType=RunState.START,
            eventTime=(now - timedelta(seconds=5)).isoformat(),
            run=run, job=job, inputs=ins, outputs=outs, producer=PRODUCER,
        )
        complete = RunEvent(
            eventType=RunState.COMPLETE,
            eventTime=now.isoformat(),
            run=run, job=job, inputs=ins, outputs=outs, producer=PRODUCER,
        )
        client.emit(start)
        client.emit(complete)
        emitted += 2
        src = ", ".join(d["name"] for d in inputs) or "—"
        dst = ", ".join(d["name"] for d in outputs) or "(terminal)"
        print(f"  ✓ {name:<26} {src}  →  {dst}")

    print(f"\nEmitted {emitted} events for {len(JOBS)} jobs to {MARQUEZ_URL}")
    print("Open the lineage graph:  http://localhost:3000")
    print("  (search the 'olpipe.orders-enriched' dataset or the 'db2-orders-source' job)")


if __name__ == "__main__":
    main()
