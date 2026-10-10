"""
Step 2 of the AWS/Tableflow setup — wire Confluent Cloud to the AWS resources
and enable Tableflow (Iceberg) on the enriched topic. No KMS/schema (unencrypted).

  1. Create a Confluent provider integration → returns Confluent's IAM ARN + external id
  2. Update the IAM role's trust policy to let that ARN assume it (with ExternalId)
  3. Enable Tableflow on ENRICHED_TOPIC with customer-managed S3 (table_formats=ICEBERG)
  4. Create the AWS Glue catalog integration (Iceberg metadata → Glue)

Prereqs (run `source .env` first):
  - AWS_* from infra/01_create_aws_resources.py (AWS_IAM_ROLE_ARN, AWS_S3_BUCKET, AWS_REGION)
  - CLOUD_API_KEY / CLOUD_API_SECRET — a Cloud resource-management API key
    (confluent.cloud > Administration > API keys > Cloud resource management)
  - ENV_ID, CLUSTER_ID, ENRICHED_TOPIC
  - AWS credentials in the environment
"""
from __future__ import annotations

import json
import os
import time

import boto3
import requests

CONFLUENT_API = "https://api.confluent.cloud"
IAM_ROLE_NAME = "olpipe-tableflow-confluent-role"
ENV_FILE = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env")


def need(name):
    v = os.environ.get(name)
    if not v:
        raise SystemExit(f"ERROR: {name} not set — run `source .env` and ensure it is populated.")
    return v


def auth():
    return (need("CLOUD_API_KEY"), need("CLOUD_API_SECRET"))


def cc_get(a, path, **params):
    r = requests.get(f"{CONFLUENT_API}{path}", auth=a, params=params, timeout=20)
    r.raise_for_status()
    return r.json()


def cc_post(a, path, body):
    r = requests.post(f"{CONFLUENT_API}{path}", auth=a, json=body, timeout=30)
    if not r.ok:
        print(f"  CC error {r.status_code}: {r.text[:500]}")
        r.raise_for_status()
    return r.json()


def cc_delete(a, path, **params):
    r = requests.delete(f"{CONFLUENT_API}{path}", auth=a, params=params, timeout=20)
    if not r.ok and r.status_code != 404:
        print(f"  CC error {r.status_code}: {r.text[:200]}")


def upsert_env(key, value):
    lines = open(ENV_FILE).read().splitlines() if os.path.exists(ENV_FILE) else []
    out, seen = [], False
    for ln in lines:
        if ln.split("=", 1)[0].strip() == key if "=" in ln else False:
            out.append(f"{key}={value}"); seen = True
        else:
            out.append(ln)
    if not seen:
        out.append(f"{key}={value}")
    open(ENV_FILE, "w").write("\n".join(out) + "\n")


# ── Step 1: provider integration ────────────────────────────────────────────
def create_provider_integration(a, env_id, role_arn):
    resp = cc_get(a, "/pim/v1/integrations", environment=env_id)
    for pi in resp.get("data", []):
        if pi.get("config", {}).get("customer_iam_role_arn") == role_arn:
            print(f"[PI] already exists: {pi['id']}")
            return pi["id"], pi["config"]["iam_role_arn"], pi["config"]["external_id"]
    resp = cc_post(a, "/pim/v1/integrations", {
        "api_version": "pim/v1", "kind": "Integration",
        "display_name": "olpipe-tableflow-pi", "provider": "aws",
        "environment": {"id": env_id},
        "config": {"kind": "AwsIntegrationConfig", "customer_iam_role_arn": role_arn},
    })
    print(f"[PI] created: {resp['id']}")
    return resp["id"], resp["config"]["iam_role_arn"], resp["config"]["external_id"]


# ── Step 2: IAM trust policy ────────────────────────────────────────────────
def update_trust_policy(iam, confluent_arn, external_id):
    from botocore.exceptions import ClientError
    # Confluent assumes the role WITH session tags, so the trust policy needs BOTH
    # sts:AssumeRole (gated by the ExternalId) and sts:TagSession (no condition).
    # Omitting TagSession fails with "Unable to assume the IAM role".
    trust = {"Version": "2012-10-17", "Statement": [
        {"Effect": "Allow", "Principal": {"AWS": confluent_arn}, "Action": "sts:AssumeRole",
         "Condition": {"StringEquals": {"sts:ExternalId": external_id}}},
        {"Effect": "Allow", "Principal": {"AWS": confluent_arn}, "Action": "sts:TagSession"},
    ]}
    # AWS may reject the just-created cross-account Confluent role as an "Invalid
    # principal" until it propagates — retry with backoff.
    last = None
    for attempt in range(1, 13):
        try:
            iam.update_assume_role_policy(RoleName=IAM_ROLE_NAME, PolicyDocument=json.dumps(trust))
            print(f"[IAM] trust updated → {confluent_arn} (externalId {external_id})")
            return
        except ClientError as e:
            if e.response["Error"]["Code"] == "MalformedPolicyDocument":
                last = e
                print(f"  [IAM] principal not valid yet (attempt {attempt}/12); waiting 10s...")
                time.sleep(10)
            else:
                raise
    raise SystemExit(f"IAM trust update kept failing: {last}")


# ── Step 3: enable Tableflow (customer-managed S3) ──────────────────────────
def setup_tableflow_topic(a, env_id, cluster_id, cspi_id, bucket, region, topic):
    path = f"/tableflow/v1/tableflow-topics/{topic}"
    r = requests.get(f"{CONFLUENT_API}{path}", auth=a,
                     params={"environment": env_id, "spec.kafka_cluster": cluster_id}, timeout=20)
    if r.ok:
        if r.json().get("spec", {}).get("storage", {}).get("kind") == "CustomerManaged":
            print("[TF] already customer-managed"); return
        print("[TF] switching to customer-managed (deleting current entry)...")
        cc_delete(a, path, environment=env_id, **{"spec.kafka_cluster": cluster_id})
        time.sleep(5)
    cc_post(a, "/tableflow/v1/tableflow-topics", {
        "spec": {
            "display_name": topic, "kafka_cluster": {"id": cluster_id}, "environment": {"id": env_id},
            "storage": {"kind": "CustomerManaged", "provider_integration": {"id": cspi_id},
                        "bucket_name": bucket, "bucket_region": region},
            "table_formats": ["ICEBERG"],
            "config": {"enable_compaction": True, "enable_partitioning": True,
                       "record_failure_strategy": "SUSPEND"},
        },
    })
    print(f"[TF] enabled on {topic} → s3://{bucket}")


# ── Step 4: Glue catalog integration ────────────────────────────────────────
def create_glue_catalog_integration(a, env_id, cluster_id, cspi_id):
    resp = cc_get(a, "/tableflow/v1/catalog-integrations",
                  environment=env_id, **{"spec.kafka_cluster": cluster_id})
    for ci in resp.get("data") or []:
        if ci.get("spec", {}).get("kind") == "AwsGlue":
            print(f"[CI] Glue catalog integration already exists: {ci['id']}"); return
    resp = cc_post(a, "/tableflow/v1/catalog-integrations", {
        "spec": {"display_name": "olpipe-glue-catalog", "kafka_cluster": {"id": cluster_id},
                 "environment": {"id": env_id}, "kind": "AwsGlue",
                 "provider_integration": {"id": cspi_id}},
    })
    print(f"[CI] created Glue catalog integration: {resp.get('id')}")


def main():
    a = auth()
    env_id = need("ENV_ID"); cluster_id = need("CLUSTER_ID")
    role_arn = need("AWS_IAM_ROLE_ARN"); bucket = need("AWS_S3_BUCKET")
    region = os.environ.get("AWS_REGION", "us-east-2")
    topic = os.environ.get("ENRICHED_TOPIC", "orders-enriched")
    iam = boto3.Session().client("iam")

    print(f"\n[Tableflow setup] env={env_id} cluster={cluster_id} topic={topic} bucket={bucket}\n")
    cspi_id, confluent_arn, external_id = create_provider_integration(a, env_id, role_arn)
    upsert_env("TABLEFLOW_PROVIDER_INTEGRATION_ID", cspi_id)
    update_trust_policy(iam, confluent_arn, external_id)
    time.sleep(8)  # let the trust policy propagate before Tableflow assumes the role
    setup_tableflow_topic(a, env_id, cluster_id, cspi_id, bucket, region, topic)
    create_glue_catalog_integration(a, env_id, cluster_id, cspi_id)
    print("\n✓ Tableflow enabled. Produce enough rows to trigger an S3 flush, then: make read")


if __name__ == "__main__":
    main()
