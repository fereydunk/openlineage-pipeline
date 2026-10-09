"""
Step 1 of the AWS/Tableflow setup — create the AWS resources Tableflow needs
(idempotent). No KMS (this pipeline doesn't encrypt):

  - S3 bucket     (AWS_S3_BUCKET, default olpipe-tableflow-<account>)  — Iceberg storage
  - Glue database (AWS_GLUE_DATABASE, default olpipe)                   — Iceberg catalog
  - IAM role      (olpipe-tableflow-confluent-role)                     — Confluent assumes this

Reads values from the environment (run `source .env` first) and AWS credentials
from the standard chain. Prints AWS_* to paste into .env, then run
`python3 infra/02_setup_tableflow.py`.
"""
from __future__ import annotations

import json
import os

import boto3
from botocore.exceptions import ClientError

ROLE_NAME = "olpipe-tableflow-confluent-role"

# Placeholder trust policy — filled in by 02_setup_tableflow.py once we have
# Confluent's IAM ARN + external id from the provider integration.
PLACEHOLDER_TRUST = {
    "Version": "2012-10-17",
    "Statement": [{
        "Effect": "Deny",
        "Principal": {"AWS": "*"},
        "Action": "sts:AssumeRole",
        "Condition": {"StringEquals": {"sts:ExternalId": "PLACEHOLDER_REPLACE_IN_STEP_02"}},
    }],
}


def create_s3_bucket(s3, region, bucket):
    try:
        if region == "us-east-1":
            s3.create_bucket(Bucket=bucket)
        else:
            s3.create_bucket(Bucket=bucket,
                             CreateBucketConfiguration={"LocationConstraint": region})
        s3.put_bucket_versioning(Bucket=bucket,
                                 VersioningConfiguration={"Status": "Enabled"})
        print(f"[S3] created s3://{bucket}")
    except ClientError as e:
        if e.response["Error"]["Code"] in ("BucketAlreadyOwnedByYou", "BucketAlreadyExists"):
            print(f"[S3] already exists: s3://{bucket}")
        else:
            raise


def create_glue_database(glue, db_name):
    try:
        glue.create_database(DatabaseInput={"Name": db_name})
        print(f"[Glue] created database {db_name}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "AlreadyExistsException":
            print(f"[Glue] database already exists: {db_name}")
        else:
            raise


def create_iam_role(iam, bucket):
    try:
        role_arn = iam.get_role(RoleName=ROLE_NAME)["Role"]["Arn"]
        print(f"[IAM] role already exists: {role_arn}")
    except ClientError as e:
        if e.response["Error"]["Code"] != "NoSuchEntity":
            raise
        role_arn = iam.create_role(
            RoleName=ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(PLACEHOLDER_TRUST),
            Description="Confluent Tableflow + Glue catalog integration role (openlineage-pipeline)",
            Tags=[{"Key": "project", "Value": "openlineage-pipeline"}],
        )["Role"]["Arn"]
        print(f"[IAM] created role: {role_arn}")

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Sid": "TableflowS3", "Effect": "Allow",
             "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket",
                        "s3:GetBucketLocation", "s3:AbortMultipartUpload",
                        "s3:ListMultipartUploadParts"],
             "Resource": [f"arn:aws:s3:::{bucket}", f"arn:aws:s3:::{bucket}/*"]},
            {"Sid": "TableflowGlue", "Effect": "Allow",
             "Action": ["glue:CreateDatabase", "glue:GetDatabase", "glue:CreateTable",
                        "glue:GetTable", "glue:UpdateTable", "glue:DeleteTable", "glue:GetTables",
                        "glue:BatchDeleteTable", "glue:GetPartition", "glue:GetPartitions",
                        "glue:BatchCreatePartition"],
             "Resource": ["*"]},
        ],
    }
    iam.put_role_policy(RoleName=ROLE_NAME, PolicyName=f"{ROLE_NAME}-policy",
                        PolicyDocument=json.dumps(policy))
    print(f"[IAM] attached inline policy {ROLE_NAME}-policy")
    return role_arn


def main():
    session = boto3.Session()
    region = os.environ.get("AWS_REGION") or session.region_name or "us-east-2"
    account_id = session.client("sts").get_caller_identity()["Account"]
    bucket = os.environ.get("AWS_S3_BUCKET") or f"olpipe-tableflow-{account_id}"
    glue_db = os.environ.get("AWS_GLUE_DATABASE", "olpipe")

    print(f"\n[Setup] account={account_id} region={region}\n")
    s3 = session.client("s3", region_name=region)
    glue = session.client("glue", region_name=region)
    iam = session.client("iam")

    create_s3_bucket(s3, region, bucket)
    create_glue_database(glue, glue_db)
    role_arn = create_iam_role(iam, bucket)

    print(f"""
┌─ Add these to .env, then run: python3 infra/02_setup_tableflow.py ─┐
  AWS_REGION={region}
  AWS_ACCOUNT_ID={account_id}
  AWS_S3_BUCKET={bucket}
  AWS_GLUE_DATABASE={glue_db}
  AWS_IAM_ROLE_ARN={role_arn}
└────────────────────────────────────────────────────────────────────┘
""")


if __name__ == "__main__":
    main()
