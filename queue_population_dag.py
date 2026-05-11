"""
dags/queue_population_dag.py
----------------------------
Airflow DAG — Queue Population Flow
  1. Read test_data.json from S3
  2. Send one SQS message per record

"""

import os
import sys
import json
import boto3
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import config

from airflow import DAG
from airflow.operators.python import PythonOperator


def task_populate_queue(**kwargs):
    s3  = boto3.client("s3",  region_name=config.AWS_REGION)
    sqs = boto3.client("sqs", region_name=config.AWS_REGION)

    # Read test records from S3
    resp = s3.get_object(Bucket=config.AWS_S3_BUCKET, Key=config.S3_TEST_KEY)
    records = json.loads(resp["Body"].read().decode())
    print(f"[queue_pop] Loaded {len(records)} test records from S3")

    # Send one message per record in batches of 10 (SQS limit per batch call)
    messages = [
        {"record_id": r["record_id"], "features": r["features"]}
        for r in records
    ]
    for i in range(0, len(messages), 10):
        batch = messages[i : i + 10]
        entries = [
            {"Id": str(j), "MessageBody": json.dumps(msg)}
            for j, msg in enumerate(batch)
        ]
        sqs.send_message_batch(QueueUrl=config.AWS_SQS_URL, Entries=entries)

    print(f"[queue_pop] Enqueued {len(messages)} messages to SQS")


with DAG(
    dag_id="queue_population_dag",
    default_args={"owner": "airflow", "start_date": datetime(2026, 1, 1)},
    schedule_interval=None,
    catchup=False,
    description="Read test data from S3 and send records to SQS",
) as dag:
    PythonOperator(
        task_id="populate_queue",
        python_callable=task_populate_queue,
    )
