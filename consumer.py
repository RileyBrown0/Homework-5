"""
consumer/consumer.py
--------------------
Kubernetes consumer — polls SQS, runs inference, writes predictions to S3.

  - Loads model from S3 once on startup
  - Polls SQS in a loop with long-polling (10s wait)
  - Writes each prediction to S3 before deleting the message (at-least-once delivery)
  - On error, leaves the message in the queue so SQS redelivers after visibility timeout

boto3 picks up AWS credentials automatically from the Canvas/Learner Lab
environment — no explicit credential passing needed.
"""

import io
import os
import sys
import json
import time
import boto3
import joblib
import numpy as np
from datetime import datetime, timezone

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import config


def load_model():
    print("[consumer] Loading model from S3 ...")
    s3    = boto3.client("s3", region_name=config.AWS_REGION)
    resp  = s3.get_object(Bucket=config.AWS_S3_BUCKET, Key=config.S3_MODEL_KEY)
    model = joblib.load(io.BytesIO(resp["Body"].read()))
    print("[consumer] Model loaded.")
    return model


def write_prediction(s3, record_id: str, prediction: int):
    output = {
        "record_id":  record_id,
        "prediction": prediction,
        "timestamp":  datetime.now(timezone.utc).isoformat(),
    }
    key = f"{config.S3_PREDS_PREFIX}/{record_id}.json"
    s3.put_object(
        Bucket=config.AWS_S3_BUCKET,
        Key=key,
        Body=json.dumps(output, indent=2).encode(),
    )
    print(f"[consumer]   Written s3://{config.AWS_S3_BUCKET}/{key}")


def run():
    model = load_model()
    s3    = boto3.client("s3",  region_name=config.AWS_REGION)
    sqs   = boto3.client("sqs", region_name=config.AWS_REGION)
    print(f"[consumer] Polling {config.AWS_SQS_URL} ...\n")

    while True:
        resp = sqs.receive_message(
            QueueUrl=config.AWS_SQS_URL,
            MaxNumberOfMessages=config.SQS_BATCH_SIZE,
            WaitTimeSeconds=10,
        )
        messages = resp.get("Messages", [])

        if not messages:
            print(f"[consumer] Queue empty. Waiting {config.POLL_INTERVAL}s ...")
            time.sleep(config.POLL_INTERVAL)
            continue

        for msg in messages:
            receipt   = msg["ReceiptHandle"]
            body      = json.loads(msg["Body"])
            record_id = body["record_id"]
            features  = body["features"]

            print(f"[consumer] Processing {record_id} ...")
            try:
                X          = np.array(features, dtype=float).reshape(1, -1)
                prediction = int(model.predict(X)[0])

                # Write to S3 BEFORE deleting — no lost predictions on crash
                write_prediction(s3, record_id, prediction)
                sqs.delete_message(QueueUrl=config.AWS_SQS_URL, ReceiptHandle=receipt)
                print(f"[consumer]   {record_id} → prediction={prediction} ✓")

            except Exception as exc:
                print(f"[consumer]   ERROR on {record_id}: {exc}. Message NOT deleted.")


if __name__ == "__main__":
    run()
