"""
dags/training_dag.py
--------------------
Airflow DAG — Training Flow
  1. Generate synthetic classification data and split into train / test
  2. Train a logistic regression model
  3. Upload model.pkl and test_data.json to S3

boto3 picks up AWS credentials automatically from the Canvas/Learner Lab
environment — no explicit credential passing needed.
"""

import io
import os
import sys
import json
import boto3
import joblib
import numpy as np
from sklearn.datasets import make_classification
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from datetime import datetime

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import config

from airflow import DAG
from airflow.operators.python import PythonOperator


def task_generate_and_train(**kwargs):
    s3 = boto3.client("s3", region_name=config.AWS_REGION)

    # 1. Generate synthetic data
    X, y = make_classification(
        n_samples=config.N_SAMPLES,
        n_features=config.N_FEATURES,
        n_informative=6,
        random_state=config.RANDOM_SEED,
    )
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=config.TEST_SIZE, random_state=config.RANDOM_SEED
    )
    print(f"[training] Split: {len(X_train)} train / {len(X_test)} test")

    # 2. Train
    model = LogisticRegression(max_iter=500, random_state=config.RANDOM_SEED)
    model.fit(X_train, y_train)
    print(f"[training] Model trained. Classes: {model.classes_}")

    # 3. Upload model to S3
    buf = io.BytesIO()
    joblib.dump(model, buf)
    buf.seek(0)
    s3.put_object(Bucket=config.AWS_S3_BUCKET, Key=config.S3_MODEL_KEY, Body=buf.read())
    print(f"[training] Uploaded s3://{config.AWS_S3_BUCKET}/{config.S3_MODEL_KEY}")

    # 4. Upload test records to S3
    test_records = [
        {
            "record_id": f"sample_{i:03d}",
            "features":  row.tolist(),
            "true_label": int(label),
        }
        for i, (row, label) in enumerate(zip(X_test, y_test))
    ]
    s3.put_object(
        Bucket=config.AWS_S3_BUCKET,
        Key=config.S3_TEST_KEY,
        Body=json.dumps(test_records).encode(),
    )
    print(f"[training] Uploaded {len(test_records)} test records to s3://{config.AWS_S3_BUCKET}/{config.S3_TEST_KEY}")


with DAG(
    dag_id="training_dag",
    default_args={"owner": "airflow", "start_date": datetime(2026, 1, 1)},
    schedule_interval=None,
    catchup=False,
    description="Train model on synthetic data and upload to S3",
) as dag:
    PythonOperator(
        task_id="generate_and_train",
        python_callable=task_generate_and_train,
    )
