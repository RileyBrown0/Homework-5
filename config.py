"""
config.py
---------
Central config for the pipeline.

AWS credentials are NOT set here. When running in an AWS Academy / Canvas
Learner Lab environment, boto3 automatically picks up temporary session
credentials from the environment. No credential handling needed.

The only values you need to set are AWS_S3_BUCKET and AWS_SQS_URL.
Run setup_aws.py once to create those resources and get the values.
"""

import os
from dotenv import load_dotenv

load_dotenv()

# ── AWS region ─────────────────────────────────────────────────────────────────
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

# ── AWS resource names (the only two values you must set) ─────────────────────
AWS_S3_BUCKET = os.environ["AWS_S3_BUCKET"]  # e.g. my-inference-bucket
AWS_SQS_URL   = os.environ["AWS_SQS_URL"]    # printed by setup_aws.py

# ── S3 key paths ───────────────────────────────────────────────────────────────
S3_MODEL_KEY    = "model.pkl"
S3_TEST_KEY     = "test_data.json"
S3_PREDS_PREFIX = "predictions"

# ── Consumer settings ──────────────────────────────────────────────────────────
POLL_INTERVAL  = int(os.getenv("POLL_INTERVAL",  "3"))
SQS_BATCH_SIZE = int(os.getenv("SQS_BATCH_SIZE", "10"))

# ── Dataset settings ───────────────────────────────────────────────────────────
N_SAMPLES   = int(os.getenv("N_SAMPLES",   "200"))
N_FEATURES  = int(os.getenv("N_FEATURES",  "10"))
TEST_SIZE   = float(os.getenv("TEST_SIZE", "0.2"))
RANDOM_SEED = int(os.getenv("RANDOM_SEED", "42"))
