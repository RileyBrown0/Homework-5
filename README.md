# Riley Brown MLOPS Final Project

(For future reference), this is a distributed ML pipeline: Airflow → S3 → SQS → Kubernetes consumers.


Context: Most of the code in bulk was ran by AI, with my decisions from class material on how to structure the pipeline. For whatever reason I could not get the breast cancer data to work, so I ended up just generating synthetic data.

---

## Project Structure

```
.
├── config.py                    # Central config — reads from .env
├── requirements.txt             # Python dependencies
├── Dockerfile                   # Build from project root
├── .env.example                 # Copy to .env and fill in bucket/queue values
├── .gitignore
├── dags/
│   ├── training_dag.py          # Airflow: generate data → train → upload to S3
│   └── queue_population_dag.py  # Airflow: read test set from S3 → enqueue to SQS
├── consumer/
│   ├── consumer.py              # Kubernetes: poll SQS → infer → write to S3
│   └── requirements.txt
└── k8s/
    └── deployment.yaml
```

---

## Setup (do this once after cloning)

### 1. Clone and install

```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Create your AWS resources in the Cloud9 terminal

**Create the S3 bucket:**
```bash
aws s3 mb s3://your-unique-bucket-name --region us-east-1
```

**Create the SQS queue:**
```bash
aws sqs create-queue --queue-name inference-queue --region us-east-1
```

This prints your queue URL — copy it, you'll need it in the next step.

**Verify both were created:**
```bash
aws s3 ls
aws sqs list-queues
```

### 3. Create your .env

```bash
cp .env.example .env
```

Fill in `.env` with the bucket name you chose and the SQS URL printed above:
```
AWS_REGION=us-east-1
AWS_S3_BUCKET=your-unique-bucket-name
AWS_SQS_URL=https://sqs.us-east-1.amazonaws.com/123456789/inference-queue
```

> Do NOT add AWS credentials. Your Canvas/Learner Lab session handles that automatically.

---

## Running the Pipeline

### Step 1 — Start Airflow

```bash
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create --username admin --password admin \
    --firstname Air --lastname Flow --role Admin --email admin@example.com

cp dags/*.py ~/airflow/dags/

# Two terminals:
airflow scheduler
airflow webserver --port 8080
```

### Step 2 — Trigger DAGs

Open http://localhost:8080, trigger **training_dag** then **queue_population_dag**.

### Step 3 — Run the consumer

```bash
python consumer/consumer.py
```

---

## Docker + Kubernetes

### Build (from project root)

```bash
docker build -t inference-consumer:latest .
```

### Deploy

```bash
kubectl create secret generic aws-inference-secrets \
    --from-literal=region=$AWS_REGION \
    --from-literal=s3-bucket=$AWS_S3_BUCKET \
    --from-literal=sqs-url=$AWS_SQS_URL

kubectl apply -f k8s/deployment.yaml

kubectl get pods
kubectl scale deployment inference-consumer --replicas=3
kubectl logs -l app=inference-consumer -f
```

---

## Writeup

### 1. System End-to-End
The Airflow training DAG generates 200 synthetic samples, trains a logistic regression model, and uploads `model.pkl` and `test_data.json` to S3. The queue population DAG reads the test set from S3 and pushes one JSON message per record to SQS. Kubernetes consumers load the model from S3 on startup, then continuously long-poll SQS. Each record is inferred and written as a timestamped JSON file to S3; the message is only deleted after a confirmed write.

### 2. Why a Queue Instead of Direct API Calls?
A queue decouples producers from consumers so each scales independently. If consumers are busy or temporarily down, messages accumulate safely in SQS rather than being dropped. It also absorbs traffic bursts without overwhelming a synchronous endpoint.

### 3. What Happens if a Consumer Crashes Mid-Processing?
Messages are only deleted from SQS after a successful S3 write. If a pod crashes before that point, SQS makes the message visible again after the visibility timeout expires and another pod picks it up automatically. No data is lost.

### 4. Bottlenecks
Consumer throughput is the primary bottleneck — each pod processes records serially. S3 write latency adds overhead per record. The SQS queue itself is effectively unlimited and is not a bottleneck.

### 5. One Production Improvement
Batch inference: accumulate a full SQS batch (up to 10 records) and call `model.predict()` once on the entire matrix instead of one row at a time. This reduces per-record CPU overhead significantly and pairs naturally with the HorizontalPodAutoscaler for high-throughput workloads.
