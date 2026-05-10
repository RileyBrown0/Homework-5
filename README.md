# Riley Brown: MLOPS Final Project

## Project Structure

```
.
├── config.py                    # Central config — reads from environment / .env
├── requirements.txt             # Python dependencies
├── Dockerfile                   # Build from project root
├── .env.example                 # Copy to .env and fill in credentials
├── .gitignore
├── dags/
│   ├── training_dag.py          # Airflow: generate data → train → upload to S3
│   └── queue_population_dag.py  # Airflow: read test set from S3 → enqueue to SQS
├── consumer/
│   ├── consumer.py              # Kubernetes: poll SQS → infer → write to S3
│   └── requirements.txt
└── k8s/
    └── deployment.yaml          # Kubernetes Deployment + HorizontalPodAutoscaler
```

---

## Setup

### 1. Clone and install

```bash
git clone https://github.com/YOUR_USERNAME/YOUR_REPO.git
cd YOUR_REPO

python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Credentials

```bash
cp .env.example .env
```

Fill in `.env`:
```
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=your_key
AWS_SECRET_ACCESS_KEY=your_secret
AWS_S3_BUCKET=your-unique-bucket-name
AWS_SQS_QUEUE_NAME=inference-queue
```

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

### Push to ECR

```bash
aws ecr create-repository --repository-name inference-consumer --region us-east-1

docker tag inference-consumer:latest \
    <account_id>.dkr.ecr.us-east-1.amazonaws.com/inference-consumer:latest

aws ecr get-login-password --region us-east-1 \
    | docker login --username AWS \
      --password-stdin <account_id>.dkr.ecr.us-east-1.amazonaws.com

docker push <account_id>.dkr.ecr.us-east-1.amazonaws.com/inference-consumer:latest
```

### Deploy to Kubernetes

```bash
kubectl create secret generic aws-inference-secrets \
    --from-literal=region=$AWS_REGION \
    --from-literal=access-key-id=$AWS_ACCESS_KEY_ID \
    --from-literal=secret-access-key=$AWS_SECRET_ACCESS_KEY \
    --from-literal=s3-bucket=$AWS_S3_BUCKET \
    --from-literal=sqs-url=$AWS_SQS_URL

kubectl apply -f k8s/deployment.yaml

# Check pods
kubectl get pods

# Scale manually
kubectl scale deployment inference-consumer --replicas=3

# View logs
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
