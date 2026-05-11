# Riley Brown MLOPS Final Project

(For future reference), this is a distributed ML pipeline: Airflow → S3 → SQS → Kubernetes consumers.


Context: Most of the code in bulk was ran by AI (i.e modules like boto3 for AWS <-> Python), with my decisions from class material on how to structure the pipeline. ALSO, For whatever reason I could not get the breast cancer data to work, so I ended up just generating synthetic data, so that is why there are additional files for design. 

---

## Project Structure

```
.
├── config.py                    # Central config — reads from .env
├── requirements.txt             # Python dependencies
├── Dockerfile                   # Build from project root
├── .env.example                 # Copy to .env and fill in bucket/queue values

├── training_dag.py          # Airflow: generate data → train → upload to S3
├── queue_population_dag.py  # Airflow: read test set from S3 → enqueue to SQS
├── consumer.py              # Kubernetes: poll SQS → infer → write to S3
├── requirements.txt
├── deployment.yaml
```

---

## Setup (This is way more for me in the future than for you :D) 

### 1. Clone and install

```bash
git clone https://github.com/RileyBrown0/Homework-5
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

This prints your queue URL (copy it, you'll need it in the next step)

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

## Writeup Section

### 1. System End-to-End
The system is broken into two main flows: training and inference.

For training, an Airflow DAG kicks things off by generating a synthetic dataset of 200 samples using scikit-learn (since I could not figure out the breast cancer dataset). It splits that data into a training set and a test set, then trains a simple logistic regression model on the training portion. Once the model is done training, it gets serialized into a file called model.pkl and uploaded to an S3 bucket. The test set gets saved to S3 as well in a file called test_data.json, so it can be used later during inference without needing to regenerate it.

For inference, a second Airflow DAG reads the test data from S3 and sends one message per record into an SQS queue. Each message contains a record ID and the feature values for that sample. From there, a consumer application running inside a Kubernetes pod takes over. When the consumer starts up, it loads the trained model directly from S3. It then sits in a loop, constantly checking the SQS queue for new messages. When it finds one, it runs the model on those features to get a prediction, writes the result as a JSON file back to S3 under a predictions/folder, and then removes the message from the queue. This process repeats until the queue is empty.

### 2. Why a Queue Instead of Direct API Calls?
Using a queue instead of calling the consumer directly primarily is used here as it decouples the two sides of the system. Airflow does not need to know anything about the consumers; it just drops messages into the queue and moves on. The consumers do not need to know anything about Airflow, they just pull from the queue whenever they are ready. If the consumers are slow or one of them crashes, the messages just sit in the queue safely until a consumer is available to process them, and nothing would be getting lost. With a direct API call, if the consumer is not available at the exact moment Airflow tries to reach it, the request fails and that data is gone. The queue also makes it easy to scale; if the queue starts filling up, you can simply add more consumer pods and they will all start pulling from the same queue in parallel.

### 3. What Happens if a Consumer Crashes Mid-Processing?
This was a deliberate design decision I asked the AI to generate within the consumer code. The consumer only deletes a message from SQS after it has successfully written the prediction to S3. If the consumer crashes after writing to S3 but before deleting the message, SQS will eventually make that message visible again after the visibility timeout period expires, and another consumer will pick it up and process it. The prediction file will just get overwritten with the same result, which would be acceptable in processing. If the consumer crashes before it finishes writing to S3, the message also stays in the queue and gets reprocessed. This means every record is guaranteed to be processed at least once, even if something goes wrong partway through. The only outlier case would be if a crash happened repeatedly on the same message, which in production you would set a dead letter queue to catch those after a few failed attempts.

### 4. Bottlenecks
The main bottleneck is the consumer. Each pod processes one message at a time in a loop, so how fast the system runs depends almost entirely on how many consumer pods are running. A single pod can only go as fast as one prediction plus one S3 write per cycle. S3 write latency is a smaller but real bottleneck — every prediction requires a round trip to S3, which adds a bit of overhead per record. The SQS queue itself is not a bottleneck at this scale; it can handle thousands of messages per second and is not the limiting factor here.

### 5. One Production Improvement
Other than using the correct data to meet the criteria of the project, the biggest improvement for this specific pipeline would be switching from single-record inference to batch inference if I needed to scale exponentially. Right now, the consumer processes one message at a time, which means the model runs predict() on a single row per loop iteration. Instead, the consumer could pull a full batch of up to 10 messages at once (which SQS already supports), stack all the feature arrays into a single matrix, and call model.predict() once on the whole batch. This would cut down on repeated overhead and make much better use of cloud resources. Combined with the HorizontalPodAutoscaler already configured in the Kubernetes deployment, the system would be able to handle a much higher volume of requests without needing to add significantly more infrastructure.
