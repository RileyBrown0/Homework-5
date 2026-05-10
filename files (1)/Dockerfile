# Build from the PROJECT ROOT:
#   docker build -t inference-consumer .

FROM python:3.11-slim

WORKDIR /app

COPY consumer/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py       ./config.py
COPY consumer/consumer.py ./consumer.py

CMD ["python", "consumer.py"]
