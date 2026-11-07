FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libc6-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir flask confluent-kafka

COPY app.py .

EXPOSE 5000

CMD ["python", "app.py"]
