FROM gcc:13.2

RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-dev \
    librdkafka-dev \
    python3 \
    python3-pip \
    make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Makefile ./
COPY include/ ./include/
COPY src/ ./src/
RUN mkdir -p data
RUN make

COPY dashboard/app.py ./app.py

RUN pip3 install --no-cache-dir --break-system-packages flask confluent-kafka

COPY entrypoint.sh ./entrypoint.sh
RUN chmod +x entrypoint.sh

EXPOSE 8080
EXPOSE 5000

ENTRYPOINT ["./entrypoint.sh"]