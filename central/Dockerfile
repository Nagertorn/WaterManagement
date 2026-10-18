FROM gcc:13.2

RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-dev \
    librdkafka-dev \
    make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Makefile ./
COPY include/ ./include/
COPY src/ ./src/

RUN mkdir -p data

RUN make

EXPOSE 8080

ENTRYPOINT ["/app/WM_Central.out"]
