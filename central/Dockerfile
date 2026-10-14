FROM gcc:13.2

RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-dev \
    make \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY Makefile db.h db.c WM_Central.c ./

CMD ["sh", "-c", "make && ./WM_Central.out 8080 127.0.0.1 9092"]
