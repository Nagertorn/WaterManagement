FROM gcc:latest

RUN apt-get update && apt-get install -y \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY include/ ./include/
COPY src/ ./src/
COPY Makefile ./
#COPY commands.txt ./  # Copiamos el fichero opcional de órdenes

RUN make clean && make

CMD ["bash"]
