#!/bin/bash
/app/WM_Central.out "${CENTRAL_PORT:-8080}" "${KAFKA_BOOTSTRAP_SERVERS}" &

python3 /app/app.py