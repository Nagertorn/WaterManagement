#!/bin/bash
/app/WM_Central.out "${CENTRAL_PORT:-8080}" "${KAFKA_BOOTSTRAP_SERVERS}" &

python /app/app.py