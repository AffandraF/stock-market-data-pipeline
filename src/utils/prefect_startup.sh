#!/bin/bash
set -e

echo "Waiting for Prefect server..."
until curl -s http://prefect-server:4200/api/health > /dev/null; do
  sleep 2
done

echo "Prefect server is ready"

if ! prefect work-pool ls | grep -q default-pool; then
  echo "Creating work pool: default-pool"
  prefect work-pool create default-pool -t process
else
  echo "Work pool already exists"
fi

sleep 10

echo ">>> Deploying daily-stock-etl flow"
prefect deploy src/flows.py:daily_stock_etl \
    -n "daily-stock-etl" \
    --pool default-pool

echo ">>> Adding schedule (cron: 17:00 every day)"
prefect deployment schedule create daily-stock-etl/daily-stock-etl --cron "0 17 * * *" || true

echo ">>> Starting Prefect agent"
prefect worker start --pool default-pool