#!/bin/bash

# check_containers.sh
# End-to-end validation script for the e-commerce recommendation stack

set -e  # Exit on any error

# Detect if running inside Docker or on host
if [ -f /.dockerenv ]; then
    # Inside Docker container
    HOST="app"
    POSTGRES_HOST="postgres"
else
    # On host machine
    HOST="127.0.0.1"
    POSTGRES_HOST="localhost"
fi


echo "Running Stack Validation Tests"



# Test 1: FastAPI Health Check
echo "Test 1: FastAPI Health Check"
echo "────────────────────────────────────────────────"
if curl -f -s "http://${HOST}:8000/health" > /tmp/health.json; then
    echo "✔ FastAPI health OK"
    cat /tmp/health.json
    echo ""
else
    echo "FastAPI health check failed"
    exit 1
fi

echo ""

# Test 2: Postgres Connection & Sample Query (Orders)
echo "Test 2: Postgres - Orders Table"
echo "────────────────────────────────────────────────"
if [ -f /.dockerenv ]; then
    # Inside Docker: use psql directly
    psql -h postgres -U app -d shop -c "SELECT * FROM orders LIMIT 5;"
else
    # On host: use docker exec
    docker compose exec -T postgres psql -U app -d shop -c "SELECT * FROM orders LIMIT 5;"
fi

if [ $? -eq 0 ]; then
    echo ""
    echo "Orders query OK"
else
    echo "Orders query failed"
    exit 1
fi

echo ""

# Test 3: Postgres Timestamp Check
echo "Test 3: Postgres - Timestamp Function"
echo "────────────────────────────────────────────────"
if [ -f /.dockerenv ]; then
    psql -h postgres -U app -d shop -c "SELECT now();"
else
    docker compose exec -T postgres psql -U app -d shop -c "SELECT now();"
fi

if [ $? -eq 0 ]; then
    echo ""
    echo "now() query OK"
else
    echo "now() query failed"
    exit 1
fi

echo ""

# Test 4: Run ETL
echo "Test 4: ETL Execution"
echo "────────────────────────────────────────────────"
if [ -f /.dockerenv ]; then
    # Inside Docker: run Python directly
    cd /work/app && python etl.py
else
    # On host: use docker exec
    docker compose exec -T app python /work/app/etl.py
fi

if [ $? -eq 0 ]; then
    echo ""
    echo "ETL completed successfully"
else
    echo " ETL execution failed"
    exit 1
fi

echo ""
echo "================================================"
echo "All tests passed!"
echo "================================================"