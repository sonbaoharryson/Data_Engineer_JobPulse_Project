#!/bin/bash
set -e

echo "Waiting for database..."

export PGPASSWORD="$DB_PASSWORD"

until pg_isready \
  -h "$DB_HOST" \
  -p "$DB_PORT" \
  -U "$DB_USER" \
  -d "$DB_NAME"; do
  echo "Postgres not ready yet..."
  sleep 5
done

echo "Postgres is ready."

# Initialize the database
airflow db migrate

# Check if admin user exists
echo "Checking if admin user exists..."
ADMIN_EXISTS=$(airflow users list | grep -c "$DB_USER" || true)
if [ "$ADMIN_EXISTS" -eq "0" ]; then
  echo "Creating admin user..."
  airflow users create \
    --username "$DB_USER" \
    --firstname admin \
    --lastname admin \
    --role Admin \
    --email admin@example.com \
    --password "$DB_PASSWORD"
fi

echo "Airflow init completed."