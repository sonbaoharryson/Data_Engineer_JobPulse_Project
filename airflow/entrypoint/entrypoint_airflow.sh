#!/bin/bash
set -e

echo "Waiting for database..."
until pg_isready -h "$DB_HOST" -p "$DB_PORT" -U "$DB_USER"; do
  sleep 5
done
echo "Database is ready."

if [ -e "/opt/airflow/requirements.txt" ]; then
  $(command -v python) -m pip install --upgrade pip
  $(command -v pip) install -r requirements.txt
fi

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

# Execute the command passed as arguments (command webserver will be parsed by the command in docker-compose)
exec airflow "$@"