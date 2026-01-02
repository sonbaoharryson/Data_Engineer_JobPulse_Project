#!/bin/bash
set -e

# Set default values for PostgreSQL
export DB_USER="${DB_USER:-airflow}"
export DB_PASSWORD="${DB_PASSWORD:-airflow}"
export DB_AIRFLOW="${DB_AIRFLOW:-airflow}"
export DB_HOST="${DB_HOST:-localhost}"
export DB_PORT="${DB_PORT:-5432}"

# Construct SQL Alchemy connection string for local PostgreSQL
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${DB_USER}:${DB_PASSWORD}@${DB_HOST}:${DB_PORT}/${DB_AIRFLOW}"
echo "Using database connection: postgresql+psycopg2://${DB_USER}:***@${DB_HOST}:${DB_PORT}/${DB_AIRFLOW}"

# Initialize PostgreSQL if needed
echo "Initializing PostgreSQL..."
/usr/local/bin/init-postgres.sh

# Create supervisor log directory
mkdir -p /var/log/supervisor

# Note: PostgreSQL will be started by supervisord
# We'll wait for it after supervisord starts

# Start all services with supervisord
echo "Starting all services with supervisord..."
/usr/bin/supervisord -c /etc/supervisor/conf.d/supervisord.conf

# Wait for PostgreSQL to be ready
echo "Waiting for PostgreSQL to be ready..."
for i in {1..30}; do
  if pg_isready -h localhost -p 5432 -U "$DB_USER" 2>/dev/null; then
    echo "PostgreSQL is ready!"
    break
  fi
  if [ $i -eq 30 ]; then
    echo "PostgreSQL failed to start after 30 attempts"
    exit 1
  fi
  echo "Waiting for PostgreSQL... ($i/30)"
  sleep 2
done

# Initialize Airflow database
echo "Initializing Airflow database..."
airflow db migrate

# Check if admin user exists, create if not
echo "Checking if admin user exists..."
ADMIN_USERNAME="${ADMIN_USERNAME:-$DB_USER}"
ADMIN_PASSWORD="${ADMIN_PASSWORD:-$DB_PASSWORD}"

if ! airflow users list 2>/dev/null | grep -q "$ADMIN_USERNAME"; then
  echo "Creating admin user..."
  airflow users create \
    --username "$ADMIN_USERNAME" \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email "${ADMIN_EMAIL:-admin@example.com}" \
    --password "$ADMIN_PASSWORD" || echo "User may already exist or creation failed"
else
  echo "Admin user already exists"
fi

# Keep the script running (supervisord is running in background)
echo "All services started. Monitoring..."
tail -f /var/log/supervisor/*.log

