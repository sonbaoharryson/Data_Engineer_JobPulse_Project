#!/bin/bash
set -e

# Set default values if not provided
POSTGRES_USER="${DB_USER:-airflow}"
POSTGRES_PASSWORD="${DB_PASSWORD:-airflow}"
POSTGRES_DB="${DB_AIRFLOW:-airflow}"
POSTGRES_DATA_DIR="/var/lib/postgresql/data"

echo "Initializing PostgreSQL..."

# Find PostgreSQL version
PG_VERSION=$(pg_config --version | grep -oP '\d+' | head -1)
PG_BIN="/usr/lib/postgresql/${PG_VERSION}/bin"
PG_ETC="/etc/postgresql/${PG_VERSION}/main"

# Check if data directory is empty (first run)
if [ -z "$(ls -A $POSTGRES_DATA_DIR 2>/dev/null)" ]; then
    echo "Initializing PostgreSQL data directory..."
    
    # Initialize PostgreSQL data directory
    su - postgres -c "$PG_BIN/initdb -D $POSTGRES_DATA_DIR"
    
    # Create postgresql.conf if it doesn't exist
    if [ ! -f "$POSTGRES_DATA_DIR/postgresql.conf" ]; then
        echo "listen_addresses = '*'" >> "$POSTGRES_DATA_DIR/postgresql.conf"
        echo "port = 5432" >> "$POSTGRES_DATA_DIR/postgresql.conf"
    else
        sed -i "s/#listen_addresses = 'localhost'/listen_addresses = '*'/" "$POSTGRES_DATA_DIR/postgresql.conf" || \
        echo "listen_addresses = '*'" >> "$POSTGRES_DATA_DIR/postgresql.conf"
    fi
    
    # Update pg_hba.conf to allow connections
    echo "host    all             all             0.0.0.0/0               md5" >> "$POSTGRES_DATA_DIR/pg_hba.conf"
    echo "local   all             all                                     trust" >> "$POSTGRES_DATA_DIR/pg_hba.conf"
    
    # Start PostgreSQL temporarily to create database and user
    su - postgres -c "$PG_BIN/pg_ctl -D $POSTGRES_DATA_DIR -l /tmp/postgres.log start" || true
    
    # Wait for PostgreSQL to be ready
    for i in {1..10}; do
        if su - postgres -c "$PG_BIN/pg_isready" 2>/dev/null; then
            break
        fi
        sleep 1
    done
    
    # Create user and database
    su - postgres -c "psql -c \"CREATE USER $POSTGRES_USER WITH PASSWORD '$POSTGRES_PASSWORD';\" || true"
    su - postgres -c "psql -c \"ALTER USER $POSTGRES_USER CREATEDB;\" || true"
    su - postgres -c "psql -c \"CREATE DATABASE $POSTGRES_DB OWNER $POSTGRES_USER;\" || true"
    
    # Stop PostgreSQL (supervisord will start it)
    su - postgres -c "$PG_BIN/pg_ctl -D $POSTGRES_DATA_DIR stop" || true
    
    echo "PostgreSQL initialized successfully"
else
    echo "PostgreSQL data directory already exists, skipping initialization"
fi

echo "PostgreSQL initialization complete"

