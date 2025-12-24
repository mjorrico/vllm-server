#!/bin/bash
set -e

POSTGRES_DB="${POSTGRES_DB:-postgres}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"

# --- Part 1: Database Verification ---
echo "Verifying database: $POSTGRES_DB"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<EOSQL
    SELECT 'CREATE DATABASE ${POSTGRES_DB}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${POSTGRES_DB}')\gexec
EOSQL

# --- Part 2: Table Initialization ---
echo "Initializing tables in: $POSTGRES_DB"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<EOSQL

    -- 1. Article Table (UUIDv7)
    CREATE TABLE IF NOT EXISTS articles (
        id UUID PRIMARY KEY DEFAULT uuidv7(),
        content TEXT,
        introtext TEXT,
        category VARCHAR(100),
        duration INTEGER DEFAULT 5,
        updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
    );

    -- 2. Project Table (UUIDv7)
    CREATE TABLE IF NOT EXISTS projects (
        id UUID PRIMARY KEY DEFAULT uuidv7(),
        project_url TEXT,
        introtext TEXT,
        thumbnail_url TEXT
    );

    -- 3. User Table (UUIDv7)
    CREATE TABLE IF NOT EXISTS users (
        id UUID PRIMARY KEY DEFAULT uuidv7(),
        email VARCHAR(255) UNIQUE NOT NULL,
        name VARCHAR(100),
        is_active BOOLEAN DEFAULT TRUE
    );

EOSQL

echo "Database initialization completed successfully!"