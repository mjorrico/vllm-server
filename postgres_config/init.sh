#!/bin/bash
set -e

# This script creates databases for vLLM Server components
# It runs automatically when the PostgreSQL container initializes

# Get database names from environment variables with defaults
VLLM_LOGGER_DB="${VLLM_LOGGER_POSTGRES_DB:-vllm_logger}"
LITELLM_DB="${LITELLM_POSTGRES_DB:-litellm}"
MLFLOW_DB="${MLFLOW_POSTGRES_DB:-mlflow}"

# Create vLLM Logger database if it doesn't exist
echo "Creating database: $VLLM_LOGGER_DB"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<EOSQL
    SELECT 'CREATE DATABASE ${VLLM_LOGGER_DB}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${VLLM_LOGGER_DB}')\gexec
EOSQL

# # Create LiteLLM database if it doesn't exist
# echo "Creating database: $LITELLM_DB"
# psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<EOSQL
#     SELECT 'CREATE DATABASE ${LITELLM_DB}'
#     WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${LITELLM_DB}')\gexec
# EOSQL

# # Create MLflow database if it doesn't exist
# echo "Creating database: $MLFLOW_DB"
# psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<EOSQL
#     SELECT 'CREATE DATABASE ${MLFLOW_DB}'
#     WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${MLFLOW_DB}')\gexec
# EOSQL

echo "Database initialization completed successfully!"
