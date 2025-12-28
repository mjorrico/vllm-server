#!/bin/bash
set -e

BACKEND_POSTGRES_DB="${BACKEND_POSTGRES_DB:-postgres}"
POSTGRES_USER="${POSTGRES_USER:-postgres}"
POSTGRES_PASSWORD="${POSTGRES_PASSWORD:-postgres}"

MLFLOW_POSTGRES_DB="${MLFLOW_POSTGRES_DB:-postgres}"

# --- Part 1: Database Verification ---
echo "Verifying database: $BACKEND_POSTGRES_DB"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<EOSQL
    SELECT 'CREATE DATABASE ${BACKEND_POSTGRES_DB}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${BACKEND_POSTGRES_DB}')\gexec
EOSQL

# --- Part 1.5: MLflow Database Verification ---
echo "Verifying database: $MLFLOW_POSTGRES_DB"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "postgres" <<EOSQL
    SELECT 'CREATE DATABASE ${MLFLOW_POSTGRES_DB}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${MLFLOW_POSTGRES_DB}')\gexec
EOSQL

# --- Part 2: Table Initialization ---
echo "Initializing tables in: $BACKEND_POSTGRES_DB"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$BACKEND_POSTGRES_DB" <<EOSQL

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

# --- Part 3: Dummy Data ---
echo "Inserting dummy data in: $BACKEND_POSTGRES_DB"
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$BACKEND_POSTGRES_DB" <<'EOSQL'
INSERT INTO public.articles (content, introtext, category, duration)
VALUES 
(
'# The Physics of Quantum Superposition

Quantum mechanics introduces the concept of a qubit, which exists in a linear combination of states. This state is mathematically represented by the state vector:

$$ |\psi\rangle = \alpha |0\rangle + \beta |1\rangle $$

Where $\alpha$ and $\beta$ are complex numbers such that $|\alpha|^2 + |\beta|^2 = 1$.

## Simulation in Python

Using frameworks like Qiskit, we can simulate a Hadamard gate which puts a qubit into superposition:

```python
from qiskit import QuantumCircuit, assemble, Aer
from qiskit.visualization import plot_histogram

# Create a quantum circuit with one qubit
qc = QuantumCircuit(1)
qc.h(0)  # Apply Hadamard gate
qc.measure_all()

print("Circuit initialized and Hadamard gate applied.")
```

## Performance Modeling

The probability of measuring a specific state  is defined as . In a multi-qubit system, the state space grows exponentially:

$ \text{Dimensions} = 2^n $

This exponential scaling is what gives quantum computers their theoretical advantage over classical systems for specific algorithms.',
'An exploration of quantum states, superposition math, and basic circuit simulation.',
'Quantum Computing',
12
),
(
'# Systems Engineering: Throughput and Latency

In distributed systems, Little''s Law is a fundamental law relating the long-term average number of items in a stationary system , the average arrival rate , and the average time an item spends in the system :

$$ L = \lambda W $$

## Practical Implementation in Rust

To track system metrics efficiently, we often use atomic counters to avoid lock contention:

```rust
use std::sync::atomic::{AtomicUsize, Ordering};

struct Metrics {
    total_requests: AtomicUsize,
}

impl Metrics {
    fn increment(&self) {
        self.total_requests.fetch_add(1, Ordering::SeqCst);
    }
}

fn main() {
    let m = Metrics { total_requests: AtomicUsize::new(0) };
    m.increment();
    println!("Request logged.");
}
```

## Database Monitoring

To analyze latency distribution within a PostgreSQL database, you can use the following query:

```sql
SELECT 
    endpoint,
    PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time_ms) AS p95_latency,
    AVG(response_time_ms) AS avg_latency
FROM api_logs
WHERE request_date > NOW() - INTERVAL ''24 hours''
GROUP BY endpoint
ORDER BY p95_latency DESC;
```

Understanding these metrics is vital for maintaining High Availability (HA) clusters.',
'A technical guide to system performance metrics, Little''s Law, and monitoring strategies.',
'Infrastructure',
15
);
EOSQL

echo "Database initialization completed successfully!"