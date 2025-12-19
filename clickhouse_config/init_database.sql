CREATE DATABASE IF NOT EXISTS vllm_logger;

CREATE TABLE IF NOT EXISTS vllm_logger.vllm_log (
    created_at DateTime64(3),
    gpu_index Int32,
    gpu_utilization Float64,
    memory_used Int64,
    memory_total Int64,
    temperature Nullable(Float64)
) 
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(created_at)
-- PARTITION BY toStartOfInterval(created_at, INTERVAL 30 SECOND)
ORDER BY (created_at, gpu_index)
TTL created_at + INTERVAL 31 DAY;