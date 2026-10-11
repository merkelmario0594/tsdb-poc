DROP TABLE tsdb_poc.metrics_5min;
DROP VIEW tsdb_poc.metrics_5min_mv;
DROP TABLE tsdb_poc.kafka_direct_metrics_5min;
DROP VIEW tsdb_poc.kafka_direct_metrics_5min_mv;

CREATE TABLE tsdb_poc.metrics_5min
(
    interval_start DateTime,
    
    -- Min & Max (SimpleAggregateFunction)
    min_kafka_timestamp_diff SimpleAggregateFunction(min, Int64),
    max_kafka_timestamp_diff SimpleAggregateFunction(max, Int64),
    
    -- Average (AggregateFunction having states)
    avg_kafka_timestamp_diff AggregateFunction(avg, Float64),
    
    min_clickhouse_ingested_diff SimpleAggregateFunction(min, Int64),
    max_clickhouse_ingested_diff SimpleAggregateFunction(max, Int64),
    avg_clickhouse_ingested_diff AggregateFunction(avg, Float64),
    
    -- New timestamps
    created_at SimpleAggregateFunction(min, DateTime),
    updated_at SimpleAggregateFunction(max, DateTime),

    min_mat_view_diff SimpleAggregateFunction(min, Float64),
    max_mat_view_diff SimpleAggregateFunction(max, Float64),
    avg_mat_view_diff AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree()
ORDER BY interval_start;


CREATE MATERIALIZED VIEW tsdb_poc.metrics_5min_mv TO tsdb_poc.metrics_5min AS
SELECT
    toStartOfInterval(timestamp, INTERVAL 5 MINUTE) AS interval_start,
    min(_timestamp - timestamp) AS min_kafka_timestamp_diff,
    max(_timestamp - timestamp) AS max_kafka_timestamp_diff,
    avgState(toFloat64(_timestamp - timestamp)) AS avg_kafka_timestamp_diff,
    
    min(ingested_at - timestamp) AS min_clickhouse_ingested_diff,
    max(ingested_at - timestamp) AS max_clickhouse_ingested_diff,
    avgState(toFloat64(coalesce(ingested_at,timestamp) - timestamp)) AS avg_clickhouse_ingested_diff,
    
    -- Timestamps UTC
    min(now('UTC')) AS created_at,
    max(now('UTC')) AS updated_at,

    min(toFloat64(now('UTC') - timestamp)) AS min_mat_view_diff,
    max(toFloat64(now('UTC') - timestamp)) AS max_mat_view_diff,
    avgState(toFloat64(now('UTC') - timestamp)) AS avg_mat_view_diff
    
FROM tsdb_poc.tsdb_poc
GROUP BY interval_start;



CREATE TABLE tsdb_poc.kafka_direct_metrics_5min
(
    interval_start DateTime,
    
    -- Min & Max (SimpleAggregateFunction)
    min_kafka_timestamp_diff SimpleAggregateFunction(min, Int64),
    max_kafka_timestamp_diff SimpleAggregateFunction(max, Int64),
    
    -- Average (AggregateFunction having states)
    avg_kafka_timestamp_diff AggregateFunction(avg, Float64),
    
    min_clickhouse_ingested_diff SimpleAggregateFunction(min, Int64),
    max_clickhouse_ingested_diff SimpleAggregateFunction(max, Int64),
    avg_clickhouse_ingested_diff AggregateFunction(avg, Float64),
    
    -- New timestamps
    created_at SimpleAggregateFunction(min, DateTime),
    updated_at SimpleAggregateFunction(max, DateTime),

    min_mat_view_diff SimpleAggregateFunction(min, Float64),
    max_mat_view_diff SimpleAggregateFunction(max, Float64),
    avg_mat_view_diff AggregateFunction(avg, Float64)
)
ENGINE = AggregatingMergeTree()
ORDER BY interval_start;



CREATE MATERIALIZED VIEW tsdb_poc.kafka_direct_metrics_5min_mv TO tsdb_poc.kafka_direct_metrics_5min AS
SELECT
    toStartOfInterval(timestamp, INTERVAL 5 MINUTE) AS interval_start,
    min(_timestamp - timestamp) AS min_kafka_timestamp_diff,
    max(_timestamp - timestamp) AS max_kafka_timestamp_diff,
    avgState(toFloat64(_timestamp - timestamp)) AS avg_kafka_timestamp_diff,
    
    min(ingested_at - timestamp) AS min_clickhouse_ingested_diff,
    max(ingested_at - timestamp) AS max_clickhouse_ingested_diff,
    avgState(toFloat64(coalesce(ingested_at,timestamp) - timestamp)) AS avg_clickhouse_ingested_diff,
    
    -- Timestamps UTC
    min(now('UTC')) AS created_at,
    max(now('UTC')) AS updated_at,

    min(toFloat64(now('UTC') - timestamp)) AS min_mat_view_diff,
    max(toFloat64(now('UTC') - timestamp)) AS max_mat_view_diff,
    avgState(toFloat64(now('UTC') - timestamp)) AS avg_mat_view_diff
    
FROM tsdb_poc.tsdb_poc_raw
GROUP BY interval_start;


SELECT 
    interval_start,
    
    -- Kafka Timestamp Differenz
    min(min_kafka_timestamp_diff) AS min_kafka_timestamp_diff,
    max(max_kafka_timestamp_diff) AS max_kafka_timestamp_diff,
    avgMerge(avg_kafka_timestamp_diff) AS avg_kafka_timestamp_diff,
    
    -- ClickHouse Ingested Differenz
    min(min_clickhouse_ingested_diff) AS min_clickhouse_ingested_diff,
    max(max_clickhouse_ingested_diff) AS max_clickhouse_ingested_diff,
    avgMerge(avg_clickhouse_ingested_diff) AS avg_clickhouse_ingested_diff,
    
    -- Mat-View / Now Differenz
    min(min_mat_view_diff) AS min_mat_view_diff,
    max(max_mat_view_diff) AS max_mat_view_diff,
    avgMerge(avg_mat_view_diff) AS avg_mat_view_diff,
    
    -- Timestamps (Erstellungs- und Update-Zeitpunkt)
    min(created_at) AS created_at,
    max(updated_at) AS updated_at

FROM tsdb_poc.kafka_direct_metrics_5min
GROUP BY interval_start
ORDER BY interval_start ASC
;