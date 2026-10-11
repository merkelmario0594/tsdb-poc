CREATE TABLE tsdb_poc.forecast_minute_latest_state
(
    id String,
    forecastDt DateTime64(9),

    state AggregateFunction
    (
        argMax,
        Tuple(
            Float64,       -- forecast value
            DateTime64(9), -- as_of
            String         -- tags
        ),
        Tuple(
            DateTime64(9), -- as_of
            DateTime64(3), -- Kafka _timestamp
            Int32,         -- Kafka _partition
            Int64          -- Kafka _offset
        )
    )
)
ENGINE = AggregatingMergeTree
ORDER BY (id, forecastDt);


CREATE MATERIALIZED VIEW tsdb_poc.mv_forecast_minute_latest_state
TO forecast_minute_latest_state
AS
SELECT
    id,
    forecastDt,
    argMaxState(
        tuple(forecast_value, as_of, tags),
        tuple(as_of, _timestamp, _partition, _offset)
    ) AS state
FROM
(
    SELECT
        id,
        as_of,
        tags,
        _timestamp,
        _partition,
        _offset,
        timestamp
            + toIntervalMinute(index - 1) AS forecastDt,
        forecast_value
    FROM tsdb_poc.tsdb_poc_forecast
    ARRAY JOIN
        value AS forecast_value,
        arrayEnumerate(value) AS index
)
GROUP BY
    id,
    forecastDt;


SELECT
    id,
    forecastDt,
    tupleElement(result, 1) AS value,
    tupleElement(result, 2) AS as_of,
    tupleElement(result, 3) AS tags
FROM
(
    SELECT
        id,
        forecastDt,
        argMaxMerge(state) AS result
    FROM forecast_minute_latest_state
    WHERE id = 'sensor-1'
    GROUP BY
        id,
        forecastDt
)
ORDER BY
    id,
    forecastDt;
