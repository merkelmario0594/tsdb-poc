CREATE TABLE tsdb_poc.forecast_by_room
(
    characteristic_id String,

    timeband_values AggregateFunction(groupUniqArray, String),

    min_forecast_date AggregateFunction(min, DateTime64(9)),
    max_forecast_date AggregateFunction(max, DateTime64(9)),

    avg_forecast AggregateFunction(avg, Float64),
    min_forecast AggregateFunction(min, Float64),
    max_forecast AggregateFunction(max, Float64),

    sensor_count_state AggregateFunction(uniq, String),

    first_as_of_state AggregateFunction(min, DateTime64(9)),
    latest_as_of_state AggregateFunction(max, DateTime64(9))
)
ENGINE = AggregatingMergeTree
ORDER BY characteristic_id;






CREATE MATERIALIZED VIEW tsdb_poc.mv_forecast_by_room
TO tsdb_poc.forecast_by_room
AS
SELECT
    c.characteristic_id AS characteristic_id,

    groupUniqArrayState(toString(c.value)) AS timeband_values,

    minState(f.forecastDt) AS min_forecast_date,
    maxState(f.forecastDt) AS max_forecast_date,

    avgState(tupleElement(f.result, 1)) AS avg_forecast,
    minState(tupleElement(f.result, 1)) AS min_forecast,
    maxState(tupleElement(f.result, 1)) AS max_forecast,

    uniqState(toString(f.id)) AS sensor_count_state,

    minState(tupleElement(f.result, 2)) AS first_as_of_state,
    maxState(tupleElement(f.result, 2)) AS latest_as_of_state
FROM
(
    SELECT
        id,
        forecastDt,
        argMaxMerge(state) AS result
    FROM tsdb_poc.forecast_minute_latest_state
    GROUP BY id, forecastDt
) AS f
INNER JOIN tsdb_poc.tsdb_poc_characteristics AS c
    ON JSONExtractString(tupleElement(f.result, 3), 'location')
       = c.characteristic_id
   AND f.forecastDt >= c.valid_from
   AND f.forecastDt < c.valid_until
GROUP BY
    c.characteristic_id;



SELECT
    characteristic_id,

    arrayStringConcat(
        arraySort(groupUniqArrayMerge(timeband_values)),
        ', '
    ) AS timeband_values,

    minMerge(min_forecast_date) AS min_forecast_date,
    maxMerge(max_forecast_date) AS max_forecast_date,

    avgMerge(avg_forecast) AS avg_forecast,
    minMerge(min_forecast) AS min_forecast,
    maxMerge(max_forecast) AS max_forecast,

    uniqMerge(sensor_count_state) AS sensor_count,

    minMerge(first_as_of_state) AS first_as_of,
    maxMerge(latest_as_of_state) AS latest_as_of

FROM tsdb_poc.forecast_by_room

GROUP BY characteristic_id;

