CREATE TABLE event_log
(
    tsclient    Int64,
    tsserver    Int64,
    sessid      String,
    event       LowCardinality(String),
    value       String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(toDateTime64(tsserver / 1000, 3))
ORDER BY (event, tsserver, sessid)

-- Automatically deletes raw event lines 7 days after the server receives them
TTL toDateTime64(tsserver / 1000, 3) + INTERVAL 7 DAY;