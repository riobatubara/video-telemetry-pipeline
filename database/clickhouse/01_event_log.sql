CREATE TABLE event_log
(
    tsclient    Int64,
    tsserver    Int64,
    sessid      String,
    event       LowCardinality(String),
    value       String
)
ENGINE = MergeTree()
-- Convert millisecond Int64 to pure DateTime for partition and TTL stability
PARTITION BY toYYYYMMDD(toDateTime(tsserver / 1000))
ORDER BY (event, tsserver, sessid)

-- Automatically deletes raw event lines 7 days after the server receives them
TTL toDateTime(tsserver / 1000) + INTERVAL 7 DAY;
