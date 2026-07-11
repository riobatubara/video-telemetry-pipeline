CREATE TABLE invalid_event_log
(
    tsserver       Int64,                  -- Captured by API Gateway
    gateway_ip     String,                 -- Captured by API Gateway (X-Forwarded-For)
    uadev          String,                 -- Captured by API Gateway (User-Agent header)
    error_reason   LowCardinality(String), -- e.g., 'MISSING_SESSID', 'MALFORMED_JSON'
    raw_payload    String                  -- The untouched broken text from the body)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMMDD(toDateTime(tsserver / 1000))
ORDER BY (error_reason, tsserver)

-- Keep for only 3 to 7 days
TTL toDateTime(tsserver / 1000) + INTERVAL 3 DAY;