CREATE TABLE video_measure (
  -- IDENTIFIERS & USER INFO
  sessid                        String,
  duuid                         String,
  userid                        String,
  uadev                         String,
  
  -- CONTENT METADATA
  video_id                      String,
  video_name                    String,
  tags                          String,
  duration                      String,
  
  -- THE REPEATING/DUPLICATE EVENTS (Stored as lists to capture everything)
  bitrate                  Array(String),   -- Stores: ['6000,128', '2400,128']
  buffer                   Array(Int64),    -- Stores: [1783682317985, 1783682318286]
  playing                  Array(String),   -- Stores: ['1783682318587,5.00', '1783682319389,10.00']
  seek                     Array(String),   -- Stores: ['1783682318000,25.00']
  pause                    Array(Int64),    -- Stores: ['1750482986082']

  -- SINGLE-OCCURRENCE LIFECYCLE FIELDS
  load                       Nullable(Int64),
  play                       Nullable(Int64),
  complete                   Nullable(Int64),
  unload                     Nullable(Int64),
  error                      Nullable(String),


  tsclient                      Int64,
  tsserver                      Int64
) 
ENGINE = MergeTree() 
PARTITION BY toYYYYMM(toDateTime(tsserver / 1000))
ORDER BY (video_id, tsclient, sessid)

-- Automatic deletion rule (90 Days)
TTL toDateTime(tsserver / 1000) + INTERVAL 90 DAY;