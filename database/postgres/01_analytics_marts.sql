-- Create a performance index look-up type for video formats if needed later
CREATE TABLE hourly_video_metrics (
    -- Composite Primary Key Fields (For unique time + video grouping)
    metric_hour                           TIMESTAMP NOT NULL,
    video_id                              VARCHAR(32) NOT NULL,
    video_name                            VARCHAR(100) NOT NULL,
    
    -- Plays & Attempt Counters
    attempts                              BIGINT DEFAULT 0,
    plays                                 BIGINT DEFAULT 0,
    active_plays                          BIGINT DEFAULT 0, -- Active sessions within this hour window
    ended_plays                           BIGINT DEFAULT 0,
    concurrent_plays                      INT DEFAULT 0,    -- Peak concurrency reached in this hour window
    
    -- Device Metrics
    unique_devices                        BIGINT DEFAULT 0,

    -- Watch Time Metrics
    total_minutes                         DOUBLE PRECISION DEFAULT 0.0,
    minutes_ended_play                    DOUBLE PRECISION DEFAULT 0.0,

    -- Quality of Experience (QoE) Timings (Stored in milliseconds)
    video_startup_time_avg_ms             INT DEFAULT 0,
    video_restart_time_avg_ms             INT DEFAULT 0,

    -- Failure Metrics
    video_start_failures                  BIGINT DEFAULT 0,
    video_playback_failures               BIGINT DEFAULT 0,
    exit_before_video_start               BIGINT DEFAULT 0,

    -- Core Performance Ratios (Stored as floats/percentages)
    average_bitrate_kbps                  DOUBLE PRECISION DEFAULT 0.0,
    rebuffering_ratio                     REAL DEFAULT 0.0,
    connection_induced_rebuffering_ratio  REAL DEFAULT 0.0,
    average_frame_rate                    REAL DEFAULT 0.0,

    -- Constraint constraints to prevent duplicate hourly data inserts
    CONSTRAINT pk_hourly_video_metrics PRIMARY KEY (metric_hour, video_id)
);

-- Indexes for live dashboards
-- Optimizes group-by trends over time (e.g., "Show me La La Land's performance over the last 30 days")
CREATE INDEX idx_metrics_video_time ON hourly_video_metrics (video_id, metric_hour);
CREATE INDEX idx_metrics_time ON hourly_video_metrics (metric_hour DESC);
