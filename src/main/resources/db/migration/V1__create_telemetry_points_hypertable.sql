CREATE EXTENSION IF NOT EXISTS timescaledb;
CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE telemetry_points (
    id UUID NOT NULL DEFAULT gen_random_uuid(),
    time TIMESTAMPTZ NOT NULL,
    device_id UUID NOT NULL,
    location GEOMETRY(Point, 4326) NOT NULL,
    speed REAL,
    altitude REAL,
    sensors JSONB,
    PRIMARY KEY (device_id,time)
);

SELECT create_hypertable('telemetry_points', 'time', chunk_time_interval => INTERVAL '1 day');

ALTER TABLE telemetry_points SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'device_id'
    );
SELECT add_compression_policy('telemetry_points', INTERVAL '7 days');

