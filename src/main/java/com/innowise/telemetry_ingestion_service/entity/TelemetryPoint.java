package com.innowise.telemetry_ingestion_service.entity;

import org.springframework.data.relational.core.mapping.Table;


import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Table("telemetry_points")
public record  TelemetryPoint(
        UUID deviceId,
        Instant time,
        double latitude,
        double longitude,
        Float speed,
        Float altitude,
        Float heading,
        Float gpsAccuracy,
        Map<String, Object> sensors
) {
    public TelemetryPoint {
        if (deviceId == null) {
            throw new IllegalArgumentException("deviceId must not be null");
        }
        if (time == null) {
            throw new IllegalArgumentException("time must not be null");
        }
        if (latitude < -90.0 || latitude > 90.0) {
            throw new IllegalArgumentException("latitude out of range [-90, 90]: " + latitude);
        }
        if (longitude < -180.0 || longitude > 180.0) {
            throw new IllegalArgumentException("longitude out of range [-180, 180]: " + longitude);
        }
    }
}