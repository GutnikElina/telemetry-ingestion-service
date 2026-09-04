package com.innowise.telemetry_ingestion_service.entity;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

public record TelemetryPoint(
        UUID deviceId,
        Instant timestamp,
        double latitude,
        double longitude,
        float altitude,
        float angle,
        byte satellites,
        float speed,
        Map<Short, Object> sensorsData
        ) {
}
