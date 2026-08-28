package com.innowise.telemetry_ingestion_service.repository;

import java.time.Instant;
import java.util.UUID;

record TelemetryPointRow(
        UUID deviceId,
        Instant time,
        double latitude,
        double longitude,
        Float speed,
        Float altitude,
        Float heading,
        Float gpsAccuracy,
        String sensors
) {
}