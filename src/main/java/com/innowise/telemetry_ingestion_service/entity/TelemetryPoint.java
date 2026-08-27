package com.innowise.telemetry_ingestion_service.entity;

import java.time.LocalDateTime;
import java.util.Map;

public record TelemetryPoint(
        String deviceId,
        LocalDateTime timestamp,
        int latitude,
        int longitude,
        short altitude,
        short angle,
        byte satellites,
        short speed,
        Map<Byte, Object> sensorsData
        ) {
}
