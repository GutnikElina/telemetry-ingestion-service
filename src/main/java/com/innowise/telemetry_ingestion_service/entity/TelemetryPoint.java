package com.innowise.telemetry_ingestion_service.entity;

import java.time.LocalDateTime;

public record TelemetryPoint(
        String deviceId,
        LocalDateTime timestamp,
        int latitude,
        int longitude,
        short altitude,
        short angle,
        byte satellites,
        short speed,
        SensorsData sensorsData
        ) {
}
