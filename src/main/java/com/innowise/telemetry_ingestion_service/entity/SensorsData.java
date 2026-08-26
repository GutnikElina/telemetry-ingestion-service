package com.innowise.telemetry_ingestion_service.entity;

public record SensorsData(
    boolean doorOpen,
    byte temperature,
    byte humidity,
    byte batteryLevel
) {
}
