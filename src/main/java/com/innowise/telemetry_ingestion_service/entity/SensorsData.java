package com.innowise.telemetry_ingestion_service.entity;

public record SensorsData(
    Boolean doorOpen,
    Byte temperature,
    Byte humidity,
    Byte batteryLevel
) {
}
