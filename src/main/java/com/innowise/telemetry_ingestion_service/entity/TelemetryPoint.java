package com.innowise.telemetry_ingestion_service.entity;

import io.r2dbc.postgresql.codec.Json;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;


import java.time.Instant;
import java.util.UUID;

@Table("telemetry_points")
public record  TelemetryPoint(
        @Id
        UUID id,
        UUID deviceId,
        Instant time,
        String location,
        Float speed,
        Float altitude,
        Json sensors
) {}