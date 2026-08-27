package com.innowise.telemetry_ingestion_service.kafka;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;

import java.util.List;

public interface TelemetryPointKafkaProducer {

    void sendPointsList(List<TelemetryPoint> points);
}
