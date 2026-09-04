package com.innowise.telemetry_ingestion_service.kafka;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import reactor.core.publisher.Mono;

import java.util.List;

public interface TelemetryPointKafkaProducer {

    Mono<Void> sendPointsList(List<TelemetryPoint> points);
}
