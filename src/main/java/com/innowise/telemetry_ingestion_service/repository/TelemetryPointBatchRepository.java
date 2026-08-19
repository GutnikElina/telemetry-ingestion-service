package com.innowise.telemetry_ingestion_service.repository;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

public interface TelemetryPointBatchRepository {
    Mono<Void> saveAll(Publisher<TelemetryPoint> points);
}
