package com.innowise.telemetry_ingestion_service.repository;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface TelemetryPointBatchRepository {
    Mono<BatchInsertResult> saveAll(Publisher<TelemetryPoint> points);

    default Mono<BatchInsertResult> saveAll(Iterable<TelemetryPoint> points) {
        return saveAll(Flux.fromIterable(points));
    }
}
