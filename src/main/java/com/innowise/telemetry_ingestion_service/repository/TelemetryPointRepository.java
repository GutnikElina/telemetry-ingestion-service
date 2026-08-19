package com.innowise.telemetry_ingestion_service.repository;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

public interface TelemetryPointRepository extends ReactiveCrudRepository<TelemetryPoint, UUID> {
    Flux<TelemetryPoint> findByDeviceIdAndTimeBetweenOrderByTimeAsc(UUID deviceId, Instant from,
                                                                    Instant to);

    @Query("SELECT * FROM telemetry_points WHERE device_id=:deviceId ORDER BY time DESC LIMIT 1")
    Mono<TelemetryPoint> findLatestPoint(@Param("deviceId") UUID device_id);
}
