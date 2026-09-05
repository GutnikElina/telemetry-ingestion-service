package com.innowise.telemetry_ingestion_service.repository;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import com.innowise.telemetry_ingestion_service.mapper.TelemetryPointMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Repository
@RequiredArgsConstructor
public class TelemetryPointRepository {

    private final TelemetryPointRowRepository rowRepository;
    private final TelemetryPointMapper mapper;

    public Flux<TelemetryPoint> findByDeviceIdAndTimeBetweenOrderByTimeAsc(UUID deviceId, Instant from, Instant to) {
        return rowRepository.findByDeviceIdAndTimeBetweenOrderByTimeAsc(deviceId, from, to)
                .map(mapper::toDomain);
    }

    public Mono<TelemetryPoint> findLatestPoint(UUID deviceId) {
        return rowRepository.findLatestPoint(deviceId)
                .map(mapper::toDomain);
    }
}