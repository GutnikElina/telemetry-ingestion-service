package com.innowise.telemetry_ingestion_service.repository;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

@Repository
@AllArgsConstructor
public class TelemetryPointRepository {

    private final TelemetryPointRowRepository rowRepository;
    private final JsonMapper jsonMapper;


    public Flux<TelemetryPoint> findByDeviceIdAndTimeBetweenOrderByTimeAsc(UUID deviceId, Instant from, Instant to) {
        return rowRepository.findByDeviceIdAndTimeBetweenOrderByTimeAsc(deviceId, from, to)
                .map(this::toDomain);
    }

    public Mono<TelemetryPoint> findLatestPoint(UUID deviceId) {
        return rowRepository.findLatestPoint(deviceId).map(this::toDomain);
    }

    private TelemetryPoint toDomain(TelemetryPointRow row) {
        return new TelemetryPoint(
                row.deviceId(),
                row.time(),
                row.latitude(),
                row.longitude(),
                row.speed(),
                row.altitude(),
                row.heading(),
                row.gpsAccuracy(),
                parseSensors(row.sensors())
        );
    }

    private Map<String, Object> parseSensors(String sensorsJson) {
        if (sensorsJson == null || sensorsJson.isBlank()) {
            return null;
        }
        try {
            return jsonMapper.readValue(sensorsJson, new TypeReference<Map<String, Object>>() {
            });
        } catch (JacksonException e) {
            throw new IllegalArgumentException("Could not parse sensors JSON: " + sensorsJson, e);
        }
    }
}