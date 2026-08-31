package com.innowise.telemetry_ingestion_service.repository;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

@Repository

@Slf4j
public class TelemetryPointRepository {

    private static final TypeReference<Map<String, Object>> SENSORS_TYPE_REF =
            new TypeReference<>() {};

    private final TelemetryPointRowRepository rowRepository;
    private final JsonMapper jsonMapper;
    private final Counter sensorsSerializationErrorCounter;

    public TelemetryPointRepository(
            TelemetryPointRowRepository rowRepository,
            JsonMapper jsonMapper,
            MeterRegistry meterRegistry) {
        this.rowRepository = rowRepository;
        this.jsonMapper = jsonMapper;
        this.sensorsSerializationErrorCounter = Counter.builder("telemetry_sensors_deserialization_errors_total")
                .description("Total number of errors during sensors JSON deserialization from database")
                .register(meterRegistry);
    }

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
            return jsonMapper.readValue(sensorsJson, SENSORS_TYPE_REF);
        } catch (Exception e) {
            log.error("Could not parse sensors JSON from database, falling back to empty map. JSON: {}", sensorsJson, e);
            sensorsSerializationErrorCounter.increment();
            return Collections.emptyMap();
        }
    }
}