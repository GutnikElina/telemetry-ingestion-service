package com.innowise.telemetry_ingestion_service.repository;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
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

    private static final TypeReference<Map<Short, Object>> SENSORS_TYPE_REF =
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
                row.speed() != null ? row.speed() : 0.0f,
                row.altitude() != null ? row.altitude() : 0.0f,
                row.heading() != null ? row.heading() : 0.0f,
                row.satellites() != null ? row.satellites().byteValue() : (byte) 0,
                parseSensors(row.sensors())
        );
    }

    private Map<Short, Object> parseSensors(String sensorsJson) {
        if (sensorsJson == null || sensorsJson.isBlank()) {
            return Collections.emptyMap();
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