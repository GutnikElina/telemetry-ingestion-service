package com.innowise.telemetry_ingestion_service.mapper;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import com.innowise.telemetry_ingestion_service.repository.TelemetryPointRow;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.json.JsonMapper;

import java.util.Collections;
import java.util.Map;

@Component
@Slf4j
public class TelemetryPointMapper {

    private static final TypeReference<Map<Short, Object>> SENSORS_TYPE_REF =
            new TypeReference<>() {};

    private final JsonMapper jsonMapper;
    private final Counter sensorsDeserializationErrorCounter;

    public TelemetryPointMapper(JsonMapper jsonMapper, MeterRegistry meterRegistry) {
        this.jsonMapper = jsonMapper;
        this.sensorsDeserializationErrorCounter = Counter.builder("telemetry_sensors_deserialization_errors_total")
                .description("Total number of errors during sensors JSON deserialization from database")
                .register(meterRegistry);
    }

    public TelemetryPoint toDomain(TelemetryPointRow row) {
        if (row == null) {
            return null;
        }
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
        if (!StringUtils.hasText(sensorsJson)) {
            return Collections.emptyMap();
        }
        try {
            return jsonMapper.readValue(sensorsJson, SENSORS_TYPE_REF);
        } catch (Exception e) {
            log.error("Could not parse sensors JSON from database, falling back to empty map. JSON: {}", sensorsJson, e);
            sensorsDeserializationErrorCounter.increment();
            return Collections.emptyMap();
        }
    }
}