package com.innowise.telemetry_ingestion_service.entity;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.time.Instant;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TelemetryPointTest {

    @Test

    void shouldCreateTelemetryPointWhenValid() {
        assertThatCode(() -> new TelemetryPoint(
                UUID.randomUUID(),
                Instant.now(),
                53.9000, 27.5667,
                60.0f, 200.0f, 180.0f, (byte) 8,
                Map.of((short) 1, 100)
        )).doesNotThrowAnyException();
    }

    @Test
    void shouldThrowExceptionWhenDeviceIdIsNull() {
        assertThatThrownBy(() -> new TelemetryPoint(
                null, Instant.now(), 50.0, 30.0, 0f, 0f, 0f, (byte) 0, Map.of()
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("deviceId must not be null");
    }

    @Test
    void shouldThrowExceptionWhenTimeIsNull() {
        assertThatThrownBy(() -> new TelemetryPoint(
                UUID.randomUUID(), null, 50.0, 30.0, 0f, 0f, 0f, (byte) 0, Map.of()
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessage("time must not be null");
    }

    @ParameterizedTest
    @ValueSource(doubles = {-90.1, 90.1, -100.0, 100.0})
    void shouldThrowExceptionWhenLatitudeIsInvalid(double invalidLat) {
        assertThatThrownBy(() -> new TelemetryPoint(
                UUID.randomUUID(), Instant.now(), invalidLat, 30.0, 0f, 0f, 0f, (byte) 0, Map.of()
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("latitude out of range");
    }

    @ParameterizedTest
    @ValueSource(doubles = {-180.1, 180.1, -200.0, 200.0})
    void shouldThrowExceptionWhenLongitudeIsInvalid(double invalidLon) {
        assertThatThrownBy(() -> new TelemetryPoint(
                UUID.randomUUID(), Instant.now(), 50.0, invalidLon, 0f, 0f, 0f, (byte) 0, Map.of()
        )).isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("longitude out of range");
    }
}
