package com.innowise.telemetry_ingestion_service.repository;

import com.innowise.telemetry_ingestion_service.mapper.TelemetryPointMapper;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import tools.jackson.databind.json.JsonMapper;

import java.time.Instant;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class TelemetryPointRepositoryTest {

    @Mock
    private TelemetryPointRowRepository rowRepository;

    private MeterRegistry meterRegistry;
    private TelemetryPointRepository repository;

    @BeforeEach
    void setUp() {
        meterRegistry = new SimpleMeterRegistry();
        var jsonMapper = new JsonMapper();
        var mapper=new TelemetryPointMapper(jsonMapper,meterRegistry);
        repository = new TelemetryPointRepository(rowRepository,mapper );
    }

    @Test
    void shouldFallbackToEmptyMapAndIncrementCounterWhenJsonIsCorrupted() {
        var deviceId = UUID.randomUUID();
        var time = Instant.now();

        TelemetryPointRow corruptedRow = new TelemetryPointRow(
                deviceId, time, 55.0, 37.0, 50.0f, 100.0f, 0.0f, (short) 5,
                "{CORRUPTED_JSON_STRING"
        );

        when(rowRepository.findLatestPoint(deviceId)).thenReturn(Mono.just(corruptedRow));

        StepVerifier.create(repository.findLatestPoint(deviceId))
                .assertNext(point -> {
                    assertThat(point.deviceId()).isEqualTo(deviceId);
                    assertThat(point.sensors()).isEmpty();
                })
                .verifyComplete();

        Counter counter = meterRegistry.find("telemetry_sensors_deserialization_errors_total").counter();
        assertThat(counter).isNotNull();
        assertThat(counter.count()).isEqualTo(1.0);
    }
}
