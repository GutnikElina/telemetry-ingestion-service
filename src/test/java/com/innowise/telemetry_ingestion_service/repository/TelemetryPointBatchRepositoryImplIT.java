package com.innowise.telemetry_ingestion_service.repository;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import com.innowise.telemetry_ingestion_service.support.AbstractIntegrationTest;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
class TelemetryPointBatchRepositoryImplIT extends AbstractIntegrationTest {

    @Autowired
    private TelemetryPointBatchRepository batchRepository;

    @Autowired
    private TelemetryPointRepository repository;

    @Test
    void shouldInsertBatchAndReadBack() {
        UUID deviceA = UUID.randomUUID();
        UUID deviceB = UUID.randomUUID();
        Instant now = Instant.now();

        TelemetryPoint point1 = new TelemetryPoint(
                deviceA, now,
                55.7558, 37.6173,
                60.5f, 150.0f, 12.0f, 3.5f,
                Map.of("fuel", 80)
        );
        TelemetryPoint point2 = new TelemetryPoint(
                deviceB, now.plusSeconds(5),
                59.9386, 30.3141,
                45.0f, 20.0f, null, null,
                null
        );

        StepVerifier.create(batchRepository.saveAll(Flux.fromIterable(List.of(point1, point2))))
                .assertNext(result -> {
                    assertThat(result.inserted()).isEqualTo(2);
                    assertThat(result.duplicates()).isZero();
                    assertThat(result.rejected()).isZero();
                })
                .verifyComplete();

        StepVerifier.create(
                        repository.findByDeviceIdAndTimeBetweenOrderByTimeAsc(
                                deviceA, now.minusSeconds(1), now.plusSeconds(1)
                        )
                )
                .assertNext(found -> {
                    assertThat(found.deviceId()).isEqualTo(deviceA);
                    assertThat(found.latitude()).isCloseTo(55.7558, org.assertj.core.data.Offset.offset(1e-6));
                    assertThat(found.longitude()).isCloseTo(37.6173, org.assertj.core.data.Offset.offset(1e-6));
                    assertThat(found.speed()).isEqualTo(60.5f);
                    assertThat(found.sensors()).containsEntry("fuel", 80);
                })
                .verifyComplete();

        StepVerifier.create(repository.findLatestPoint(deviceB))
                .assertNext(found -> {
                    assertThat(found.sensors()).isNull();
                    assertThat(found.altitude()).isEqualTo(20.0f);
                    assertThat(found.heading()).isNull();
                })
                .verifyComplete();
    }

    @Test
    void shouldSkipDuplicateOnConflictWithoutFailingWholeBatch() {
        UUID deviceId = UUID.randomUUID();
        Instant time = Instant.now();

        TelemetryPoint original = new TelemetryPoint(
                deviceId, time, 10.0, 20.0, 50.0f, 100.0f, null, null, null
        );
        TelemetryPoint redelivered = new TelemetryPoint(
                deviceId, time, 10.0, 20.0, 999.0f, 999.0f, null, null, null
        );
        TelemetryPoint otherValidPoint = new TelemetryPoint(
                deviceId, time.plusSeconds(1), 11.0, 21.0, 55.0f, 110.0f, null, null, null
        );

        StepVerifier.create(batchRepository.saveAll(
                        Flux.fromIterable(List.of(original, redelivered, otherValidPoint))
                ))
                .assertNext(result -> {
                    assertThat(result.inserted()).isEqualTo(2);
                    assertThat(result.duplicates()).isEqualTo(1);
                })
                .verifyComplete();
    }
}