package com.innowise.telemetry_ingestion_service.repository;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class TelemetryPointBatchRepositoryImplIT {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
            DockerImageName.parse("timescale/timescaledb-ha:pg16")
                    .asCompatibleSubstituteFor("postgres")
    )
            .withDatabaseName("telemetry_db")
            .withUsername("telemetry_user")
            .withPassword("telemetry_password");

    @DynamicPropertySource
    static void registerDynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.r2dbc.url", () ->
                "r2dbc:postgresql://%s:%d/%s".formatted(
                        postgres.getHost(),
                        postgres.getMappedPort(5432),
                        postgres.getDatabaseName()
                )
        );
        registry.add("spring.r2dbc.username", postgres::getUsername);
        registry.add("spring.r2dbc.password", postgres::getPassword);

        registry.add("spring.flyway.url", () ->
                "jdbc:postgresql://%s:%d/%s".formatted(
                        postgres.getHost(),
                        postgres.getMappedPort(5432),
                        postgres.getDatabaseName()
                )
        );
        registry.add("spring.flyway.user", postgres::getUsername);
        registry.add("spring.flyway.password", postgres::getPassword);
    }

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
                null,
                deviceA,
                now,
                "POINT(37.6173 55.7558)",
                60.5f,
                150.0f,
                io.r2dbc.postgresql.codec.Json.of("{\"fuel\": 80}")
        );

        TelemetryPoint point2 = new TelemetryPoint(
                null,
                deviceB,
                now.plusSeconds(5),
                "POINT(30.3141 59.9386)",
                45.0f,
                20.0f,
                null
        );

        List<TelemetryPoint> batch = List.of(point1, point2);

        StepVerifier.create(batchRepository.saveAll(Flux.fromIterable(batch)))
                .verifyComplete();

        StepVerifier.create(
                        repository.findByDeviceIdAndTimeBetweenOrderByTimeAsc(
                                deviceA, now.minusSeconds(1), now.plusSeconds(1)
                        )
                )
                .assertNext(found -> {
                    assertThat(found.deviceId()).isEqualTo(deviceA);
                    assertThat(found.speed()).isEqualTo(60.5f);
                    assertThat(found.id()).isNotNull();
                })
                .verifyComplete();

        StepVerifier.create(repository.findLatestPoint(deviceB))
                .assertNext(found -> {
                    assertThat(found.sensors()).isNull();
                    assertThat(found.altitude()).isEqualTo(20.0f);
                })
                .verifyComplete();
    }
}