package com.innowise.telemetry_ingestion_service.repository;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcNonTransientResourceException;
import io.r2dbc.spi.R2dbcTransientException;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.Map;

@Repository
@Slf4j
public class TelemetryPointBatchRepositoryImpl implements TelemetryPointBatchRepository {

    private static final String INSERT_SQL = """
            INSERT INTO telemetry_points
                (device_id, time, location, speed, altitude, heading, satellites, sensors)
            VALUES
                ($1, $2, ST_SetSRID(ST_MakePoint($3, $4), 4326), $5, $6, $7, $8, $9::jsonb)
            ON CONFLICT (device_id, time) DO NOTHING
            """;

    private final DatabaseClient databaseClient;
    private final JsonMapper jsonMapper;
    private final int batchSize;
    private final Duration batchTimeout;
    private final Counter sensorsSerializationErrorCounter;

    public TelemetryPointBatchRepositoryImpl(
            DatabaseClient databaseClient,
            JsonMapper jsonMapper,
            MeterRegistry meterRegistry,
            @Value("${telemetry.ingestion.batch-size:500}") int batchSize,
            @Value("${telemetry.ingestion.batch-timeout-ms:100}") long batchTimeoutMs
    ) {
        this.databaseClient = databaseClient;
        this.jsonMapper = jsonMapper;
        this.batchSize = batchSize;
        this.batchTimeout = Duration.ofMillis(batchTimeoutMs);
        this.sensorsSerializationErrorCounter = Counter.builder("telemetry_sensors_serialization_errors_total")
                .description("Total number of errors during sensors JSON serialization before batch insert")
                .register(meterRegistry);
    }

    @Override
    public Mono<BatchInsertResult> saveAll(Publisher<TelemetryPoint> points) {
        return Flux.from(points)
                .bufferTimeout(batchSize, batchTimeout)
                .concatMap(this::insertChunk)
                .reduce(BatchInsertResult.empty(), BatchInsertResult::merge);
    }

    private Mono<BatchInsertResult> insertChunk(List<TelemetryPoint> chunk) {
        if (chunk.isEmpty()) {
            return Mono.just(BatchInsertResult.empty());
        }

        return databaseClient.inConnection(connection -> executeBatch(connection, chunk))
                .retryWhen(Retry.backoff(3, Duration.ofMillis(200))
                        .filter(this::isTransientError)
                        .doBeforeRetry(signal -> log.warn(
                                "Retrying batch insert of {} points (attempt {}/3) due to transient error: {}",
                                chunk.size(),
                                signal.totalRetries() + 1,
                                signal.failure().getMessage()
                        ))
                )
                .map(insertedCount -> new BatchInsertResult(
                        insertedCount,
                        chunk.size() - insertedCount,
                        0
                ));
    }

    private boolean isTransientError(Throwable throwable) {
        return throwable instanceof R2dbcTransientException;
    }

    private Mono<Long> executeBatch(Connection connection, List<TelemetryPoint> chunk) {
        Statement statement = connection.createStatement(INSERT_SQL);
        for (int i = 0; i < chunk.size(); i++) {
            bindPoint(statement, chunk.get(i));
            if (i < chunk.size() - 1) {
                statement.add();
            }
        }
        return Flux.from(statement.execute())
                .flatMap(Result::getRowsUpdated)
                .reduce(0L, Long::sum);
    }

    private void bindPoint(Statement statement, TelemetryPoint point) {
        statement.bind(0, point.deviceId());
        statement.bind(1, point.time());
        statement.bind(2, point.longitude());
        statement.bind(3, point.latitude());
        statement.bind(4, point.speed());
        statement.bind(5, point.altitude());
        statement.bind(6, point.heading());
        statement.bind(7, (short) point.satellites());

        if (point.sensors() != null && !point.sensors().isEmpty()) {
            statement.bind(8, toJson(point.sensors()));
        } else {
            statement.bind(8, "{}");
        }
    }

    private String toJson(Map<Short, Object> sensors) {
        try {
            return jsonMapper.writeValueAsString(sensors);
        } catch (JacksonException e) {
            log.error("Could not serialize sensors map to JSON, falling back to empty JSON. Sensors: {}", sensors, e);
            sensorsSerializationErrorCounter.increment();
            return "{}";
        }
    }
}