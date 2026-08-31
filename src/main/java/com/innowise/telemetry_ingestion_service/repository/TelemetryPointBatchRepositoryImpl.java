package com.innowise.telemetry_ingestion_service.repository;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import lombok.extern.slf4j.Slf4j;
import org.reactivestreams.Publisher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import tools.jackson.databind.json.JsonMapper;

import java.time.Duration;
import java.util.List;
import java.util.Map;


@Repository
@Slf4j
public class TelemetryPointBatchRepositoryImpl implements TelemetryPointBatchRepository {

    private static final String INSERT_SQL = """
            INSERT INTO telemetry_points
                (device_id, time, location, speed, altitude, heading, gps_accuracy, sensors)
            VALUES
                ($1, $2, ST_SetSRID(ST_MakePoint($3, $4), 4326), $5, $6, $7, $8, $9::jsonb)
            ON CONFLICT (device_id, time) DO NOTHING
            """;

    private final JsonMapper jsonMapper;
    private final DatabaseClient databaseClient;
    private final int batchSize;
    private final Duration batchTimeout;

    public TelemetryPointBatchRepositoryImpl(
            JsonMapper jsonMapper,
            DatabaseClient databaseClient,
            @Value("${telemetry.ingestion.batch-size:500}") int batchSize,
            @Value("${telemetry.ingestion.batch-timeout-ms:100}") long batchTimeoutMs
    ) {
        this.jsonMapper = jsonMapper;
        this.databaseClient=databaseClient;
        this.batchSize = batchSize;
        this.batchTimeout = Duration.ofMillis(batchTimeoutMs);
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

        return databaseClient.inConnection(connection -> executeBatch(connection,chunk))
                .map(insertedCount->new BatchInsertResult(
                        insertedCount,
                        chunk.size()-insertedCount,
                        0
                ));
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
        bindNullable(statement, 4, point.speed(), Float.class);
        bindNullable(statement, 5, point.altitude(), Float.class);
        bindNullable(statement, 6, point.heading(), Float.class);
        bindNullable(statement, 7, point.gpsAccuracy(), Float.class);
        if (point.sensors() != null && !point.sensors().isEmpty()) {
            statement.bind(8, toJson(point.sensors()));
        } else {
            statement.bindNull(8, String.class);
        }
    }

    private static <T> void bindNullable(Statement statement, int index, T value, Class<T> type) {
        if (value != null) {
            statement.bind(index, value);
        } else {
            statement.bindNull(index, type);
        }
    }

    private String toJson(Map<String, Object> sensors) {
        try {
            return jsonMapper.writeValueAsString(sensors);
        } catch (Exception e) {
            log.error("Failed to serialize sensors map to JSON: {}", sensors, e);
            return "{}";
        }
    }
}