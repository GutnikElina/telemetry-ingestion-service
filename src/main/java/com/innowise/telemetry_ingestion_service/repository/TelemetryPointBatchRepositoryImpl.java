package com.innowise.telemetry_ingestion_service.repository;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import lombok.AllArgsConstructor;
import org.reactivestreams.Publisher;
import org.springframework.r2dbc.core.DatabaseClient;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Repository
@AllArgsConstructor
public class TelemetryPointBatchRepositoryImpl implements TelemetryPointBatchRepository {

    private final DatabaseClient databaseClient;

    @Override
    public Mono<Void> saveAll(Publisher<TelemetryPoint> points) {
        return Flux.from(points)
                .collectList()
                .flatMap(this::insertBatch);
    }

    private Mono<Void> insertBatch(List<TelemetryPoint> batch) {
        if(batch.isEmpty()){
            return Mono.empty();
        }

        StringBuilder sql = new StringBuilder(
                "INSERT INTO telemetry_points (time, device_id, location, speed, altitude, sensors) VALUES "
        );

        int paramIndex = 1;

        for (int i = 0; i < batch.size(); i++) {
            sql.append(String.format(
                    "($%d, $%d, ST_GeomFromText($%d, 4326), $%d, $%d, $%d::jsonb)",
                    paramIndex,
                    paramIndex + 1,
                    paramIndex + 2,
                    paramIndex + 3,
                    paramIndex + 4,
                    paramIndex + 5
            ));

            paramIndex += 6;

            if (i < batch.size() - 1) {
                sql.append(", ");
            }
        }

        DatabaseClient.GenericExecuteSpec spec = databaseClient.sql(sql.toString());

        int bindIndex = 0;
        for (TelemetryPoint point : batch) {
            spec = spec.bind(bindIndex++, point.time());
            spec = spec.bind(bindIndex++, point.deviceId());
            spec = spec.bind(bindIndex++, point.location());

            if (point.speed() != null) {
                spec = spec.bind(bindIndex++, point.speed());
            } else {
                spec = spec.bindNull(bindIndex++, Float.class);
            }

            if (point.altitude() != null) {
                spec = spec.bind(bindIndex++, point.altitude());
            } else {
                spec = spec.bindNull(bindIndex++, Float.class);
            }

            if (point.sensors() != null) {
                spec = spec.bind(bindIndex++, point.sensors().asString());
            } else {
                spec = spec.bindNull(bindIndex++, String.class);
            }
        }

        return spec.then();
    }
}