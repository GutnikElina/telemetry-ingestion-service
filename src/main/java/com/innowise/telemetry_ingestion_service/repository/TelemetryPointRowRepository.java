package com.innowise.telemetry_ingestion_service.repository;

import org.springframework.data.r2dbc.repository.Query;
import org.springframework.data.repository.Repository;
import org.springframework.data.repository.query.Param;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

public interface TelemetryPointRowRepository extends Repository<TelemetryPointRow, Void> {

    String SELECT_COLUMNS = """
            device_id, time,
            ST_Y(location) AS latitude, ST_X(location) AS longitude,
            speed, altitude, heading, satellites, sensors::text AS sensors
            """;

    @Query("SELECT " + SELECT_COLUMNS + " FROM telemetry_points "
            + "WHERE device_id = :deviceId AND time BETWEEN :from AND :to "
            + "ORDER BY time ASC")
    Flux<TelemetryPointRow> findByDeviceIdAndTimeBetweenOrderByTimeAsc(
            @Param("deviceId") UUID deviceId,
            @Param("from") Instant from,
            @Param("to") Instant to
    );

    @Query("SELECT " + SELECT_COLUMNS + " FROM telemetry_points "
            + "WHERE device_id = :deviceId ORDER BY time DESC LIMIT 1")
    Mono<TelemetryPointRow> findLatestPoint(@Param("deviceId") UUID deviceId);
}