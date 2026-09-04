package com.innowise.telemetry_ingestion_service.service;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import com.innowise.telemetry_ingestion_service.kafka.TelemetryPointKafkaProducer;
import com.innowise.telemetry_ingestion_service.repository.TelemetryPointRepository;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Collections;
import java.util.List;

@Slf4j
@Service
@AllArgsConstructor
public class BatchPointsProcessingServiceImpl implements BatchPointsProcessingService {

    private final TelemetryPointRepository repository;
    private final ReactiveStringRedisTemplate redis;
    private final TelemetryPointKafkaProducer kafka;

    @Override
    public Mono<List<TelemetryPoint>> processPoints(List<TelemetryPoint> points) {
        return Flux.fromIterable(points).filterWhen(point ->
                redis.opsForValue().setIfAbsent(
                        point.deviceId() + ":" + point.timestamp().toEpochMilli(),
                        "1",
                        Duration.ofMinutes(5)
                )  //todo gps Drift
        ).collectList().flatMap(uniquePoints -> {
            if (uniquePoints.isEmpty()) {
                return Mono.just(Collections.emptyList());
            }

            return repository.saveAll(uniquePoints).onErrorResume(error -> {
                        log.error("Error saving points to database", error);
                        List<String> pointsToDelete = uniquePoints.stream().map(
                                point -> point.deviceId() + ":" + point.timestamp().toEpochMilli()
                        ).toList();
                        return redis.delete(Flux.fromIterable(pointsToDelete)).then(Mono.error(error));
                    })
                    .collectList()
                    .flatMap(kafka::sendPointsList)
                    .thenReturn(uniquePoints);
        });
    }
}
