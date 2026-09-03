package com.innowise.telemetry_ingestion_service.service;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import com.innowise.telemetry_ingestion_service.kafka.TelemetryPointKafkaProducer;
import com.innowise.telemetry_ingestion_service.repository.TelemetryPointRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.reactivestreams.Publisher;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class BatchPointsProcessingServiceImplTest {
    @Mock
    private TelemetryPointRepository repository;

    @Mock
    private ReactiveStringRedisTemplate redis;

    @Mock
    private ReactiveValueOperations<String, String> valueOperations;

    @Mock
    private TelemetryPointKafkaProducer kafka;

    @InjectMocks
    private BatchPointsProcessingServiceImpl service;

    private TelemetryPoint point1;
    private TelemetryPoint point2;
    private final String device1Id = "11111111-1111-1111-1111-111111111111";
    private final String device2Id = "22222222-2222-2222-2222-222222222222";

    @BeforeEach
    void setUp() {
        point1 = new TelemetryPoint(
                UUID.fromString(device1Id),
                Instant.parse("2026-09-04T10:00:00Z"),
                10.0, 20.0, 100.0f, 0.0f, (byte) 1, 60.0f, null
        );
        point2 = new TelemetryPoint(
                UUID.fromString(device2Id),
                Instant.parse("2026-09-04T10:00:01Z"),
                11.0, 21.0, 100.0f, 0.0f, (byte) 1, 60.0f, null
        );

        lenient().when(redis.opsForValue()).thenReturn(valueOperations);
    }

    @Test
    void processPoints_AllUnique_SavesAndSendsToKafka() {
        when(valueOperations.setIfAbsent(anyString(), eq("1"), any(Duration.class)))
                .thenReturn(Mono.just(true));

        when(repository.saveAll(anyIterable())).thenReturn(Flux.just(point1, point2));
        when(kafka.sendPointsList(anyList())).thenReturn(Mono.empty());

        List<TelemetryPoint> input = List.of(point1, point2);

        StepVerifier.create(service.processPoints(input))
                .expectNextMatches(result -> result.size() == 2 && result.containsAll(input))
                .verifyComplete();

        verify(repository).saveAll(input);
        verify(kafka).sendPointsList(input);
    }

    @Test
    void processPoints_AllDuplicates_ReturnsEmptyListAndSkipsDb() {
        when(valueOperations.setIfAbsent(anyString(), eq("1"), any(Duration.class)))
                .thenReturn(Mono.just(false));

        List<TelemetryPoint> input = List.of(point1, point2);

        StepVerifier.create(service.processPoints(input))
                .expectNextMatches(List::isEmpty)
                .verifyComplete();

        verifyNoInteractions(repository);
        verifyNoInteractions(kafka);
    }

    @Test
    void processPoints_DbSaveFails_DeletesRedisKeysAndPropagatesError() {
        when(valueOperations.setIfAbsent(anyString(), eq("1"), any(Duration.class)))
                .thenReturn(Mono.just(true));

        RuntimeException dbError = new RuntimeException("DB Connection failed");
        when(repository.saveAll(anyIterable())).thenReturn(Flux.error(dbError));

        when(redis.delete(any(Publisher.class))).thenReturn(Mono.just(2L));

        List<TelemetryPoint> input = List.of(point1, point2);

        StepVerifier.create(service.processPoints(input))
                .expectErrorMatches(throwable -> throwable.equals(dbError))
                .verify();

        verify(redis).delete(any(Publisher.class));
        verifyNoInteractions(kafka);
    }
}