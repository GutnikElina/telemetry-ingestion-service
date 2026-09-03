package com.innowise.telemetry_ingestion_service.kafka;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import tools.jackson.databind.ObjectMapper;

import java.util.List;

@Log4j2
@Component
@RequiredArgsConstructor
public class TelemetryPointKafkaProducerImpl implements TelemetryPointKafkaProducer {

    private final ObjectMapper objectMapper;
    private final KafkaSender<String, String> kafkaSender;

    @Value("${kafka.telemetry-topic.name}")
    private String topicName;

    @Override
    public Mono<Void> sendPointsList(List<TelemetryPoint> points) {
        Flux<SenderRecord<String, String, String>> records = Flux.fromIterable(points)
                .map(point -> {
                    String jsonPayload = objectMapper.writeValueAsString(point);
                    ProducerRecord<String, String> producerRecord = new ProducerRecord<>(
                            topicName,
                            point.deviceId().toString(),
                            jsonPayload
                    );
                    return SenderRecord.create(producerRecord, point.deviceId().toString());
                });

        return kafkaSender.send(records)
                .doOnError(e -> log.error("Error sending telemetry batch to Kafka", e))
                .then();
    }
}
