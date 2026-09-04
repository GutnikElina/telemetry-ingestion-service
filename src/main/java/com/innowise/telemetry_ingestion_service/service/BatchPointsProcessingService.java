package com.innowise.telemetry_ingestion_service.service;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import reactor.core.publisher.Mono;

import java.util.List;

public interface BatchPointsProcessingService {

    Mono<List<TelemetryPoint>> processPoints(List<TelemetryPoint> points);
}
