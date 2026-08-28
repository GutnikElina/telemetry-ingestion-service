package com.innowise.telemetry_ingestion_service.support;

import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public abstract class AbstractIntegrationTest {

    static final PostgreSQLContainer<?> POSTGRES = new PostgreSQLContainer<>(
            DockerImageName.parse("timescale/timescaledb-ha:pg16")
                    .asCompatibleSubstituteFor("postgres")
    )
            .withDatabaseName("telemetry_db")
            .withUsername("telemetry_user")
            .withPassword("telemetry_password");

    static {
        POSTGRES.start();

        try (Connection conn = DriverManager.getConnection(
                POSTGRES.getJdbcUrl(),
                POSTGRES.getUsername(),
                POSTGRES.getPassword()
        );
             Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;");
            stmt.execute("CREATE EXTENSION IF NOT EXISTS postgis;");

        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize PostgreSQL extensions in test container", e);
        }
    }

    @DynamicPropertySource
    static void registerDynamicProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.r2dbc.url", () ->
                "r2dbc:postgresql://%s:%d/%s".formatted(
                        POSTGRES.getHost(),
                        POSTGRES.getMappedPort(5432),
                        POSTGRES.getDatabaseName()
                )
        );
        registry.add("spring.r2dbc.username", POSTGRES::getUsername);
        registry.add("spring.r2dbc.password", POSTGRES::getPassword);

        registry.add("spring.flyway.url", () ->
                "jdbc:postgresql://%s:%d/%s".formatted(
                        POSTGRES.getHost(),
                        POSTGRES.getMappedPort(5432),
                        POSTGRES.getDatabaseName()
                )
        );
        registry.add("spring.flyway.user", POSTGRES::getUsername);
        registry.add("spring.flyway.password", POSTGRES::getPassword);
    }
}