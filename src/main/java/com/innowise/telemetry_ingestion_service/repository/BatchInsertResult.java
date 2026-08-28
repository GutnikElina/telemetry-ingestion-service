package com.innowise.telemetry_ingestion_service.repository;

public record BatchInsertResult(
        long inserted,
        long duplicates,
        long rejected
) {
    public static BatchInsertResult empty() {
        return new BatchInsertResult(0, 0, 0);
    }

    public BatchInsertResult merge(BatchInsertResult other) {
        return new BatchInsertResult(
                this.inserted + other.inserted,
                this.duplicates + other.duplicates,
                this.rejected + other.rejected
        );
    }
}