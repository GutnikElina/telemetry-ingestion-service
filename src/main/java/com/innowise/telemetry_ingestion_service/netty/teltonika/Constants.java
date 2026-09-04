package com.innowise.telemetry_ingestion_service.netty.teltonika;

import io.netty.util.AttributeKey;

public class Constants {
    public static final AttributeKey<String> DEVICE_ID_KEY = AttributeKey.valueOf("DEVICE_IMEI");
}
