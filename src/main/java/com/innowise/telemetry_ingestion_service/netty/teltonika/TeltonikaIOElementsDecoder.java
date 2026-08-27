package com.innowise.telemetry_ingestion_service.netty.teltonika;

import io.netty.buffer.ByteBuf;

import java.util.Map;

public interface TeltonikaIOElementsDecoder {

     Map<Byte, Object> decode(ByteBuf in);
}
