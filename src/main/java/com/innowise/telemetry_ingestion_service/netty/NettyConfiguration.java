package com.innowise.telemetry_ingestion_service.netty;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class NettyConfiguration {

    @Bean
    public LengthFieldBasedFrameDecoder frameBuffer() {
        return new LengthFieldBasedFrameDecoder(2048, 4, 4, 4, 0);
    }
}
