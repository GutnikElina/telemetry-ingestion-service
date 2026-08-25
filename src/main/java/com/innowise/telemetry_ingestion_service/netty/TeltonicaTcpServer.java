package com.innowise.telemetry_ingestion_service.netty;

import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TeltonicaTcpServer extends AbstractTcpServer {

    protected TeltonicaTcpServer(
            @Value("${telemetry.ports.tcp.teltonika}") int port,
            ObjectProvider<TeltonicaDecoder> decoder,
            LengthFieldBasedFrameDecoder byteBuffer
    ) {
        super(port, decoder, byteBuffer);
    }
}
