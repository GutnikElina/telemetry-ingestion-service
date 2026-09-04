package com.innowise.telemetry_ingestion_service.netty.teltonika;

import com.innowise.telemetry_ingestion_service.netty.AbstractTcpServer;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class TeltonicaTcpServer extends AbstractTcpServer {

    protected TeltonicaTcpServer(
            @Value("${telemetry.ports.tcp.teltonika}") int port,
            ObjectProvider<TeltonikaAuthHandlerAdapter> decoder
    ) {
        super(port, decoder);
    }
}
