package com.innowise.telemetry_ingestion_service.netty;

import io.netty.channel.ChannelHandler;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import reactor.netty.DisposableServer;
import reactor.netty.tcp.TcpServer;

@Slf4j
public abstract class AbstractTcpServer {

    private final int port;
    private final ObjectProvider<? extends ChannelHandler> decoder;
    private final LengthFieldBasedFrameDecoder frameBuffer;
    private DisposableServer server;


    public AbstractTcpServer(
            int port,
            ObjectProvider<? extends ChannelHandler> decoder,
            LengthFieldBasedFrameDecoder frameBuffer
    ) {
        this.port = port;
        this.decoder = decoder;
        this.frameBuffer = frameBuffer;
    }

    @PostConstruct
    private void init() {
        this.server = TcpServer.create()
                .port(port)
                .doOnConnection(connection -> {
                    log.debug("New connection on port {}", port);
                    connection.addHandlerLast(frameBuffer);
                    connection.addHandlerLast(decoder.getObject());
                })
                .bindNow();
        log.info("Server {} started on port {}.", this.getClass().getName(), this.port);
    }

    @PreDestroy
    private void destroy() {
        if (server != null) {
            server.disposeNow();
            log.info("Server {} stopped.", this.getClass().getName());
        }
    }
}
