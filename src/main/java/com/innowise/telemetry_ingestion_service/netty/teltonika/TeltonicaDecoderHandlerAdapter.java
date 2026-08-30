package com.innowise.telemetry_ingestion_service.netty.teltonika;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import com.innowise.telemetry_ingestion_service.repository.TelemetryPointRepository;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.innowise.telemetry_ingestion_service.netty.teltonika.Constants.DEVICE_ID_KEY;

@Slf4j
@Component
@Scope("prototype")
@RequiredArgsConstructor
public class TeltonicaDecoderHandlerAdapter extends ChannelInboundHandlerAdapter {
    private final static String preambleErrorMsg = "Corrupted package occurred. Preamble should be equal to 0, but was %d";
    private final static String crcMismatchErrorMsg = "Package was corrupted. Calculated IBM CRC-16 (%d) doesn't match with expected (%d)";

    private final TeltonikaIOElementsDecoder ioDecoder;
    private final TelemetryPointRepository repository;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof ByteBuf in) {
            try {
                log.debug("Received {} bytes.\nStarting to parse.", in.readableBytes());
                validateTheData(in);

                List<TelemetryPoint> points = new ArrayList<>();
                byte codecId = in.readByte();
                byte numberOfData = in.readByte();
                populatePointsList(ctx, in, points, codecId, numberOfData);

                //todo rest of logic
                // gps drift
                // redundancy check
                repository.saveAll(points).collectList().subscribe(
                        savedPoints -> {
                            ctx.channel().eventLoop().execute(() -> {
                                byte bufferSize = 4;
                                ByteBuf successResponse = ctx.alloc().buffer(bufferSize);
                                successResponse.writeInt(numberOfData);
                                ctx.writeAndFlush(successResponse);
                            });
                        },
                        error -> {
                            log.error("Persistence Error occurred:\n", error);
                            ctx.channel().eventLoop().execute(ctx::close);
                        });

            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    private static void validateTheData(ByteBuf in) {
        int preamble = in.readInt();
        if (preamble != 0) {
            throw new IllegalArgumentException(String.format(preambleErrorMsg, preamble));
        }

        int contentLength = in.readInt();
        int expectedCrc = in.getInt(contentLength);

        int calculatedCrc = Crc16IbmUtils.calculate(in, 0, contentLength);
        log.debug("Checking CRC-16/IBM. Value: {}", calculatedCrc);
        if (expectedCrc != calculatedCrc) {
            throw new IllegalArgumentException(String.format(crcMismatchErrorMsg, calculatedCrc, expectedCrc));
        }
    }

    private void populatePointsList(ChannelHandlerContext ctx, ByteBuf in, List<TelemetryPoint> points,
                                    byte codecId, byte numberOfData) {
        String imei = ctx.channel().attr(DEVICE_ID_KEY).get();

        for (int i = 0; i < numberOfData; i++) {
            long time = in.readLong();
            byte priority = in.readByte();

            int longitude = in.readInt();
            int latitude = in.readInt();
            short altitude = in.readShort();
            short angle = in.readShort();
            byte satellites = in.readByte();
            short speed = in.readShort();

            Map<Byte, Object> sensorsData = ioDecoder.decode(in);

            TelemetryPoint telemetryPoint = new TelemetryPoint(
                    imei,
                    Instant.ofEpochMilli(time).atZone(ZoneOffset.UTC).toLocalDateTime(),
                    latitude,
                    longitude,
                    altitude,
                    angle,
                    satellites,
                    speed,
                    sensorsData);

            log.debug("Telemetry point with priority {} and codec {} was successfully parsed:\n {}",
                    priority, codecId, telemetryPoint);

            points.add(telemetryPoint);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Teltonica parsing error:\n", cause);
        ctx.close();
    }
}
