package com.innowise.telemetry_ingestion_service.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@Scope("prototype")
public class TeltonicaDecoderHandlerAdapter extends ChannelInboundHandlerAdapter {
    private final static String preambleErrorMsg = "Corrupted package occurred. Preamble should be equal to 0, but was %d";
    private final static String crcMismatchErrorMsg = "Package was corrupted. Calculated IBM CRC-16 (%d) doesn't match with expected (%d)";

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof ByteBuf in) {
            try {
                log.debug("Received {} bytes", in.readableBytes());

                int preamble = in.readInt();
                if (preamble != 0) {
                    throw new IllegalArgumentException(String.format(preambleErrorMsg, preamble));
                }

                int contentLength = in.readInt();
                int expectedCrc = in.getInt(contentLength);

                int calculatedCrc = Crc16IbmUtils.calculate(in, 0, contentLength);

                if (expectedCrc != calculatedCrc) {
                    throw new IllegalArgumentException(String.format(crcMismatchErrorMsg, calculatedCrc, expectedCrc));
                }

                byte codecId = in.readByte();
                byte numberOfData = in.readByte();

                for (int i = 0; i < numberOfData; i++) {
                    long time = in.readLong();
                    byte priority = in.readByte();

                    int longitude = in.readInt();
                    int latitude = in.readInt();
                    short altitude = in.readShort();
                    short angle = in.readShort();
                    byte satellites = in.readByte();
                    short speed = in.readShort();

                    //todo создать рекорд
                }

            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        log.error("Teltonica parsing error:\n", cause);
        ctx.close();
    }
}
