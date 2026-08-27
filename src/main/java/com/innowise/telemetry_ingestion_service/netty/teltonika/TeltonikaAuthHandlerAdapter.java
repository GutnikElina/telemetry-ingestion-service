package com.innowise.telemetry_ingestion_service.netty.teltonika;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

import static com.innowise.telemetry_ingestion_service.netty.teltonika.Constants.DEVICE_IMEI_KEY;

@Slf4j
@Component
@RequiredArgsConstructor
@Scope("prototype")
public class TeltonikaAuthHandlerAdapter extends ChannelInboundHandlerAdapter {

    private final ObjectProvider<TeltonicaDecoderHandlerAdapter> decoderProvider;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if(msg instanceof ByteBuf in) {
            try {
                byte imeiSize = in.readByte();
                String imei = in.readString(imeiSize, StandardCharsets.US_ASCII);
                //todo redis imei to deviceId

                ctx.channel().attr(DEVICE_IMEI_KEY).set(imei);

                ByteBuf response = ctx.alloc().buffer(1);
                response.writeByte(0x01);
                log.info("New client with id {} successfully connected.", imei);
                ctx.writeAndFlush(response);

                ctx.pipeline().addLast(new LengthFieldBasedFrameDecoder(2048, 4, 4, 4, 0));
                ctx.pipeline().addLast(decoderProvider.getObject());

                ctx.pipeline().remove(this);
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }
}
