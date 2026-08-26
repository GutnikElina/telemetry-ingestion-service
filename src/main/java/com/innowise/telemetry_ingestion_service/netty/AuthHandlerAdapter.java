package com.innowise.telemetry_ingestion_service.netty;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Slf4j
@Component
@Scope("prototype")
public class AuthHandlerAdapter extends ChannelInboundHandlerAdapter {

    public static final AttributeKey<String> DEVICE_IMEI_KEY = AttributeKey.valueOf("DEVICE_IMEI");

    private final ObjectProvider<TeltonicaDecoderHandlerAdapter> decoderProvider;

    public AuthHandlerAdapter(ObjectProvider<TeltonicaDecoderHandlerAdapter> decoderProvider) {
        this.decoderProvider = decoderProvider;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if(msg instanceof ByteBuf in) {
            try {
                byte imeiSize = in.readByte(); //todo redis check here?
                String imei = in.readString(imeiSize, StandardCharsets.US_ASCII);
                ctx.channel().attr(DEVICE_IMEI_KEY).set(imei);

                ByteBuf response = ctx.alloc().buffer(1);
                response.writeByte(0x01);
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
