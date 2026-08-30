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
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

import static com.innowise.telemetry_ingestion_service.netty.teltonika.Constants.DEVICE_ID_KEY;

@Slf4j
@Component
@RequiredArgsConstructor
@Scope("prototype")
public class TeltonikaAuthHandlerAdapter extends ChannelInboundHandlerAdapter {

    private final ObjectProvider<TeltonicaDecoderHandlerAdapter> decoderProvider;
    private final ReactiveStringRedisTemplate redisTemplate;

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
        if (msg instanceof ByteBuf in) {
            try {
                short imeiSize = in.readShort();  //todo проверка на целость
                String imei = in.readString(imeiSize, StandardCharsets.US_ASCII);

                redisTemplate.opsForValue().get("imei" + imei)
                        .subscribe(
                                deviceId -> proceedAuthLogic(ctx, deviceId),
                                error -> onRedisError(ctx, error),
                                () -> onImeiAbsence(ctx, imei)
                        );
            } finally {
                ReferenceCountUtil.release(msg);
            }
        } else {
            ctx.fireChannelRead(msg);
        }
    }


    private void proceedAuthLogic(ChannelHandlerContext ctx, String deviceId) {
        ctx.channel().eventLoop().execute(() -> {
            ctx.channel().attr(DEVICE_ID_KEY).set(deviceId);

            ByteBuf response = ctx.alloc().buffer(1);
            response.writeByte(0x01);
            log.info("New client with id {} successfully connected.", deviceId);
            ctx.writeAndFlush(response);

            ctx.pipeline().addLast(new LengthFieldBasedFrameDecoder(2048, 4, 4, 4, 0));
            ctx.pipeline().addLast(decoderProvider.getObject());

            ctx.pipeline().remove(this);
        });
    }

    private void onRedisError(ChannelHandlerContext ctx, Throwable error) {
        log.error("Redis error occurred.\n", error);
        ctx.channel().eventLoop().execute(ctx::close);
    }

    private void onImeiAbsence(ChannelHandlerContext ctx, String imei) {
        log.debug("There is no deviceId for imei: {}", imei);
        ctx.channel().eventLoop().execute(ctx::close);
    }
}
