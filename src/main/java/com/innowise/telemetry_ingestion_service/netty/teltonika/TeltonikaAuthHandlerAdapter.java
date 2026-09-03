package com.innowise.telemetry_ingestion_service.netty.teltonika;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.util.ReferenceCountUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.Scope;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;

import static com.innowise.telemetry_ingestion_service.netty.teltonika.Constants.DEVICE_ID_KEY;

@Slf4j
@Component
@RequiredArgsConstructor
@Scope("prototype")
public class TeltonikaAuthHandlerAdapter extends ByteToMessageDecoder {

    private final ObjectProvider<TeltonicaDecoderHandlerAdapter> decoderProvider;
    private final ReactiveStringRedisTemplate redisTemplate;

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) {
        final byte IMEI_SIZE_BYTES = 2;

        if (in.readableBytes() < IMEI_SIZE_BYTES) {
            return;
        }

        in.markReaderIndex();
        short imeiSize = in.readShort();
        if (in.readableBytes() < imeiSize) {
            in.resetReaderIndex();
            return;
        }

        String imei = in.readCharSequence(imeiSize, StandardCharsets.US_ASCII).toString();

        ctx.channel().config().setAutoRead(false);
        redisTemplate.opsForValue().get("imei:" + imei)
                .doOnSuccess(deviceId -> {
                    if (deviceId == null) {
                        log.debug("There is no deviceId for imei: {}", imei);
                        ctx.channel().eventLoop().execute(ctx::close);
                    }
                }).subscribe(
                        deviceId -> proceedAuthLogic(ctx, deviceId),
                        error -> onRedisError(ctx, error)
                );
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
            ctx.channel().config().setAutoRead(true);
        });
    }

    private void onRedisError(ChannelHandlerContext ctx, Throwable error) {
        log.error("Redis error occurred.\n", error);
        ctx.channel().eventLoop().execute(ctx::close);
    }
}
