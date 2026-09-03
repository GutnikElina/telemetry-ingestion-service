package com.innowise.telemetry_ingestion_service.netty.teltonika;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.data.redis.core.ReactiveStringRedisTemplate;
import org.springframework.data.redis.core.ReactiveValueOperations;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;

import static com.innowise.telemetry_ingestion_service.netty.teltonika.Constants.DEVICE_ID_KEY;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TeltonikaAuthHandlerAdapterTest {

    @Mock
    private ObjectProvider<TeltonicaDecoderHandlerAdapter> decoderProvider;

    @Mock
    private ReactiveStringRedisTemplate redisTemplate;

    @Mock
    private ReactiveValueOperations<String, String> valueOperations;

    @Mock
    private TeltonicaDecoderHandlerAdapter decoderMock;

    private EmbeddedChannel channel;

    @BeforeEach
    void setUp() {
        lenient().when(redisTemplate.opsForValue()).thenReturn(valueOperations);
        lenient().when(decoderProvider.getObject()).thenReturn(decoderMock);

        channel = new EmbeddedChannel(new TeltonikaAuthHandlerAdapter(decoderProvider, redisTemplate));
    }

    @AfterEach
    void tearDown() {
        if (channel != null) {
            channel.finishAndReleaseAll();
        }
    }

    @Test
    void shouldNotDecodeIfLessThanTwoBytes() {
        //Arrange
        ByteBuf buf = Unpooled.buffer();
        buf.writeByte(1);

        //Act
        channel.writeInbound(buf);

        //Assert
        assertEquals(1, buf.readableBytes());
        verifyNoInteractions(redisTemplate);
    }

    @Test
    void shouldNotDecodeIfMissingImeiBytes() {
        //Arrange
        final short imeiLength = 15;
        final String imei = "12345";
        final int actualBytesLength = 7;

        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(imeiLength);
        buf.writeCharSequence(imei, StandardCharsets.US_ASCII);

        //Act
        channel.writeInbound(buf);

        //Assert
        assertEquals(actualBytesLength, buf.readableBytes());
        verifyNoInteractions(redisTemplate);
    }

    @Test
    void shouldAuthenticateSuccessfully() {
        //Arrange
        String imei = "123456789012345";
        String deviceId = "device-123";

        when(valueOperations.get("imei:" + imei)).thenReturn(Mono.just(deviceId));

        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(imei.length());
        buf.writeCharSequence(imei, StandardCharsets.US_ASCII);

        //Act
        channel.writeInbound(buf);
        channel.runPendingTasks();

        //Assert
        ByteBuf response = channel.readOutbound();
        assertNotNull(response);
        assertEquals(0x01, response.readByte());
        response.release();

        assertEquals(deviceId, channel.attr(DEVICE_ID_KEY).get());

        assertNull(channel.pipeline().get(TeltonikaAuthHandlerAdapter.class), "AuthHandler should be deleted");
        assertNotNull(channel.pipeline().get(LengthFieldBasedFrameDecoder.class), "LengthFieldDecoder should be added");
        assertNotNull(channel.pipeline().get(TeltonicaDecoderHandlerAdapter.class), "DecoderHandler should be added");

        assertTrue(channel.isOpen());
    }

    @Test
    void shouldCloseChannelWhenImeiNotFound() {
        //Arrange
        String imei = "111111111111111";

        when(valueOperations.get("imei:" + imei)).thenReturn(Mono.empty());

        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(imei.length());
        buf.writeCharSequence(imei, StandardCharsets.US_ASCII);

        //Act
        channel.writeInbound(buf);
        channel.runPendingTasks();

        //Assert
        assertFalse(channel.isOpen());
    }

    @Test
    void shouldCloseChannelWhenRedisErrorOccurs() {
        //Arrange
        String imei = "222222222222222";

        when(valueOperations.get("imei:" + imei)).thenReturn(Mono.error(new RuntimeException("Redis timeout")));

        ByteBuf buf = Unpooled.buffer();
        buf.writeShort(imei.length());
        buf.writeCharSequence(imei, StandardCharsets.US_ASCII);

        //Act
        channel.writeInbound(buf);
        channel.runPendingTasks();

        //Assert
        assertFalse(channel.isOpen());
    }
}