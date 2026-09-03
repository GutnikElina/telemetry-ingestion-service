package com.innowise.telemetry_ingestion_service.netty.teltonika;

import com.innowise.telemetry_ingestion_service.entity.TelemetryPoint;
import com.innowise.telemetry_ingestion_service.service.BatchPointsProcessingService;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelConfig;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.EventLoop;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.*;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TeltonicaDecoderHandlerAdapterTest {
    @Mock
    private TeltonikaIOElementsDecoder ioDecoder;
    @Mock
    private BatchPointsProcessingService processor;

    @Mock
    private ChannelHandlerContext ctx;
    @Mock
    private Channel channel;
    @Mock
    private ChannelConfig config;
    @Mock
    private EventLoop eventLoop;
    @Mock
    private Attribute<String> deviceIdAttribute;

    @InjectMocks
    private TeltonicaDecoderHandlerAdapter handler;

    @Captor
    private ArgumentCaptor<List<TelemetryPoint>> pointsCaptor;

    private static final String DEVICE_ID = UUID.randomUUID().toString();
    private static final int CONTENT_LENGTH = 40;

    @BeforeEach
    void setUp() {
        // We only mock base Netty components leniently, configuring specifics in individual tests
        lenient().when(ctx.channel()).thenReturn(channel);
        lenient().when(channel.config()).thenReturn(config);
        lenient().when(channel.eventLoop()).thenReturn(eventLoop);

        lenient().when(channel.attr(any(AttributeKey.class))).thenReturn(deviceIdAttribute);
        lenient().when(deviceIdAttribute.get()).thenReturn(DEVICE_ID);
        lenient().when(ctx.alloc()).thenReturn(UnpooledByteBufAllocator.DEFAULT);

        // Emulate EventLoop immediate execution for reactive callbacks
        lenient().doAnswer(invocation -> {
            Runnable task = invocation.getArgument(0);
            task.run();
            return null;
        }).when(eventLoop).execute(any(Runnable.class));
    }

    @Test
    @DisplayName("Should pass down the pipeline if message is not a ByteBuf")
    void shouldFireChannelRead_whenMessageNotByteBuf() {
        Object msg = new Object();
        handler.channelRead(ctx, msg);
        verify(ctx).fireChannelRead(msg);
        verifyNoInteractions(processor, ioDecoder);
    }

    @Test
    @DisplayName("Should throw exception and release buffer if preamble is invalid")
    void shouldThrowException_whenPreambleIsInvalid() {
        ByteBuf buf = Unpooled.buffer().writeInt(1); // Invalid preamble

        IllegalArgumentException exception = assertThrows(
                IllegalArgumentException.class,
                () -> handler.channelRead(ctx, buf)
        );

        assertTrue(exception.getMessage().contains("Preamble should be equal to 0"));
        assertEquals(0, buf.refCnt(), "ByteBuf memory leak: buffer was not released");
    }

    @Test
    @DisplayName("Should throw exception and release buffer if CRC check fails")
    void shouldThrowException_whenCrcMismatch() {
        int calculatedCrc = 1111;

        ByteBuf buf = buildPayload();

        try (MockedStatic<Crc16IbmUtils> crcMock = mockStatic(Crc16IbmUtils.class)) {
            crcMock.when(() -> Crc16IbmUtils.calculate(any(ByteBuf.class), eq(0), eq(CONTENT_LENGTH)))
                    .thenReturn(calculatedCrc);

            IllegalArgumentException exception = assertThrows(
                    IllegalArgumentException.class,
                    () -> handler.channelRead(ctx, buf)
            );

            assertTrue(exception.getMessage().contains("Package was corrupted"));
            assertEquals(0, buf.refCnt(), "ByteBuf memory leak: buffer was not released");
        }
    }

    @Test
    @DisplayName("Should successfully parse payload, process points, and respond")
    void shouldSuccessfullyParseAndProcessPoints() {
        int expectedCrc = 61994;
        byte numberOfData = 1;

        ByteBuf buf = buildPayload();

        when(ioDecoder.decode(any(ByteBuf.class))).thenReturn(Collections.emptyMap());
        when(processor.processPoints(any())).thenReturn(Mono.just(List.of(new TelemetryPoint(UUID.fromString(DEVICE_ID), Instant.MIN, 0.0, 0.0, 0.0f, 0.0f, (byte) 0, 0, null))));

        try (MockedStatic<Crc16IbmUtils> crcMock = mockStatic(Crc16IbmUtils.class)) {
            crcMock.when(() -> Crc16IbmUtils.calculate(any(ByteBuf.class), eq(0), eq(CONTENT_LENGTH)))
                    .thenReturn(expectedCrc);

            // ACT
            handler.channelRead(ctx, buf);

            // ASSERT: Verify flow control
            verify(config).setAutoRead(false);
            verify(config).setAutoRead(true);

            // ASSERT: Verify processing payload
            verify(processor).processPoints(pointsCaptor.capture());
            List<TelemetryPoint> parsedPoints = pointsCaptor.getValue();
            assertEquals(1, parsedPoints.size());
            assertEquals(UUID.fromString(DEVICE_ID), parsedPoints.getFirst().deviceId());

            // ASSERT: Verify network response sent to device
            ArgumentCaptor<ByteBuf> responseCaptor = ArgumentCaptor.forClass(ByteBuf.class);
            verify(ctx).writeAndFlush(responseCaptor.capture());

            ByteBuf responseBuf = responseCaptor.getValue();
            assertEquals(numberOfData, responseBuf.readInt());

            // Memory leak checks
            assertEquals(0, buf.refCnt(), "Inbound buffer was not released");
        }
    }

    @Test
    @DisplayName("Should close context gracefully when processing pipeline fails")
    void shouldCloseContext_whenProcessingFails() {
        int expectedCrc = 61994;
        ByteBuf buf = buildPayload();

        when(ioDecoder.decode(any(ByteBuf.class))).thenReturn(Collections.emptyMap());
        when(processor.processPoints(any())).thenReturn(Mono.error(new RuntimeException("DB Connection failed")));

        try (MockedStatic<Crc16IbmUtils> crcMock = mockStatic(Crc16IbmUtils.class)) {
            crcMock.when(() -> Crc16IbmUtils.calculate(any(ByteBuf.class), eq(0), eq(CONTENT_LENGTH)))
                    .thenReturn(expectedCrc);

            handler.channelRead(ctx, buf);

            verify(config).setAutoRead(false);
            verify(ctx).close(); // Ensures channel drops on internal processing errors

            assertEquals(0, buf.refCnt(), "Inbound buffer was not released on processor error");
        }
    }

    @Test
    @DisplayName("Should log and close channel when unhandled exception caught")
    void shouldCloseChannel_whenExceptionCaught() {
        handler.exceptionCaught(ctx, new RuntimeException("Unexpected crash"));
        verify(ctx).close();
    }

    /**
     * Helper to assemble a mocked valid/invalid Teltonika payload.
     */
    private ByteBuf buildPayload() {
        ByteBuf buf = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump("000000000000002808010000016B40D9AD80010000000000000000000000000000000103021503010101425E100000010000F22A"));
        return buf.readerIndex(0); // Reset for the handler
    }
}