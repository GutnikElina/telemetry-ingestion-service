package com.innowise.telemetry_ingestion_service.netty.teltonika;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class TeltonikaIOElementsCodec8DecoderImplTest {
    private TeltonikaIOElementsCodec8DecoderImpl decoder;
    private ByteBuf buf;

    @BeforeEach
    void setUp() {
        decoder = new TeltonikaIOElementsCodec8DecoderImpl();
    }

    @AfterEach
    void tearDown() {
        if (buf != null && buf.refCnt() > 0) {
            buf.release();
        }
    }

    @ParameterizedTest
    @CsvSource({"0105021503010101425E0F01F10000601A014E0000000000000000010000C7CF, 2, 1, 1, 1",
            "0103021503010101425E100000010000F22A, 2, 1, 0, 0",
            "01010101000000000000016B40D5C198010000000000000000000000000000000101010101000000020000252C, 1, 0, 0, 0"})
    void shouldDecodeAllIoElementTypesCorrectly(String input,
                                                int expectedOneByteEventNumber,
                                                int expectedTwoByteEventNumber,
                                                int expectedFourByteEventNumber,
                                                int expectedEightByteEventNumber) {
        // Arrange
        buf = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(input));

        // Act
        Map<Short, Object> decodedMap = decoder.decode(buf);

        // Assert
        assertNotNull(decodedMap);

        assertTrue(decodedMap.containsKey(null), "Map should contain reason under null key");

        long actualOneByteCount = decodedMap.values().stream().filter(v -> v instanceof Byte).count() - 1;
        long actualTwoByteCount = decodedMap.values().stream().filter(v -> v instanceof Short).count();
        long actualFourByteCount = decodedMap.values().stream().filter(v -> v instanceof Integer).count();
        long actualEightByteCount = decodedMap.values().stream().filter(v -> v instanceof Long).count();

        assertEquals(expectedOneByteEventNumber, actualOneByteCount, "One byte event number should match");
        assertEquals(expectedTwoByteEventNumber, actualTwoByteCount, "Two byte event number should match");
        assertEquals(expectedFourByteEventNumber, actualFourByteCount, "Four byte event number should match");
        assertEquals(expectedEightByteEventNumber, actualEightByteCount, "Eight byte event number should match");
    }

    @Test
    void shouldDecodeSpecificValuesCorrectly() {
        // Arrange
        // Constructing hex-payload:
        // reason: 1
        // total events: 4
        // 1-byte events (1 шт): ID = 02, Value = 03
        // 2-byte events (1 шт): ID = 04, Value = 0005
        // 4-byte events (1 шт): ID = 06, Value = 00000007
        // 8-byte events (1 шт): ID = 08, Value = 0000000000000009
        String hexPayload = "01" + "04" +
                "01" + "0203" +
                "01" + "040005" +
                "01" + "0600000007" +
                "01" + "080000000000000009";

        buf = Unpooled.wrappedBuffer(ByteBufUtil.decodeHexDump(hexPayload));

        // Act
        Map<Short, Object> decodedMap = decoder.decode(buf);

        // Assert
        assertEquals((byte) 1, decodedMap.get(null), "Reason должен быть 1");

        assertEquals((byte) 3, decodedMap.get((short) 2));
        assertEquals((short) 5, decodedMap.get((short) 4));
        assertEquals(7, decodedMap.get((short) 6));
        assertEquals(9L, decodedMap.get((short) 8));
    }
}