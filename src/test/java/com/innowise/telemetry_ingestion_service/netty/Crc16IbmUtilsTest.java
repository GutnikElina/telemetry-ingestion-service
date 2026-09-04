package com.innowise.telemetry_ingestion_service.netty;

import com.innowise.telemetry_ingestion_service.netty.teltonika.Crc16IbmUtils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Crc16IbmUtilsTest {

    @ParameterizedTest
    @CsvSource({
            "08010000016B40D8EA30010000000000000000000000000000000105021503010101425E0F01F10000601A014E000000000000000001, 51151",
            "08010000016B40D9AD80010000000000000000000000000000000103021503010101425E10000001, 61994",
            "08020000016B40D57B480100000000000000000000000000000001010101000000000000016B40D5C19801000000000000000000000000000000010101010100000002, 9516"
    })
    void calculate(String bytes, int expectedResult) {
        byte[] realBytes = ByteBufUtil.decodeHexDump(bytes);
        ByteBuf byteBuf = Unpooled.wrappedBuffer(realBytes);

        int calculate = Crc16IbmUtils.calculate(byteBuf, 0, realBytes.length);

        assertEquals(expectedResult, calculate);
    }
}