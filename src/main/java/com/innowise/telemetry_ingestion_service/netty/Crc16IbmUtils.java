package com.innowise.telemetry_ingestion_service.netty;

import io.netty.buffer.ByteBuf;

public class Crc16IbmUtils {
    /**
     * Calculates CRC-16 for a Teltonika packet.
     *
     * @param buf data buffer (Off-Heap)
     * @param startIndex index where the payload begins
     * @param length payload length (Data Field Length)
     * @return calculated checksum
     */
    public static int calculate(ByteBuf buf, int startIndex, int length) {
        int crc = 0x0000;

        for (int i = 0; i < length; i++) {
            crc ^= (buf.getByte(startIndex + i) & 0xFF);

            for (int j = 0; j < 8; j++) {
                if ((crc & 1) != 0) {
                    crc = (crc >> 1) ^ 0xA001;
                } else {
                    crc >>= 1;
                }
            }
        }
        return crc;
    }
}

