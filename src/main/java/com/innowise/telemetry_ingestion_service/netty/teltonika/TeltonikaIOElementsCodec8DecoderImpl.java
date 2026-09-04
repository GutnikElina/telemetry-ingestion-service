package com.innowise.telemetry_ingestion_service.netty.teltonika;

import io.netty.buffer.ByteBuf;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

@Slf4j
@Component
public class TeltonikaIOElementsCodec8DecoderImpl implements TeltonikaIOElementsDecoder {

    @Override
    public Map<Short, Object> decode(ByteBuf in) {
        Map<Short, Object> result = new HashMap<>();

        byte reason = in.readByte();
        result.put(null, reason);

        byte numberOfTotalEvents = in.readByte();

        byte numberOfOneByteEvents = in.readByte();

        for (int i = 0; i < numberOfOneByteEvents; i++) {
            result.put((short) in.readByte(), in.readByte());
        }

        byte numberOfTwoByteEvents = in.readByte();
        for (int i = 0; i < numberOfTwoByteEvents; i++) {
            result.put((short) in.readByte(), in.readShort());
        }

        byte numberOfFourByteEvents = in.readByte();
        for (int i = 0; i < numberOfFourByteEvents; i++) {
            result.put((short) in.readByte(), in.readInt());
        }

        byte numberOfEightByteEvents = in.readByte();
        for (int i = 0; i < numberOfEightByteEvents; i++) {
            result.put((short) in.readByte(), in.readLong());
        }

        if (numberOfTotalEvents != result.size()) {
            log.error("Event count mismatch. Expected {}, but was {}",
                    numberOfTotalEvents, result.size());
        }

        return result;
    }
}
