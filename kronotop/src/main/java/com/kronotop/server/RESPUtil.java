/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.server;

import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.BooleanRedisMessage;
import com.kronotop.server.resp3.DoubleRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.NullRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utility methods for creating binary-safe RESP bulk string messages.
 */
public class RESPUtil {
    private static final IntegerRedisMessage RESP2_TRUE = new IntegerRedisMessage(1);
    private static final IntegerRedisMessage RESP2_FALSE = new IntegerRedisMessage(0);

    /**
     * Wraps a raw byte array into a {@link FullBulkStringRedisMessage} without copying.
     */
    public static FullBulkStringRedisMessage wrapBytes(byte[] bytes) {
        return new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(bytes));
    }

    /**
     * Creates a {@link FullBulkStringRedisMessage} from a UTF-8 encoded string.
     */
    public static FullBulkStringRedisMessage bulkString(String input) {
        String value = input != null ? input : "";
        return wrapBytes(value.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Returns the null message for the given protocol version. RESP3 has its own null type,
     * RESP2 encodes null as a bulk string with a length of -1.
     */
    public static RedisMessage nullMessage(RESPVersion version) {
        return version == RESPVersion.RESP3
                ? NullRedisMessage.INSTANCE
                : FullBulkStringRedisMessage.NULL_INSTANCE;
    }

    /**
     * Returns the double message for the given protocol version. RESP3 has its own double type,
     * RESP2 has no double type, so the value is sent as a bulk string that holds the 8 byte
     * IEEE 754 big-endian form of the value.
     */
    public static RedisMessage doubleMessage(double value, RESPVersion version) {
        if (version == RESPVersion.RESP3) {
            return new DoubleRedisMessage(value);
        }
        ByteBuf data = Unpooled.wrappedBuffer(ByteBuffer.allocate(8).putDouble(value).array());
        return new FullBulkStringRedisMessage(data);
    }

    /**
     * Returns the boolean message for the given protocol version. RESP3 has its own boolean type,
     * RESP2 has no boolean type, so the value is sent as the integer 1 or 0.
     */
    public static RedisMessage booleanMessage(boolean value, RESPVersion version) {
        if (version == RESPVersion.RESP3) {
            return value ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE;
        }
        return value ? RESP2_TRUE : RESP2_FALSE;
    }

    /**
     * Returns the map message for the given protocol version. RESP3 has its own map type,
     * RESP2 has no map type, so the fields are sent as a flat array of alternating keys and
     * values. The field order is preserved in both cases.
     */
    public static RedisMessage formatMapMessage(Map<RedisMessage, RedisMessage> fields, RESPVersion version) {
        if (version == RESPVersion.RESP3) {
            return new MapRedisMessage(fields);
        }

        // RESP2 compatibility mode
        List<RedisMessage> flattened = new ArrayList<>(fields.size() * 2);
        fields.forEach((key, value) -> {
            flattened.add(key);
            flattened.add(value);
        });
        return new ArrayRedisMessage(flattened);
    }
}
