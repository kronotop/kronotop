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
import com.kronotop.server.resp3.BigNumberRedisMessage;
import com.kronotop.server.resp3.BooleanRedisMessage;
import com.kronotop.server.resp3.DoubleRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.FullBulkVerbatimStringRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.NullRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SetRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

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
     * Rewrites a message so that it only holds types the given protocol version understands.
     * RESP3 returns the message as it is. RESP2 walks the whole message tree, including nested
     * containers, and replaces every RESP3 only type with its RESP2 form:
     *
     * <ul>
     *   <li>map becomes a flat array of alternating keys and values, the field order is kept</li>
     *   <li>set becomes an array</li>
     *   <li>boolean becomes the integer 1 or 0</li>
     *   <li>double becomes a bulk string, see {@link #doubleMessage(double, RESPVersion)}</li>
     *   <li>null becomes a bulk string with a length of -1</li>
     *   <li>big number becomes a bulk string</li>
     *   <li>verbatim string becomes a bulk string without the format prefix</li>
     * </ul>
     * <p>
     * Children are moved into the new container without an extra retain, so the caller must not
     * use the original message after the call. A container whose children need no change is
     * returned as it is, without allocating a new one.
     */
    public static RedisMessage downgrade(RedisMessage message, RESPVersion version) {
        if (version == RESPVersion.RESP3) {
            return message;
        }
        return toRESP2(message);
    }

    private static RedisMessage toRESP2(RedisMessage message) {
        // Fast path for the leaf types that dominate large replies. The exact class is compared,
        // so subclasses that do need a rewrite, such as the verbatim string, walk the chain below.
        Class<?> type = message.getClass();
        if (type == FullBulkStringRedisMessage.class
                || type == IntegerRedisMessage.class
                || type == SimpleStringRedisMessage.class) {
            return message;
        }

        switch (message) {
            case MapRedisMessage map -> {
                List<RedisMessage> flattened = new ArrayList<>(map.children().size() * 2);
                map.children().forEach((key, value) -> {
                    flattened.add(toRESP2(key));
                    flattened.add(toRESP2(value));
                });
                return new ArrayRedisMessage(flattened);
            }
            case SetRedisMessage set -> {
                List<RedisMessage> children = new ArrayList<>(set.children().size());
                for (RedisMessage child : set.children()) {
                    children.add(toRESP2(child));
                }
                return new ArrayRedisMessage(children);
            }
            case ArrayRedisMessage array -> {
                return downgradeArray(array);
            }
            case FullBulkVerbatimStringRedisMessage verbatim -> {
                return bulkString(verbatim.realContent());
            }
            case BooleanRedisMessage value -> {
                return value.value() ? RESP2_TRUE : RESP2_FALSE;
            }
            case DoubleRedisMessage value -> {
                return doubleMessage(value.value(), RESPVersion.RESP2);
            }
            case BigNumberRedisMessage value -> {
                return bulkString(value.value());
            }
            case NullRedisMessage _ -> {
                return FullBulkStringRedisMessage.NULL_INSTANCE;
            }
            default -> {
            }
        }
        return message;
    }

    private static RedisMessage downgradeArray(ArrayRedisMessage array) {
        List<RedisMessage> children = array.children();
        List<RedisMessage> converted = null;
        for (int i = 0; i < children.size(); i++) {
            RedisMessage child = children.get(i);
            RedisMessage downgraded = toRESP2(child);
            if (downgraded != child && converted == null) {
                converted = new ArrayList<>(children);
            }
            if (converted != null) {
                converted.set(i, downgraded);
            }
        }
        return converted == null ? array : new ArrayRedisMessage(converted);
    }
}
