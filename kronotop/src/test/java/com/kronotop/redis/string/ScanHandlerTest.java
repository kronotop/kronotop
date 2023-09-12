/*
 * Copyright (c) 2023 Kronotop
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.redis.string;

import com.kronotop.redis.BaseHandlerTest;
import com.kronotop.redistest.RedisCommandBuilder;
import io.lettuce.core.ScanCursor;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.ArrayRedisMessage;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.RedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class ScanHandlerTest extends BaseHandlerTest {
    @Test
    public void testSCAN() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);

        SortedSet<String> expectedResult = new TreeSet<>();
        for (int i = 0; i < 10; i++) {
            ByteBuf buf = Unpooled.buffer();
            String key = String.format("key-{%d}", i);
            expectedResult.add(key);
            cmd.set(key, String.format("value-{%d}", i)).encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("OK", actualMessage.content());
        }

        SortedSet<String> result = new TreeSet<>();
        ScanCursor cursor = new ScanCursor();
        cursor.setCursor("0");

        while (true) {
            ByteBuf buf = Unpooled.buffer();
            cmd.scan(cursor).encode(buf);
            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(ArrayRedisMessage.class, msg);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

            FullBulkStringRedisMessage rawCursor = (FullBulkStringRedisMessage) actualMessage.children().get(0);
            String nextCursor = rawCursor.content().toString(CharsetUtil.US_ASCII);
            cursor.setCursor(nextCursor);

            ArrayRedisMessage rawKeys = (ArrayRedisMessage) actualMessage.children().get(1);
            for (RedisMessage rawKey : rawKeys.children()) {
                FullBulkStringRedisMessage key = (FullBulkStringRedisMessage)rawKey;
                result.add(key.content().toString(CharsetUtil.US_ASCII));
            }

            if (nextCursor.equals("0")) {
                // No more data
                break;
            }
        }

        assertEquals(expectedResult, result);
    }
}
