/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.redis.handlers.hash;

import com.kronotop.commandbuilder.redis.RedisCommandBuilder;
import com.kronotop.redis.handlers.BaseHandlerTest;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class HGetAllHandlerTest extends BaseHandlerTest {

    @Test
    public void test_HGETALL() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        HashMap<String, String> map = new HashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();

            for (int i = 0; i < 10; i++) {
                map.put(String.format("key-%d", i), String.format("value-%d", i));
            }
            cmd.hset("mykey", map).encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(IntegerRedisMessage.class, msg);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) msg;
            assertEquals(10L, actualMessage.value());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.hgetall("mykey").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(ArrayRedisMessage.class, msg);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(20, actualMessage.children().size());

            for (int i = 0; i < actualMessage.children().size(); i = i + 2) {
                RedisMessage keyMessage = actualMessage.children().get(i);
                assertInstanceOf(FullBulkStringRedisMessage.class, keyMessage);
                FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) keyMessage;

                RedisMessage valueMessage = actualMessage.children().get(i + 1);
                assertInstanceOf(FullBulkStringRedisMessage.class, valueMessage);
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) valueMessage;
                String expectedValue = map.get(key.content().toString(CharsetUtil.US_ASCII));
                assertEquals(expectedValue, value.content().toString(CharsetUtil.US_ASCII));
            }
        }
    }

    @Test
    public void test_HGETALL_KeyNotExists() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.hgetall("mykey").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(0, actualMessage.children().size());
    }
}
