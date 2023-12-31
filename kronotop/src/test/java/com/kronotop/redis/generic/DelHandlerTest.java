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

package com.kronotop.redis.generic;


import com.kronotop.redis.BaseHandlerTest;
import com.kronotop.redistest.RedisCommandBuilder;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class DelHandlerTest extends BaseHandlerTest {
    @Test
    public void testDEL_Non_Existing_Keys() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.del("key-1{nodeA}", "key-2{nodeA}").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(IntegerRedisMessage.class, msg);
        IntegerRedisMessage actualMessage = (IntegerRedisMessage) msg;
        assertEquals(0, actualMessage.value());
    }

    @Test
    public void testDEL_Many_Keys() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            for (int i = 0; i < 10; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.set(String.format("key-%d{nodeA}", i), String.format("value-%d", i)).encode(buf);

                channel.writeInbound(buf);
                Object msg = channel.readOutbound();
                assertInstanceOf(SimpleStringRedisMessage.class, msg);
                SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
                assertEquals("OK", actualMessage.content());
            }
        }

        {
            List<String> keys = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                keys.add(String.format("key-%d{nodeA}", i));
            }
            ByteBuf buf = Unpooled.buffer();
            cmd.del(keys).encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(IntegerRedisMessage.class, msg);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) msg;
            assertEquals(10, actualMessage.value());
        }
    }
}