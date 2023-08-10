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

package com.kronotop.redis.hash;

import com.kronotop.redis.BaseHandlerTest;
import com.kronotop.redistest.RedisCommandBuilder;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class HGetHandlerTest extends BaseHandlerTest {
    @Test
    public void testHGET() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.hset("mykey", "field", "value").encode(buf);
            channel.writeInbound(buf);
            channel.readOutbound();
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.hget("mykey", "field").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(FullBulkStringRedisMessage.class, msg);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) msg;
            assertEquals("value", actualMessage.content().toString(CharsetUtil.US_ASCII));
        }
    }

    @Test
    public void testHGET_KeyNotExists() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.hget("mykey", "field").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(FullBulkStringRedisMessage.class, msg);
        FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) msg;
        assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, actualMessage);
    }

    @Test
    public void testHGET_FieldNotExists() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.hset("mykey", "field", "value").encode(buf);
            channel.writeInbound(buf);
            channel.readOutbound();
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.hget("mykey", "not-exists").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(FullBulkStringRedisMessage.class, msg);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) msg;
            assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, actualMessage);
        }
    }
}
