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
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.FullBulkStringRedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class MSetHandlerTest extends BaseHandlerTest {
    public Map<String, String> getPairs() {
        HashMap<String, String> pairs = new HashMap<>();
        pairs.put("mykey-1{nodeA}", "myvalue-1");
        pairs.put("mykey-2{nodeA}", "myvalue-2");
        pairs.put("mykey-3{nodeA}", "myvalue-3");
        return pairs;
    }

    @Test
    public void testMSET() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.mset(getPairs()).encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals("OK", actualMessage.content());
    }

    @Test
    public void testMSET_GET() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        Map<String, String> pairs = getPairs();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.mset(getPairs()).encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("OK", actualMessage.content());
        }

        {
            for (String key : pairs.keySet()) {
                ByteBuf buf = Unpooled.buffer();
                cmd.get(key).encode(buf);

                channel.writeInbound(buf);
                Object msg = channel.readOutbound();
                assertInstanceOf(FullBulkStringRedisMessage.class, msg);
                FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) msg;
                assertEquals(pairs.get(key), actualMessage.content().toString(CharsetUtil.US_ASCII));
            }
        }
    }
}