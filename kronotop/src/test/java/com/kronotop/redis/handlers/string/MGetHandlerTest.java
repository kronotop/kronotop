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

package com.kronotop.redis.handlers.string;


import com.kronotop.commandbuilder.redis.RedisCommandBuilder;
import com.kronotop.redis.handlers.BaseRedisHandlerTest;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class MGetHandlerTest extends BaseRedisHandlerTest {
    public List<String> getKeys() {
        List<String> keys = new ArrayList<>();
        keys.add("mykey-1{nodeA}");
        keys.add("mykey-2{nodeA}");
        keys.add("mykey-3{nodeA}");
        return keys;
    }

    public Map<String, String> getPairs() {
        HashMap<String, String> pairs = new HashMap<>();
        pairs.put("mykey-1{nodeA}", "myvalue-1");
        pairs.put("mykey-2{nodeA}", "myvalue-2");
        pairs.put("mykey-3{nodeA}", "myvalue-3");
        return pairs;
    }

    @Test
    public void testMGET() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.mget(getKeys()).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(3, actualMessage.children().size());

        for (RedisMessage redisMessage : actualMessage.children()) {
            assertInstanceOf(FullBulkStringRedisMessage.class, redisMessage);
            FullBulkStringRedisMessage item = (FullBulkStringRedisMessage) redisMessage;
            assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, item);
        }
    }

    @Test
    public void testMSET_MGET() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        Map<String, String> pairs = getPairs();
        List<String> keys = getKeys();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.mset(pairs).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.mget(keys).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);
            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(3, actualMessage.children().size());

            int index = 0;
            for (RedisMessage redisMessage : actualMessage.children()) {
                assertInstanceOf(FullBulkStringRedisMessage.class, redisMessage);
                FullBulkStringRedisMessage item = (FullBulkStringRedisMessage) redisMessage;

                String key = keys.get(index);
                String value = pairs.get(key);
                assertEquals(value, item.content().toString(CharsetUtil.US_ASCII));
                index++;
            }
        }
    }
}