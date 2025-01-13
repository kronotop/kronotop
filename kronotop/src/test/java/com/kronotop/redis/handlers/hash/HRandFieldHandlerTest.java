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
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class HRandFieldHandlerTest extends BaseHandlerTest {

    @Test
    public void test_HRANDFIELD() {
        HashMap<String, String> map = new HashMap<>();
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();

            for (int i = 0; i < 10; i++) {
                map.put(String.format("field-%d", i), String.format("value-%d", i));
            }
            cmd.hmset("mykey", map).encode(buf);
            channel.writeInbound(buf);
            channel.readOutbound();
        }

        ByteBuf buf = Unpooled.buffer();
        cmd.hrandfield("mykey").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();

        assertInstanceOf(FullBulkStringRedisMessage.class, msg);
        FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) msg;
        assertTrue(map.containsKey(actualMessage.content().toString(StandardCharsets.UTF_8)));
    }
}
