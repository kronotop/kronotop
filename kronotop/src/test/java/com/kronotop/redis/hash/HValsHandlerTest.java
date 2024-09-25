/*
 * Copyright (c) 2023-2024 Kronotop
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
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class HValsHandlerTest extends BaseHandlerTest {

    @Test
    public void test_HVALS() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        HashMap<String, String> map = new HashMap<>();
        {
            ByteBuf buf = Unpooled.buffer();

            for (int i = 0; i < 10; i++) {
                map.put(String.format("key-%d", i), String.format("value-%d", i));
            }
            cmd.hmset("mykey", map).encode(buf);

            channel.writeInbound(buf);
            channel.readOutbound();
        }
        ByteBuf buf = Unpooled.buffer();
        cmd.hvals("mykey").encode(buf);
        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(10, actualMessage.children().size());

        List<String> expected = new ArrayList<>();
        for (RedisMessage redisMessage : actualMessage.children()) {
            FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) redisMessage;
            expected.add(value.content().toString(CharsetUtil.US_ASCII));
        }
        List<String> list = new ArrayList<>(map.values());
        assertEquals(expected.stream().sorted().toList(), list.stream().sorted().toList());
    }
}
