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

package com.kronotop.foundationdb.zmap;

import com.kronotop.BaseHandlerTest;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.protocol.zmap.ZGetRangeSizeArgs;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class ZGetRangeSizeHandlerTest extends BaseHandlerTest {

    @Test
    void test_ZGETRANGESIZE() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // ZSET
        {
            for (int i = 0; i < 10; i++) {
                ByteBuf buf = Unpooled.buffer();
                cmd.zset(String.format("key-%d", i), String.format("value-%d", i)).encode(buf);

                Object response = runCommand(channel, buf);
                assertInstanceOf(SimpleStringRedisMessage.class, response);
                SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
                assertEquals(Response.OK, actualMessage.content());
            }
        }

        // ZGETRANGESIZE
        {
            ByteBuf buf = Unpooled.buffer();
            ZGetRangeSizeArgs args = ZGetRangeSizeArgs.Builder.begin("key-0".getBytes()).end("key-9".getBytes());
            cmd.zgetrangesize(args).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(IntegerRedisMessage.class, response);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
            assertEquals(0, actualMessage.value());
        }
    }
}