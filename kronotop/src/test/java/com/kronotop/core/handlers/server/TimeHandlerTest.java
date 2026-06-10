/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.core.handlers.server;

import com.kronotop.BaseHandlerTest;
import com.kronotop.commands.redis.RedisCommandBuilder;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class TimeHandlerTest extends BaseHandlerTest {

    private ArrayRedisMessage runTime(EmbeddedChannel channel) {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.time().encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, response);
        return (ArrayRedisMessage) response;
    }

    private long parseLong(Object message) {
        assertInstanceOf(FullBulkStringRedisMessage.class, message);
        FullBulkStringRedisMessage bulkString = (FullBulkStringRedisMessage) message;
        return Long.parseLong(bulkString.content().toString(StandardCharsets.US_ASCII));
    }

    @Test
    void shouldReturnSecondsAndMicroseconds() {
        // Behavior: TIME returns an array of two bulk strings, the Unix timestamp in seconds
        // and the microseconds elapsed in the current second
        EmbeddedChannel channel = getChannel();

        long before = System.currentTimeMillis() / 1000;
        ArrayRedisMessage response = runTime(channel);
        long after = System.currentTimeMillis() / 1000;

        assertEquals(2, response.children().size());

        long seconds = parseLong(response.children().get(0));
        assertTrue(seconds >= before);
        assertTrue(seconds <= after);

        long microseconds = parseLong(response.children().get(1));
        assertTrue(microseconds >= 0);
        assertTrue(microseconds <= 999_999);
    }

    @Test
    void shouldNotRequireActiveTransaction() {
        // Behavior: TIME succeeds on a connection without an active transaction
        EmbeddedChannel channel = getChannel();

        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*1\r\n$4\r\nTIME\r\n".getBytes(StandardCharsets.US_ASCII));

        Object response = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) response;
        assertEquals(2, actualMessage.children().size());
    }

    @Test
    void shouldRejectExtraParameter() {
        // Behavior: TIME with a parameter returns a wrong number of arguments error
        EmbeddedChannel channel = getChannel();

        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$4\r\nTIME\r\n$1\r\nx\r\n".getBytes(StandardCharsets.US_ASCII));

        Object response = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
        assertTrue(actualMessage.content().contains("wrong number of arguments"));
    }
}
