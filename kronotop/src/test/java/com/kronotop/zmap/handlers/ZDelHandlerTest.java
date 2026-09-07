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

package com.kronotop.zmap.handlers;

import com.kronotop.BaseHandlerTest;
import com.kronotop.commands.CommandType;
import com.kronotop.commands.ZMapCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ZDelHandlerTest extends BaseHandlerTest {

    @Test
    void shouldDeleteKey() {
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        // ZSET
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zset("key", "value").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // ZDEL
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zdel("key").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        // ZGET
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zget("key").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
            assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, actualMessage);
        }
    }


    @Test
    void shouldSucceedWhenDeletingNonExistentKey() {
        EmbeddedChannel channel = getChannel();
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);

        // ZDEL
        ByteBuf buf = Unpooled.buffer();
        cmd.zdel("key").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
        assertEquals(Response.OK, actualMessage.content());
    }

    static Stream<Arguments> invalidArguments() {
        return Stream.of(
                arguments("too few arguments",
                        List.of(),
                        "ERR wrong number of arguments for 'ZDEL' command"),
                arguments("too many arguments",
                        List.of("key", "EXTRA"),
                        "ERR wrong number of arguments for 'ZDEL' command")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidArguments")
    void shouldRejectInvalidArguments(String name, List<String> rawArgs, String expectedError) {
        // Behavior: ZDEL rejects a wrong argument count with an ERR reply.
        Object response = runRaw(getChannel(), CommandType.ZDEL, rawArgs);

        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals(expectedError, ((ErrorRedisMessage) response).content());
    }
}
