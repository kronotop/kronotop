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
import com.kronotop.commands.ZGetKeyArgs;
import com.kronotop.commands.ZMapCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.output.ArrayOutput;
import io.lettuce.core.protocol.Command;
import io.lettuce.core.protocol.CommandArgs;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import io.netty.util.CharsetUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class ZGetKeyHandlerTest extends BaseHandlerTest {
    @Test
    void shouldGetKey() {
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
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

        // ZGETKEY <namespace> key-0
        {
            ByteBuf buf = Unpooled.buffer();
            ZGetKeyArgs args = ZGetKeyArgs.Builder.key("key-0".getBytes());
            cmd.zgetkey(args).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
            assertEquals("key-0", actualMessage.content().toString(CharsetUtil.US_ASCII));
        }

        // ZGETKEY <namespace> key-0 key_selector first_greater_than
        {
            ByteBuf buf = Unpooled.buffer();
            ZGetKeyArgs args = ZGetKeyArgs.Builder.
                    key("key-0".getBytes()).
                    keySelector("first_greater_than");
            cmd.zgetkey(args).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage actualMessage = (FullBulkStringRedisMessage) response;
            assertEquals("key-1", actualMessage.content().toString(CharsetUtil.US_ASCII));
        }
    }

    private Object runRaw(EmbeddedChannel channel, List<String> rawArgs) {
        CommandArgs<String, String> args = new CommandArgs<>(StringCodec.ASCII);
        rawArgs.forEach(args::add);
        Command<String, String, List<Object>> rawCmd =
                new Command<>(CommandType.ZGETKEY, new ArrayOutput<>(StringCodec.ASCII), args);

        ByteBuf buf = Unpooled.buffer();
        rawCmd.encode(buf);
        return runCommand(channel, buf);
    }

    static Stream<Arguments> invalidArguments() {
        return Stream.of(
                arguments("unknown keyword",
                        List.of("key-0", "SELECTOR", "first_greater_than"),
                        "ERR Unknown 'SELECTOR' argument"),
                arguments("key selector without value",
                        List.of("key-0", "KEY-SELECTOR"),
                        "ERR KEY-SELECTOR argument must be followed by a valid key selector"),
                arguments("invalid key selector",
                        List.of("key-0", "KEY-SELECTOR", "bogus"),
                        "ERR Unknown key selector: 'bogus'"),
                arguments("repeated key selector exceeds the argument limit",
                        List.of("key-0",
                                "KEY-SELECTOR", "first_greater_than",
                                "KEY-SELECTOR", "first_greater_or_equal"),
                        "ERR wrong number of arguments for 'ZGETKEY' command")
        );
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidArguments")
    void shouldRejectInvalidArguments(String name, List<String> rawArgs, String expectedError) {
        // Behavior: ZGETKEY rejects malformed keyword arguments with an ERR reply instead of
        // failing with an unhandled error.
        Object response = runRaw(getChannel(), rawArgs);

        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals(expectedError, ((ErrorRedisMessage) response).content());
    }
}
