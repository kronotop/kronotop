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
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.commands.ZMapCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
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

class ZSetHandlerTest extends BaseHandlerTest {

    static Stream<Arguments> invalidArguments() {
        return Stream.of(
                arguments("too few arguments",
                        List.of("key"),
                        "ERR wrong number of arguments for 'ZSET' command"),
                arguments("too many arguments",
                        List.of("key", "value", "NAMESPACE", "ns", "EXTRA"),
                        "ERR wrong number of arguments for 'ZSET' command"),
                arguments("unknown keyword",
                        List.of("key", "value", "EXTRA"),
                        "ERR Unknown 'EXTRA' argument"),
                arguments("NAMESPACE without value",
                        List.of("key", "value", "NAMESPACE"),
                        "ERR NAMESPACE argument must be followed by a namespace")
        );
    }

    @Test
    void shouldSetKeyValue() {
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        ByteBuf buf = Unpooled.buffer();
        cmd.zset("my-key", "my-value").encode(buf);

        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void shouldSetKeyInGivenNamespace() {
        // Behavior: NAMESPACE writes the key into the given namespace and leaves the session's
        // current namespace unchanged.
        EmbeddedChannel channel = getChannel();
        KronotopCommandBuilder<String, String> nsCmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            nsCmd.namespaceCreate(namespace).encode(buf);
            assertOK(runCommand(channel, buf));
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zset("my-key", "my-value", namespace).encode(buf);
            assertOK(runCommand(channel, buf));
        }
        {
            // The session still points at the default namespace, where the key does not exist.
            ByteBuf buf = Unpooled.buffer();
            cmd.zget("my-key").encode(buf);
            assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, runCommand(channel, buf));
        }
        {
            ByteBuf buf = Unpooled.buffer();
            nsCmd.namespaceUse(namespace).encode(buf);
            assertOK(runCommand(channel, buf));
        }
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.zget("my-key").encode(buf);
            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            assertEquals("my-value", ((FullBulkStringRedisMessage) response).content().toString(CharsetUtil.US_ASCII));
        }
    }

    @Test
    void shouldRejectSetWhenNamespaceDoesNotExist() {
        // Behavior: NAMESPACE with an unknown namespace returns NOSUCHNAMESPACE.
        ZMapCommandBuilder<String, String> cmd = new ZMapCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.zset("my-key", "my-value", namespace).encode(buf);
        Object response = runCommand(getChannel(), buf);
        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals(String.format("NOSUCHNAMESPACE No such namespace: '%s'", namespace),
                ((ErrorRedisMessage) response).content());
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("invalidArguments")
    void shouldRejectInvalidArguments(String name, List<String> rawArgs, String expectedError) {
        // Behavior: ZSET rejects a wrong argument count, an unknown keyword or a keyword without
        // its value with an exact ERR reply.
        Object response = runRaw(getChannel(), CommandType.ZSET, rawArgs);

        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals(expectedError, ((ErrorRedisMessage) response).content());
    }
}
