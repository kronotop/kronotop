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

package com.kronotop.core.handlers.connection;

import com.kronotop.BaseHandlerTest;
import com.kronotop.commands.redis.RedisCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class HelloHandlerAuthRequirePassTest extends BaseHandlerTest {

    @Override
    protected String getConfigFileName() {
        return "auth-requirepass-test.conf";
    }

    @Test
    void shouldAuthenticateDefaultUserWithCorrectPassword() {
        // Behavior: with requirepass set, HELLO AUTH default <password> succeeds and returns the server info reply.
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        char[] password = {'d', 'e', 'v', 'p', 'a', 's', 's'};
        cmd.hello(2, "default", password, null).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
    }

    @Test
    void shouldRejectDefaultUserWithWrongPassword() {
        // Behavior: with requirepass set, HELLO AUTH default <wrong password> is rejected with the WRONGPASS error.
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        char[] password = {'f'};
        cmd.hello(2, "default", password, null).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("WRONGPASS invalid username-password pair or user is disabled.", actualMessage.content());
    }

    @Test
    void shouldRejectHelloWithoutAuthOptionWhenNotAuthenticated() {
        // Behavior: with requirepass set, HELLO 3 without the AUTH option on a connection that has not
        // authenticated is rejected with the NOAUTH error of HELLO, and the connection stays on RESP2.
        Object msg = runRaw(channel, CommandType.HELLO, List.of("3"));

        assertInstanceOf(ErrorRedisMessage.class, msg);
        assertEquals(
                "NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> " +
                        "AUTH <user> <pass> option can be used to authenticate the client and select the RESP " +
                        "protocol version at the same time",
                ((ErrorRedisMessage) msg).content()
        );
        assertEquals(RESPVersion.RESP2, channel.attr(SessionAttributes.SESSION).get().getProtocolVersion());
    }
}
