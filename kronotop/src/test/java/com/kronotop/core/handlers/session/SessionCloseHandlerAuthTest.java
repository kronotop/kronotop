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

package com.kronotop.core.handlers.session;

import com.kronotop.BaseHandlerTest;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.commands.redis.RedisCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class SessionCloseHandlerAuthTest extends BaseHandlerTest {

    @Override
    protected String getConfigFileName() {
        return "auth-requirepass-test.conf";
    }

    private void authenticate() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.auth("devpass").encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    private Object ping() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.ping().encode(buf);
        return runCommand(channel, buf);
    }

    private void closeSession() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.sessionClose().encode(buf);
        Object response = runCommand(channel, buf);
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
    }

    @Test
    void shouldRequireAuthenticationAfterClose() {
        // Behavior: SESSION.CLOSE clears the authentication flag when auth.requirepass is set.
        authenticate();
        assertInstanceOf(SimpleStringRedisMessage.class, ping());

        closeSession();

        Object response = ping();
        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals("NOAUTH Authentication required.", ((ErrorRedisMessage) response).content());
    }

    @Test
    void shouldAuthenticateAgainAfterClose() {
        // Behavior: the same connection is usable again after it authenticates following SESSION.CLOSE.
        authenticate();
        closeSession();

        authenticate();
        assertInstanceOf(SimpleStringRedisMessage.class, ping());
    }
}
