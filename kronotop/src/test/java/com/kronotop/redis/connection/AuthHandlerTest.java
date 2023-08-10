/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.redis.connection;

import com.kronotop.ConfigTestUtil;
import com.kronotop.redis.BaseHandlerTest;
import com.kronotop.redistest.RedisCommandBuilder;
import com.typesafe.config.Config;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.redis.ErrorRedisMessage;
import io.netty.handler.codec.redis.SimpleStringRedisMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class AuthHandlerTest extends BaseHandlerTest {
    @Override
    @BeforeEach
    public void setup() throws UnknownHostException, InterruptedException {
        Config config = ConfigTestUtil.load("auth-test.conf");
        setupCommon(config);
    }

    @Test
    public void testNOAUTH() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.ping().encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("NOAUTH Authentication required.", actualMessage.content());
    }

    @Test
    public void testAuthHandlerSuccess() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.auth("devuser", "devpass").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals("OK", actualMessage.content());
    }

    @Test
    public void testAuthHandlerSuccessWrongCredentials() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.auth("foobar", "barfoo").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("WRONGPASS invalid username-password pair or user is disabled.", actualMessage.content());
    }

    @Test
    public void testAuth() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.auth("devuser", "devpass").encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.ping().encode(buf);

            channel.writeInbound(buf);
            Object msg = channel.readOutbound();
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals("PONG", actualMessage.content());
        }
    }

    @Test
    public void testAuthOnlyWithPass() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.auth("devpass").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(SimpleStringRedisMessage.class, msg);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
        assertEquals("OK", actualMessage.content());
    }
}
