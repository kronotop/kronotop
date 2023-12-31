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
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.typesafe.config.Config;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class HelloHandlerAuthRequirePassTest extends BaseHandlerTest {
    @Override
    @BeforeEach
    public void setup() throws UnknownHostException, InterruptedException {
        Config config = ConfigTestUtil.load("auth-requirepass-test.conf");
        setupCommon(config);
    }

    @Test
    public void testHELLO_AUTH_success() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        char[] password = {'d', 'e', 'v', 'p', 'a', 's', 's'};
        cmd.hello(2, "default", password, null).encode(buf);
        channel.writeInbound(buf);

        Object msg = channel.readOutbound();
        assertInstanceOf(ArrayRedisMessage.class, msg);
    }

    @Test
    public void testHELLO_AUTH_failure() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        char[] password = {'f'};
        cmd.hello(2, "default", password, null).encode(buf);
        channel.writeInbound(buf);

        Object msg = channel.readOutbound();
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("WRONGPASS invalid username-password pair or user is disabled.", actualMessage.content());
    }
}
