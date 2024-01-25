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

package com.kronotop.sql;

import com.kronotop.ConfigTestUtil;
import com.kronotop.KronotopTestInstance;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.typesafe.config.Config;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;

import java.net.UnknownHostException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class BaseHandlerTest {
    protected KronotopTestInstance kronotopInstance;
    protected EmbeddedChannel channel;

    protected EmbeddedChannel newChannel() {
        return kronotopInstance.newChannel();
    }

    protected void setupCommon(Config config) throws UnknownHostException, InterruptedException {
        kronotopInstance = new KronotopTestInstance(config);
        kronotopInstance.start();
        channel = kronotopInstance.getChannel();
    }

    @BeforeEach
    public void setup() throws UnknownHostException, InterruptedException {
        Config config = ConfigTestUtil.load("test.conf");
        setupCommon(config);
    }

    @AfterEach
    public void tearDown() {
        if (kronotopInstance == null) {
            return;
        }
        kronotopInstance.shutdown();
    }

    protected void executeSqlQueryReturnsOK(KronotopCommandBuilder<String, String> cmd, String query) {
        ByteBuf buf = Unpooled.buffer();
        cmd.sql(query).encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        if (response instanceof ErrorRedisMessage) {
            System.out.println(((ErrorRedisMessage) response).content());
        }
        assertInstanceOf(SimpleStringRedisMessage.class, response);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
        assertEquals(Response.OK, actualMessage.content());
    }

    protected ErrorRedisMessage executeSqlQueryReturnsError(KronotopCommandBuilder<String, String> cmd, String query) {
        ByteBuf buf = Unpooled.buffer();
        cmd.sql(query).encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        assertInstanceOf(ErrorRedisMessage.class, response);
        return (ErrorRedisMessage) response;
    }
}
