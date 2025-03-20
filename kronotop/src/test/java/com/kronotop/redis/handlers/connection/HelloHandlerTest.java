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

package com.kronotop.redis.handlers.connection;

import com.kronotop.commandbuilder.redis.RedisCommandBuilder;
import com.kronotop.network.clients.Client;
import com.kronotop.network.clients.Clients;
import com.kronotop.redis.handlers.BaseRedisHandlerTest;
import com.kronotop.redis.handlers.connection.protocol.HelloMessage;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Attribute;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class HelloHandlerTest extends BaseRedisHandlerTest {

    @Test
    public void testHELLO_protover_2() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.hello(2, null, null, "foobar").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage response = (ArrayRedisMessage) msg;
        for (int index = 0; index < response.children().size(); index++) {
            SimpleStringRedisMessage keyMessage;
            if (index % 2 == 0) {
                RedisMessage redisMessage = response.children().get(index);
                keyMessage = (SimpleStringRedisMessage) redisMessage;
                int valueIndex = index + 1;
                switch (keyMessage.content()) {
                    case "server":
                        SimpleStringRedisMessage serverName = (SimpleStringRedisMessage) response.children().get(valueIndex);
                        assertEquals("kronotop", serverName.content());
                        continue;
                    case "version":
                        SimpleStringRedisMessage serverVersion = (SimpleStringRedisMessage) response.children().get(valueIndex);
                        assertNotNull(serverVersion);
                        continue;
                    case "proto":
                        IntegerRedisMessage protover = (IntegerRedisMessage) response.children().get(valueIndex);
                        assertEquals(Long.valueOf(HelloMessage.RESP_VERSION_TWO), protover.value());
                        continue;
                    case "id":
                        Attribute<Long> expectedClientId = channel.attr(SessionAttributes.CLIENT_ID);
                        IntegerRedisMessage clientId = (IntegerRedisMessage) response.children().get(valueIndex);
                        assertEquals(expectedClientId.get(), Long.valueOf(clientId.value()));
                        continue;
                    case "mode":
                        SimpleStringRedisMessage mode = (SimpleStringRedisMessage) response.children().get(valueIndex);
                        assertEquals("cluster", mode.content());
                        continue;
                    case "role":
                        SimpleStringRedisMessage role = (SimpleStringRedisMessage) response.children().get(valueIndex);
                        assertEquals("master", role.content());
                        continue;
                    case "modules":
                        ArrayRedisMessage modules = (ArrayRedisMessage) response.children().get(valueIndex);
                        assertEquals(0, modules.children().size());
                }
            }
        }
    }

    @Test
    public void testHELLO_protover_3() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.hello(3, null, null, "foobar").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage response = (MapRedisMessage) msg;
        for (RedisMessage redisMessage : response.children().keySet()) {
            SimpleStringRedisMessage key = (SimpleStringRedisMessage) redisMessage;
            switch (key.content()) {
                case "server":
                    SimpleStringRedisMessage serverName = (SimpleStringRedisMessage) response.children().get(redisMessage);
                    assertEquals("kronotop", serverName.content());
                    continue;
                case "version":
                    SimpleStringRedisMessage serverVersion = (SimpleStringRedisMessage) response.children().get(redisMessage);
                    assertNotNull(serverVersion);
                    continue;
                case "proto":
                    IntegerRedisMessage protover = (IntegerRedisMessage) response.children().get(redisMessage);
                    assertEquals(Long.valueOf(HelloMessage.RESP_VERSION_THREE), protover.value());
                    continue;
                case "id":
                    Attribute<Long> expectedClientId = channel.attr(SessionAttributes.CLIENT_ID);
                    IntegerRedisMessage clientId = (IntegerRedisMessage) response.children().get(redisMessage);
                    assertEquals(expectedClientId.get(), Long.valueOf(clientId.value()));
                    continue;
                case "mode":
                    SimpleStringRedisMessage mode = (SimpleStringRedisMessage) response.children().get(redisMessage);
                    assertEquals("cluster", mode.content());
                    continue;
                case "role":
                    SimpleStringRedisMessage role = (SimpleStringRedisMessage) response.children().get(redisMessage);
                    assertEquals("master", role.content());
                    continue;
                case "modules":
                    ArrayRedisMessage modules = (ArrayRedisMessage) response.children().get(redisMessage);
                    assertEquals(0, modules.children().size());
            }
        }
    }

    @Test
    public void testHELLO_NOPROTO() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.hello(4, null, null, null).encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("NOPROTO unsupported protocol version", actualMessage.content());
    }

    @Test
    public void testHELLO_SETNAME() {
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.hello(2, null, null, "test").encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readOutbound();
        assertInstanceOf(ArrayRedisMessage.class, msg);

        Attribute<Long> clientID = channel.attr(SessionAttributes.CLIENT_ID);
        Client client = Clients.getClient(clientID.get());
        assertNotNull(client);
        assertEquals("test", client.getName());
    }
}
