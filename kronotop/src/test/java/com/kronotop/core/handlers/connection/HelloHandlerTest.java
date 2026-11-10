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
import com.kronotop.instance.KronotopInstance;
import com.kronotop.network.clients.Client;
import com.kronotop.network.clients.Clients;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.StringCodec;
import io.lettuce.core.protocol.CommandType;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.Attribute;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class HelloHandlerTest extends BaseHandlerTest {

    @Test
    void shouldReplyWithFlatArrayWhenProtocolVersionIs2() {
        // Behavior: HELLO 2 keeps the connection on RESP2, so the reply is a flat array of fields and values.
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.hello(2, null, null, "foobar").encode(buf);

        Object msg = runCommand(channel, buf);
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
                        assertEquals(Long.valueOf(RESPVersion.RESP2.getValue()), protover.value());
                        continue;
                    case "id":
                        Attribute<Long> expectedClientId = channel.attr(SessionAttributes.CLIENT_ID);
                        IntegerRedisMessage clientId = (IntegerRedisMessage) response.children().get(valueIndex);
                        assertEquals(expectedClientId.get(), Long.valueOf(clientId.value()));
                        continue;
                    case "mode":
                        SimpleStringRedisMessage mode = (SimpleStringRedisMessage) response.children().get(valueIndex);
                        assertEquals("standalone", mode.content());
                        continue;
                    case "role":
                        SimpleStringRedisMessage role = (SimpleStringRedisMessage) response.children().get(valueIndex);
                        assertEquals("master", role.content());
                        continue;
                }
            }
        }
    }

    @Test
    void shouldReplyWithMapWhenProtocolVersionIs3() {
        // Behavior: HELLO 3 switches the connection to RESP3 before the reply is written, so the reply is a map.
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.hello(3, null, null, "foobar").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);
        MapRedisMessage response = (MapRedisMessage) msg;
        for (RedisMessage redisMessage : response.children().keySet()) {
            FullBulkStringRedisMessage key = (FullBulkStringRedisMessage) redisMessage;
            switch (key.content().toString()) {
                case "server":
                    FullBulkStringRedisMessage serverName = (FullBulkStringRedisMessage) response.children().get(redisMessage);
                    assertEquals(KronotopInstance.PRODUCT_NAME, serverName.content().toString());
                    continue;
                case "version":
                    FullBulkStringRedisMessage serverVersion = (FullBulkStringRedisMessage) response.children().get(redisMessage);
                    assertNotNull(serverVersion);
                    continue;
                case "proto":
                    IntegerRedisMessage protover = (IntegerRedisMessage) response.children().get(redisMessage);
                    assertEquals(Long.valueOf(RESPVersion.RESP3.getValue()), protover.value());
                    continue;
                case "id":
                    Attribute<Long> expectedClientId = channel.attr(SessionAttributes.CLIENT_ID);
                    IntegerRedisMessage clientId = (IntegerRedisMessage) response.children().get(redisMessage);
                    assertEquals(expectedClientId.get(), Long.valueOf(clientId.value()));
                    continue;
                case "mode":
                    FullBulkStringRedisMessage mode = (FullBulkStringRedisMessage) response.children().get(redisMessage);
                    assertEquals("cluster", mode.content().toString());
                    continue;
                case "role":
                    FullBulkStringRedisMessage role = (FullBulkStringRedisMessage) response.children().get(redisMessage);
                    assertEquals("master", role.content().toString());
                    continue;
            }
        }
    }

    @Test
    void shouldReturnNOPROTOForUnsupportedProtocolVersion() {
        // Behavior: a protocol version the server does not speak is rejected with the NOPROTO error.
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.hello(4, null, null, null).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ErrorRedisMessage.class, msg);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertEquals("NOPROTO unsupported protocol version", actualMessage.content());
    }

    @Test
    void shouldSetClientNameWithSETNAME() {
        // Behavior: the SETNAME option stores the given name on the client of the current connection.
        RedisCommandBuilder<String, String> cmd = new RedisCommandBuilder<>(StringCodec.ASCII);
        ByteBuf buf = Unpooled.buffer();
        cmd.hello(2, null, null, "test").encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        Attribute<Long> clientID = channel.attr(SessionAttributes.CLIENT_ID);
        Client client = Clients.getClient(clientID.get());
        assertNotNull(client);
        assertEquals("test", client.getName());
    }

    @Test
    void shouldReturnNOPROTOForProtocolVersionOutOfIntegerRange() {
        // Behavior: a protocol version that is a valid number but too large for a 32-bit integer is still an
        // unsupported version, so HELLO 99999999999 is rejected with the NOPROTO error.
        Object msg = runRaw(channel, CommandType.HELLO, List.of("99999999999"));

        assertInstanceOf(ErrorRedisMessage.class, msg);
        assertEquals("NOPROTO unsupported protocol version", ((ErrorRedisMessage) msg).content());
    }

    @Test
    void shouldRejectUnknownOption() {
        // Behavior: HELLO accepts only the AUTH and SETNAME options. HELLO 3 FOO is rejected with a syntax error
        // that quotes the option as the client sent it.
        Object msg = runRaw(channel, CommandType.HELLO, List.of("3", "FOO"));

        assertInstanceOf(ErrorRedisMessage.class, msg);
        assertEquals("ERR Syntax error in HELLO option 'FOO'", ((ErrorRedisMessage) msg).content());
    }

    @Test
    void shouldRejectAuthOptionWithoutPassword() {
        // Behavior: the AUTH option needs a username and a password. HELLO 3 AUTH devuser, with the password
        // missing, is rejected with a syntax error that names the AUTH option.
        Object msg = runRaw(channel, CommandType.HELLO, List.of("3", "AUTH", "devuser"));

        assertInstanceOf(ErrorRedisMessage.class, msg);
        assertEquals("ERR Syntax error in HELLO option 'AUTH'", ((ErrorRedisMessage) msg).content());
    }

    @Test
    void shouldRejectSetNameOptionWithoutName() {
        // Behavior: the SETNAME option needs a client name. HELLO 3 SETNAME, with the name missing, is rejected
        // with a syntax error that names the SETNAME option.
        Object msg = runRaw(channel, CommandType.HELLO, List.of("3", "SETNAME"));

        assertInstanceOf(ErrorRedisMessage.class, msg);
        assertEquals("ERR Syntax error in HELLO option 'SETNAME'", ((ErrorRedisMessage) msg).content());
    }

    @Test
    void shouldRejectClientNameWithSpace() {
        // Behavior: a client name may hold only printable ASCII characters without spaces. HELLO 3 SETNAME with
        // the name "my client" is rejected, and no name is stored for the connection.
        Object msg = runRaw(channel, CommandType.HELLO, List.of("3", "SETNAME", "my client"));

        assertInstanceOf(ErrorRedisMessage.class, msg);
        assertEquals(
                "ERR Client names cannot contain spaces, newlines or special characters.",
                ((ErrorRedisMessage) msg).content()
        );
        assertNull(Clients.getClient(channel.attr(SessionAttributes.CLIENT_ID).get()));
    }
}
