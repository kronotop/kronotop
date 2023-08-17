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

package com.kronotop.redis.connection.protocol;

import com.kronotop.BaseProtocolTest;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.resp.Request;
import com.kronotop.server.resp.impl.RespRequest;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AuthMessageTest extends BaseProtocolTest {
    @Test
    public void testAuthMessage() {
        KronotopCommandBuilder<String, String> cb = new KronotopCommandBuilder<>(StringCodec.ASCII);

        String expectedUsername = "devuser";
        String expectedPassword = "devpass";
        ByteBuf buf = Unpooled.buffer();
        cb.auth(expectedUsername, expectedPassword).encode(buf);

        channel.writeInbound(buf);
        Object msg = channel.readInbound();

        Request request = new RespRequest(null, msg);
        AuthMessage authMessage = new AuthMessage(request);

        assertEquals(expectedUsername, authMessage.getUsername());
        assertEquals(expectedPassword, authMessage.getPassword());
    }
}
