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

import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import com.kronotop.sql.backend.AssertResponse;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class BaseHandlerTest extends BaseSqlTest {
    protected void executeSqlQueryReturnsOK(KronotopCommandBuilder<String, String> cmd, String query) {
        ByteBuf buf = Unpooled.buffer();
        cmd.sql(query).encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        if (response instanceof ErrorRedisMessage) {
            System.out.println(((ErrorRedisMessage) response).content());
        }

        AssertResponse<SimpleStringRedisMessage> assertResponse = new AssertResponse<>();
        SimpleStringRedisMessage message = assertResponse.getMessage(response, 0, 1);
        assertEquals(Response.OK, message.content());
    }

    protected ErrorRedisMessage executeSqlQueryReturnsError(KronotopCommandBuilder<String, String> cmd, String query) {
        ByteBuf buf = Unpooled.buffer();
        cmd.sql(query).encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        AssertResponse<ErrorRedisMessage> assertResponse = new AssertResponse<>();
        return assertResponse.getMessage(response, 0, 1);
    }
}
