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

package com.kronotop.foundationdb;

import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class GetReadVersionHandlerTest extends BaseHandlerTest {
    @Test
    public void testGETREADVERSION() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals("OK", actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.getreadversion().encode(buf);
            channel.writeInbound(buf);
            Object response = channel.readOutbound();

            assertInstanceOf(IntegerRedisMessage.class, response);
            IntegerRedisMessage actualMessage = (IntegerRedisMessage) response;
            assertTrue(actualMessage.value() > 0);
        }
    }

    @Test
    public void testGETREADVERSION_NoTransactionInProgress() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        EmbeddedChannel channel = getChannel();

        ByteBuf buf = Unpooled.buffer();
        cmd.getreadversion().encode(buf);
        channel.writeInbound(buf);
        Object response = channel.readOutbound();

        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
        assertEquals("TRANSACTION there is no transaction in progress.", actualMessage.content());
    }
}
