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

package com.kronotop.core.handlers.transaction;

import com.kronotop.BaseHandlerTest;
import com.kronotop.commands.CommitArgs;
import com.kronotop.commands.CommitKeyword;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.commands.ZMapCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class CommitHandlerTest extends BaseHandlerTest {

    @Test
    void shouldCommitTransaction() {
        TestTransaction tt = new TestTransaction(channel);
        tt.begin();
        tt.commit();
        Object response = tt.getResponse();

        assertInstanceOf(SimpleStringRedisMessage.class, response);
        SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
        assertEquals(Response.OK, actualMessage.content());
    }

    @Test
    void shouldRejectCommitWhenNoTransactionInProgress() {
        TestTransaction tt = new TestTransaction(channel);

        // Start a new transaction
        tt.commit();

        Object response = tt.getResponse();
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) response;
        assertEquals("TRANSACTION there is no transaction in progress.", actualMessage.content());
    }

    @Test
    void shouldReturnVersionstampOnCommit() {
        EmbeddedChannel channel = getChannel();
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        ZMapCommandBuilder<String, String> zmapCommandBuilder = new ZMapCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            zmapCommandBuilder.zset("my-key", "my-value").encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.commit(CommitArgs.Builder.returning(CommitKeyword.VERSIONSTAMP)).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(FullBulkStringRedisMessage.class, response);
            FullBulkStringRedisMessage message = (FullBulkStringRedisMessage) response;
            assertNotNull(message.content());
        }
    }

    @Test
    void shouldReturnCommittedVersionOnCommit() {
        EmbeddedChannel channel = getChannel();
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) response;
            assertEquals(Response.OK, actualMessage.content());
        }

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.commit(CommitArgs.Builder.returning(CommitKeyword.COMMITTED_VERSION)).encode(buf);

            Object response = runCommand(channel, buf);
            // Read-only transactions do not modify the database when committed and will have a committed version of -1.
            // Keep in mind that a transaction which reads keys and then sets them to their current values may be optimized
            // to a read-only transaction.
            assertInstanceOf(IntegerRedisMessage.class, response);
            IntegerRedisMessage message = (IntegerRedisMessage) response;
            assertEquals(-1, message.value());
        }
    }

    @Test
    void shouldRejectMultipleReturningParameters() {
        // Behavior: COMMIT RETURNING accepts only one parameter. Multiple parameters must be rejected.
        EmbeddedChannel channel = getChannel();
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);

        {
            ByteBuf buf = Unpooled.buffer();
            cmd.begin().encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, response);
        }

        {
            // Manually construct: COMMIT RETURNING committed-version versionstamp
            ByteBuf buf = Unpooled.buffer();
            cmd.commit(CommitArgs.Builder.returning(CommitKeyword.COMMITTED_VERSION, CommitKeyword.VERSIONSTAMP)).encode(buf);

            Object response = runCommand(channel, buf);
            assertInstanceOf(ErrorRedisMessage.class, response);
            ErrorRedisMessage errorMessage = (ErrorRedisMessage) response;
            assertTrue(errorMessage.content().contains("wrong number of arguments"),
                    "Should reject multiple RETURNING parameters");
        }
    }
}
