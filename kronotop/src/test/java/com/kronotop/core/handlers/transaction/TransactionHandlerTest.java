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
import com.kronotop.commands.CommandType;
import com.kronotop.commands.KronotopCommandBuilder;
import com.kronotop.commands.ZMapCommandBuilder;
import com.kronotop.server.Session;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.transaction.InstrumentedTransaction;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TransactionHandlerTest extends BaseHandlerTest {
    private final KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
    private final ZMapCommandBuilder<String, String> zmap = new ZMapCommandBuilder<>(StringCodec.ASCII);

    private Object kill(long id) {
        ByteBuf buf = Unpooled.buffer();
        cmd.transactionKill(id).encode(buf);
        return runCommand(channel, buf);
    }

    private Object zset() {
        ByteBuf buf = Unpooled.buffer();
        zmap.zset("key", "value").encode(buf);
        return runCommand(channel, buf);
    }

    private Object zget() {
        ByteBuf buf = Unpooled.buffer();
        zmap.zget("key").encode(buf);
        return runCommand(channel, buf);
    }

    private void assertError(Object msg, String content) {
        assertInstanceOf(ErrorRedisMessage.class, msg);
        assertEquals(content, ((ErrorRedisMessage) msg).content());
    }

    private long sessionTransactionId() {
        Session session = Session.extractSessionFromChannel(channel);
        InstrumentedTransaction tr = (InstrumentedTransaction) session.attr(SessionAttributes.TRANSACTION).get();
        assertNotNull(tr);
        return tr.getId();
    }

    @Test
    void shouldKillBeginTransaction() {
        createNamespace(channel, namespace);
        useNamespace(channel, namespace);

        ByteBuf buf = Unpooled.buffer();
        cmd.begin().encode(buf);
        assertOK(runCommand(channel, buf));
        assertOK(zset());
        long id = sessionTransactionId();

        assertOK(kill(id));
        assertNotNull(context.getTransactionManager().get(id));

        // A write is buffered on the client side. A read hits FoundationDB and fails.
        assertError(zget(), "TRANSACTION_CANCELLED Operation aborted because the transaction was cancelled");

        buf = Unpooled.buffer();
        cmd.rollback().encode(buf);
        assertOK(runCommand(channel, buf));
        assertNull(context.getTransactionManager().get(id));
    }

    @Test
    void shouldRejectUnknownId() {
        assertError(kill(999999), "TRANSACTION no such transaction: 999999");
    }

    @Test
    void shouldRejectInternalTransaction() {
        try (InstrumentedTransaction tr = context.getTransactionManager().create()) {
            assertError(kill(tr.getId()), "TRANSACTION internal transactions cannot be killed");
            assertSame(tr, context.getTransactionManager().get(tr.getId()));
        }
    }

    @Test
    void shouldRejectUnknownSubcommand() {
        assertError(runRaw(channel, CommandType.TRANSACTION, List.of("FOO")), "ERR unknown subcommand: 'FOO'");
    }

    @Test
    void shouldRejectMissingId() {
        assertError(runRaw(channel, CommandType.TRANSACTION, List.of("KILL")),
                "ERR wrong number of arguments for 'TRANSACTION KILL' command");
    }
}
