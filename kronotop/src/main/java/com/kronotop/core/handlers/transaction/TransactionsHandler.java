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

import com.kronotop.Context;
import com.kronotop.bucket.bql.QueryText;
import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.core.handlers.transaction.protocol.TransactionsMessage;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumArgumentCount;
import com.kronotop.transaction.InstrumentedTransaction;
import com.kronotop.transaction.TransactionMetrics;
import com.kronotop.server.resp3.*;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import static com.kronotop.server.RESPUtil.bulkString;

/**
 * Lists the open transactions of this member with the metadata of their owning session.
 */
@Command(TransactionsMessage.COMMAND)
@MaximumArgumentCount(TransactionsMessage.MAXIMUM_ARGUMENT_COUNT)
public class TransactionsHandler implements Handler {
    private final Context context;

    public TransactionsHandler(Context context) {
        this.context = context;
    }

    @Override
    public boolean isRedisCompatible() {
        return false;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.TRANSACTIONS).set(new TransactionsMessage());
    }

    @Override
    public void execute(Request request, Response response) {
        RESPVersion version = request.getSession().getProtocolVersion();
        long now = System.nanoTime();
        Map<Long, RedisMessage> sorted = new TreeMap<>();
        for (InstrumentedTransaction tr : context.getTransactionManager().transactions()) {
            sorted.put(tr.getId(), RESPUtil.downgrade(describe(tr, now), version));
        }
        response.writeArray(new ArrayList<>(sorted.values()));
    }

    private MapRedisMessage describe(InstrumentedTransaction tr, long now) {
        TransactionMetrics metrics = tr.getMetrics();
        Session session = tr.getClientId() == InstrumentedTransaction.INTERNAL
                ? null
                : context.getSessionStore().get(tr.getClientId());
        BqlExpr query = session != null ? session.getQuery() : null;

        Map<RedisMessage, RedisMessage> fields = new LinkedHashMap<>();
        fields.put(bulkString("id"), new IntegerRedisMessage(tr.getId()));
        fields.put(bulkString("duration_ms"), new IntegerRedisMessage((now - metrics.getStartNanos()) / 1_000_000));
        fields.put(bulkString("client_id"), new IntegerRedisMessage(tr.getClientId()));
        fields.put(bulkString("state"), bulkString(tr.getState().name().toLowerCase()));
        fields.put(bulkString("namespace"), text(session != null ? session.getNamespace() : null));
        fields.put(bulkString("bucket"), text(session != null ? session.getBucket() : null));
        fields.put(bulkString("model"), text(session != null ? session.getModel() : null));
        fields.put(bulkString("command"), text(session != null ? session.getCommand() : null));
        fields.put(bulkString("query"), text(query != null ? QueryText.render(query) : null));
        fields.put(bulkString("watching"), metrics.isWatching() ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE);
        fields.put(bulkString("reads"), new IntegerRedisMessage(metrics.getReads()));
        fields.put(bulkString("range_reads"), new IntegerRedisMessage(metrics.getRangeReads()));
        fields.put(bulkString("writes"), new IntegerRedisMessage(metrics.getWrites()));
        fields.put(bulkString("deletes"), new IntegerRedisMessage(metrics.getDeletes()));
        fields.put(bulkString("range_deletes"), new IntegerRedisMessage(metrics.getRangeDeletes()));
        fields.put(bulkString("mutations"), new IntegerRedisMessage(metrics.getMutations()));
        fields.put(bulkString("bytes_read"), new IntegerRedisMessage(metrics.getBytesRead()));
        fields.put(bulkString("bytes_written"), new IntegerRedisMessage(metrics.getBytesWritten()));
        return new MapRedisMessage(fields);
    }

    private static RedisMessage text(String value) {
        return value == null ? NullRedisMessage.INSTANCE : bulkString(value);
    }
}
