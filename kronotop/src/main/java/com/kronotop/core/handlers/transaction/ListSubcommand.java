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
import com.kronotop.server.*;
import com.kronotop.server.resp3.*;
import com.kronotop.transaction.InstrumentedTransaction;
import com.kronotop.transaction.TransactionMetrics;
import com.kronotop.transaction.TransactionState;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeMap;

import static com.kronotop.server.RESPUtil.bulkString;

/**
 * Lists the open transactions of this member with the metadata of the command that runs on each.
 */
class ListSubcommand implements SubcommandHandler {
    private final Context context;

    ListSubcommand(Context context) {
        this.context = context;
    }

    @Override
    public void execute(Request request, Response response) {
        if (request.getArguments().size() != 1) {
            throw new WrongNumberOfArgumentsException("wrong number of arguments for 'TRANSACTION LIST' command");
        }
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
        BqlExpr query = tr.getQuery();

        Map<RedisMessage, RedisMessage> fields = new LinkedHashMap<>();
        fields.put(bulkString("id"), new IntegerRedisMessage(tr.getId()));
        fields.put(bulkString("duration_ms"), new IntegerRedisMessage((now - metrics.getStartNanos()) / 1_000_000));
        long idleMs = tr.getState() == TransactionState.IDLE ? (now - tr.getIdleSinceNanos()) / 1_000_000 : 0;
        fields.put(bulkString("idle_ms"), new IntegerRedisMessage(idleMs));
        fields.put(bulkString("client_id"), new IntegerRedisMessage(tr.getClientId()));
        fields.put(bulkString("state"), bulkString(tr.getState().name().toLowerCase()));
        fields.put(bulkString("namespace"), text(tr.getNamespace()));
        fields.put(bulkString("bucket"), text(tr.getBucket()));
        fields.put(bulkString("model"), text(tr.getModel()));
        fields.put(bulkString("command"), text(tr.getCommand()));
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
