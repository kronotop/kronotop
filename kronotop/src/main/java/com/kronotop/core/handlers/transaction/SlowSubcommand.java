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
import com.kronotop.server.*;
import com.kronotop.server.resp3.*;
import com.kronotop.transaction.SlowTransaction;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.kronotop.server.RESPUtil.bulkString;

/**
 * Lists the slow transactions logged by this member, newest first.
 */
class SlowSubcommand implements SubcommandHandler {
    private final Context context;

    SlowSubcommand(Context context) {
        this.context = context;
    }

    private static RedisMessage bool(boolean value) {
        return value ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE;
    }

    private static RedisMessage text(String value) {
        return value == null ? NullRedisMessage.INSTANCE : bulkString(value);
    }

    @Override
    public void execute(Request request, Response response) {
        if (request.getArguments().size() != 1) {
            throw new WrongNumberOfArgumentsException("wrong number of arguments for 'TRANSACTION SLOW' command");
        }
        RESPVersion version = request.getSession().getProtocolVersion();
        List<RedisMessage> result = new ArrayList<>();
        for (SlowTransaction entry : context.getTransactionManager().getSlowLog().entries()) {
            result.add(RESPUtil.downgrade(describe(entry), version));
        }
        response.writeArray(result);
    }

    private MapRedisMessage describe(SlowTransaction entry) {
        Map<RedisMessage, RedisMessage> fields = new LinkedHashMap<>();
        fields.put(bulkString("id"), new IntegerRedisMessage(entry.id()));
        fields.put(bulkString("timestamp"), new IntegerRedisMessage(entry.timestamp()));
        fields.put(bulkString("duration_ms"), new IntegerRedisMessage(entry.durationMs()));
        fields.put(bulkString("commit_latency_ms"), new IntegerRedisMessage(entry.commitLatencyMs()));
        fields.put(bulkString("client_id"), new IntegerRedisMessage(entry.clientId()));
        fields.put(bulkString("namespace"), text(entry.namespace()));
        fields.put(bulkString("bucket"), text(entry.bucket()));
        fields.put(bulkString("model"), text(entry.model()));
        fields.put(bulkString("command"), text(entry.command()));
        fields.put(bulkString("query"), text(entry.query() != null ? QueryText.render(entry.query()) : null));
        fields.put(bulkString("committed"), bool(entry.committed()));
        fields.put(bulkString("conflicted"), bool(entry.conflicted()));
        fields.put(bulkString("reads"), new IntegerRedisMessage(entry.reads()));
        fields.put(bulkString("range_reads"), new IntegerRedisMessage(entry.rangeReads()));
        fields.put(bulkString("writes"), new IntegerRedisMessage(entry.writes()));
        fields.put(bulkString("deletes"), new IntegerRedisMessage(entry.deletes()));
        fields.put(bulkString("range_deletes"), new IntegerRedisMessage(entry.rangeDeletes()));
        fields.put(bulkString("mutations"), new IntegerRedisMessage(entry.mutations()));
        fields.put(bulkString("bytes_read"), new IntegerRedisMessage(entry.bytesRead()));
        fields.put(bulkString("bytes_written"), new IntegerRedisMessage(entry.bytesWritten()));
        return new MapRedisMessage(fields);
    }
}
