/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.handlers;

import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.handlers.protocol.BucketCursorsMessage;
import com.kronotop.bucket.handlers.protocol.BucketOperation;
import com.kronotop.bucket.pipeline.QueryContext;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.resp3.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.*;

@Command(BucketCursorsMessage.COMMAND)
@MaximumParameterCount(BucketCursorsMessage.MAXIMUM_PARAMETER_COUNT)
public class BucketCursorsHandler extends AbstractBucketHandler {
    private final List<BucketOperation> operations = List.of(BucketOperation.QUERY, BucketOperation.UPDATE, BucketOperation.DELETE);

    public BucketCursorsHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETCURSORS).set(new BucketCursorsMessage(request));
    }

    private Map<RedisMessage, RedisMessage> serializeQueryContexts(Map<Integer, QueryContext> contexts) {
        Map<RedisMessage, RedisMessage> result = new HashMap<>();
        Map<Integer, RedisMessage> intermediate = new TreeMap<>();
        for (Map.Entry<Integer, QueryContext> entry : contexts.entrySet()) {
            byte[] query = entry.getValue().queryBytes();
            ByteBuf buf;
            if (BqlParser.isBSON(query)) {
                buf = Unpooled.wrappedBuffer(BSONUtil.fromBson(query).toJson().getBytes(StandardCharsets.UTF_8));
            } else {
                buf = Unpooled.wrappedBuffer(query);
            }
            intermediate.put(entry.getKey(), new FullBulkStringRedisMessage(buf));
        }
        for (Map.Entry<Integer, RedisMessage> entry : intermediate.entrySet()) {
            result.put(new IntegerRedisMessage(entry.getKey()), entry.getValue());
        }
        return result;
    }

    private void addOperationCursors(Session session, BucketOperation operation, Map<RedisMessage, RedisMessage> result) {
        Map<Integer, QueryContext> contexts = findQueryContext(session, operation);
        Map<RedisMessage, RedisMessage> child = serializeQueryContexts(contexts);
        result.put(bulkString(operation.name()), new MapRedisMessage(child));
    }

    private List<RedisMessage> serializeQueryContextsResp2(Map<Integer, QueryContext> contexts) {
        List<RedisMessage> result = new ArrayList<>();
        Map<Integer, RedisMessage> intermediate = new TreeMap<>();
        for (Map.Entry<Integer, QueryContext> entry : contexts.entrySet()) {
            byte[] query = entry.getValue().queryBytes();
            ByteBuf buf;
            if (!BqlParser.isBSON(query)) {
                buf = Unpooled.wrappedBuffer(query);
            } else {
                buf = Unpooled.wrappedBuffer(BSONUtil.fromBson(query).toJson().getBytes(StandardCharsets.UTF_8));
            }
            intermediate.put(entry.getKey(), new FullBulkStringRedisMessage(buf));
        }
        for (Map.Entry<Integer, RedisMessage> entry : intermediate.entrySet()) {
            result.add(new IntegerRedisMessage(entry.getKey()));
            result.add(entry.getValue());
        }
        return result;
    }

    private void addOperationCursorsResp2(Session session, BucketOperation operation, List<RedisMessage> result) {
        Map<Integer, QueryContext> contexts = findQueryContext(session, operation);
        List<RedisMessage> child = serializeQueryContextsResp2(contexts);
        result.add(new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(operation.name().getBytes(StandardCharsets.UTF_8))));
        result.add(new ArrayRedisMessage(child));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        BucketCursorsMessage message = request.attr(MessageTypes.BUCKETCURSORS).get();
        Session session = request.getSession();
        RESPVersion protoVer = session.protocolVersion();

        if (protoVer.equals(RESPVersion.RESP3)) {
            Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
            if (message.getOperation() != null) {
                addOperationCursors(session, message.getOperation(), result);
            } else {
                for (BucketOperation operation : operations) {
                    addOperationCursors(session, operation, result);
                }
            }
            response.writeMap(result);
        } else {
            List<RedisMessage> result = new ArrayList<>();
            if (message.getOperation() != null) {
                addOperationCursorsResp2(session, message.getOperation(), result);
            } else {
                for (BucketOperation operation : operations) {
                    addOperationCursorsResp2(session, operation, result);
                }
            }
            response.writeArray(result);
        }
    }
}
