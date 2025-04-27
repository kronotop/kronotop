// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.AsyncCommandExecutor;
import com.kronotop.KronotopException;
import com.kronotop.bucket.*;
import com.kronotop.bucket.executor.ExecutorContext;
import com.kronotop.bucket.executor.PlanExecutor;
import com.kronotop.bucket.handlers.protocol.BucketFindMessage;
import com.kronotop.bucket.planner.physical.PhysicalNode;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.internal.VersionstampUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.bson.Document;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Command(BucketFindMessage.COMMAND)
@MaximumParameterCount(BucketFindMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(BucketFindMessage.MINIMUM_PARAMETER_COUNT)
public class BucketFindHandler extends BaseBucketHandler implements Handler {

    public BucketFindHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETFIND).set(new BucketFindMessage(request));
    }

    private void resp3Response(Request request, Response response, Map<Versionstamp, ByteBuffer> entries) {
        if (entries == null || entries.isEmpty()) {
            response.writeMap(MapRedisMessage.EMPTY_INSTANCE.children());
            return;
        }
        Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
        for (Map.Entry<Versionstamp, ByteBuffer> entry : entries.entrySet()) {
            ByteBuf value;
            ReplyType replyType = getReplyType(request);
            if (replyType.equals(ReplyType.BSON)) {
                value = PooledByteBufAllocator.DEFAULT.buffer().alloc().
                        buffer(entry.getValue().remaining()).writeBytes(entry.getValue());
            } else if (replyType.equals(ReplyType.JSON)) {
                Document document = BSONUtils.toDocument(entry.getValue().array());
                byte[] data = document.toJson().getBytes(StandardCharsets.UTF_8);
                value = PooledByteBufAllocator.DEFAULT.buffer().alloc().buffer(data.length).writeBytes(data);
            } else {
                throw new KronotopException("Invalid reply type: " + replyType);
            }
            result.put(
                    new SimpleStringRedisMessage(VersionstampUtils.base32HexEncode(entry.getKey())),
                    new FullBulkStringRedisMessage(value)
            );
        }
        response.writeMap(result);
    }

    private void resp2Response(Request request, Response response, Map<Versionstamp, ByteBuffer> entries) {
        if (entries == null || entries.isEmpty()) {
            response.writeArray(List.of());
            return;
        }
        List<RedisMessage> result = new LinkedList<>();
        for (Map.Entry<Versionstamp, ByteBuffer> entry : entries.entrySet()) {
            ByteBuf value;
            ReplyType replyType = getReplyType(request);
            if (replyType.equals(ReplyType.BSON)) {
                value = PooledByteBufAllocator.DEFAULT.buffer().alloc().
                        buffer(entry.getValue().remaining()).writeBytes(entry.getValue());
            } else if (replyType.equals(ReplyType.JSON)) {
                Document document = BSONUtils.toDocument(entry.getValue().array());
                byte[] data = document.toJson().getBytes(StandardCharsets.UTF_8);
                value = PooledByteBufAllocator.DEFAULT.buffer().alloc().buffer(data.length).writeBytes(data);
            } else {
                throw new KronotopException("Invalid reply type: " + replyType);
            }
            result.add(new SimpleStringRedisMessage(VersionstampUtils.base32HexEncode(entry.getKey())));
            result.add(new FullBulkStringRedisMessage(value));
        }
        response.writeArray(result);
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        AsyncCommandExecutor.supplyAsync(context, response, () -> {
            BucketFindMessage message = request.attr(MessageTypes.BUCKETFIND).get();

            Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), request.getSession());
            BucketSubspace subspace = BucketSubspaceUtils.open(context, request.getSession(), tr);
            PhysicalNode plan = service.getPlanner().plan(message.getBucket(), message.getQuery());

            BucketShard shard = service.getShard(1);
            ExecutorContext executorContext = new ExecutorContext(shard, plan, message.getBucket(), subspace);
            PlanExecutor executor = new PlanExecutor(context, executorContext);
            try {
                return executor.execute(tr);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }, (entries) -> {
            RESPVersion protoVer = request.getSession().protocolVersion();
            if (protoVer.equals(RESPVersion.RESP3)) {
                resp3Response(request, response, entries);
            } else if (protoVer.equals(RESPVersion.RESP2)) {
                resp2Response(request, response, entries);
            } else {
                throw new KronotopException("Unknown protocol version " + protoVer.getValue());
            }
        });
    }
}
