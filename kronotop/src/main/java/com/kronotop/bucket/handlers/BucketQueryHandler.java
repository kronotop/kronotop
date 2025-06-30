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
import com.kronotop.KronotopException;
import com.kronotop.bucket.*;
import com.kronotop.bucket.executor.PlanExecutorEnvironment;
import com.kronotop.bucket.executor.PlanExecutor;
import com.kronotop.bucket.handlers.protocol.BucketQueryMessage;
import com.kronotop.bucket.index.Index;
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

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketQueryMessage.COMMAND)
@MaximumParameterCount(BucketQueryMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(BucketQueryMessage.MINIMUM_PARAMETER_COUNT)
public class BucketQueryHandler extends BaseBucketHandler implements Handler {

    public BucketQueryHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETQUERY).set(new BucketQueryMessage(request));
    }

    /**
     * Prepares a {@code ByteBuf} value based on the reply type derived from the given request.
     * The method generates either a BSON or JSON formatted {@code ByteBuf} depending on the reply type
     * of the session associated with the request. In case of an unsupported reply type, an exception is thrown.
     *
     * @param request the {@code Request} object containing the session information and other relevant data
     * @param entry   a {@code Map.Entry} containing a {@code Versionstamp} as the key and a {@code ByteBuffer} as the value,
     *                where the value represents the data to be transformed into the {@code ByteBuf}
     * @return a {@code ByteBuf} object containing the serialized data in the format specified by the reply type
     * @throws KronotopException if the reply type is invalid or not supported
     */
    private ByteBuf prepareValue(Request request, Map.Entry<Versionstamp, ByteBuffer> entry) {
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
        return value;
    }

    /**
     * Processes and writes a RESP3-compliant response to the client based on the provided entries.
     * Converts the provided map of {@code Versionstamp} to {@code ByteBuffer} into a Redis-compatible
     * map format and writes the resulting data to the client.
     *
     * @param request  the {@code Request} object containing the session and command information
     * @param response the {@code Response} object used to send the result back to the client
     * @param entries  a map where the keys are {@code Versionstamp} objects and the values are
     *                 {@code ByteBuffer} objects representing the data to be converted and sent
     */
    private void resp3Response(Request request, Response response, Map<Versionstamp, ByteBuffer> entries) {
        if (entries == null || entries.isEmpty()) {
            response.writeMap(MapRedisMessage.EMPTY_INSTANCE.children());
            return;
        }
        Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
        for (Map.Entry<Versionstamp, ByteBuffer> entry : entries.entrySet()) {
            ByteBuf value = prepareValue(request, entry);
            result.put(
                    new SimpleStringRedisMessage(VersionstampUtils.base32HexEncode(entry.getKey())),
                    new FullBulkStringRedisMessage(value)
            );
        }
        response.writeMap(result);
    }

    /**
     * Processes and writes a RESP2-compliant response to the client based on the provided entries.
     * Converts the provided map of {@code Versionstamp} to {@code ByteBuffer} into a Redis-compatible
     * list format by iterating through the map entries, preparing their values, and sending the
     * resulting data to the client.
     *
     * @param request  the {@code Request} object containing the session and command information
     * @param response the {@code Response} object used to send the result back to the client
     * @param entries  a map where the keys are {@code Versionstamp} objects and the values are
     *                 {@code ByteBuffer} objects representing the data to be converted and sent
     */
    private void resp2Response(Request request, Response response, Map<Versionstamp, ByteBuffer> entries) {
        if (entries == null || entries.isEmpty()) {
            response.writeArray(List.of());
            return;
        }
        List<RedisMessage> result = new LinkedList<>();
        for (Map.Entry<Versionstamp, ByteBuffer> entry : entries.entrySet()) {
            ByteBuf value = prepareValue(request, entry);
            result.add(new SimpleStringRedisMessage(VersionstampUtils.base32HexEncode(entry.getKey())));
            result.add(new FullBulkStringRedisMessage(value));
        }
        response.writeArray(result);
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketQueryMessage message = request.attr(MessageTypes.BUCKETQUERY).get();

            Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), request.getSession());
            BucketSubspace subspace = BucketSubspaceUtils.open(context, request.getSession(), tr);

            // ID is the default index
            Map<String, Index> indexes = Map.of(
                    DefaultIndex.ID.path(), DefaultIndex.ID
            );
            PhysicalNode plan = service.getPlanner().plan(indexes, message.getQuery());

            BucketShard shard = service.getShard(1);
            PlanExecutorEnvironment executorEnvironment = new PlanExecutorEnvironment(shard, plan, message.getBucket(), subspace);
            PlanExecutor executor = new PlanExecutor(context, executorEnvironment);
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
