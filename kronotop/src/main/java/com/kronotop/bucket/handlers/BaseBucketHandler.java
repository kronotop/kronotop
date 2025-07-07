// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.


package com.kronotop.bucket.handlers;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BSONUtils;
import com.kronotop.bucket.BucketService;
import com.kronotop.internal.VersionstampUtils;
import com.kronotop.server.*;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import org.bson.Document;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public abstract class BaseBucketHandler implements Handler {
    protected final BucketService service;
    protected final Context context;

    public BaseBucketHandler(BucketService service) {
        this.service = service;
        this.context = service.getContext();
    }

    @Override
    public boolean isRedisCompatible() {
        return false;
    }

    /**
     * Retrieves the input type associated with the given request's session.
     *
     * @param request the request object containing the session from which the input type is retrieved
     * @return the input type associated with the session, typically either JSON or BSON
     */
    protected InputType getInputType(Request request) {
        return request.getSession().attr(com.kronotop.server.SessionAttributes.INPUT_TYPE).get();
    }

    /**
     * Retrieves the reply type associated with the given request's session.
     *
     * @param request the request object containing the session from which the reply type is retrieved
     * @return the reply type associated with the session, typically either JSON or BSON
     */
    private ReplyType getReplyType(Request request) {
        return request.getSession().attr(com.kronotop.server.SessionAttributes.REPLY_TYPE).get();
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
    protected ByteBuf prepareValue(Request request, Map.Entry<Versionstamp, ByteBuffer> entry) {
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
    protected void resp3Response(Request request, Response response, Map<Versionstamp, ByteBuffer> entries) {
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
    protected void resp2Response(Request request, Response response, Map<Versionstamp, ByteBuffer> entries) {
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
}
