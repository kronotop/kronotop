/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.foundationdb.zmap;

import com.apple.foundationdb.KeySelector;
import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.kronotop.NamespaceUtils;
import com.kronotop.TransactionUtils;
import com.kronotop.foundationdb.BaseHandler;
import com.kronotop.foundationdb.FoundationDBService;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.foundationdb.zmap.protocol.RangeKeySelector;
import com.kronotop.foundationdb.zmap.protocol.ZGetRangeMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

@Command(ZGetRangeMessage.COMMAND)
@MinimumParameterCount(ZGetRangeMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(ZGetRangeMessage.MAXIMUM_PARAMETER_COUNT)
public class ZGetRangeHandler extends BaseHandler implements Handler {
    public ZGetRangeHandler(FoundationDBService service) {
        super(service);
    }

    private AsyncIterable<KeyValue> getRange(Transaction tr, KeySelector begin, KeySelector end,
                                             int limit, boolean reverse, boolean isSnapshot) {
        if (isSnapshot) {
            return tr.snapshot().getRange(begin, end, limit, reverse);
        }
        return tr.getRange(begin, end, limit, reverse);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ZGETRANGE).set(new ZGetRangeMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws ExecutionException, InterruptedException {
        ZGetRangeMessage message = request.attr(MessageTypes.ZGETRANGE).get();

        Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), request.getChannelContext());
        Namespace namespace = NamespaceUtils.open(service.getContext(), request.getChannelContext(), tr);

        byte[] begin;
        byte[] end;
        if (Arrays.equals(message.getBegin(), ZGetRangeMessage.ASTERISK)) {
            begin = namespace.getZMap().pack();
            end = ByteArrayUtil.strinc(namespace.getZMap().pack());
        } else {
            begin = namespace.getZMap().pack(message.getBegin());
            if (Arrays.equals(message.getEnd(), ZGetRangeMessage.ASTERISK)) {
                end = ByteArrayUtil.strinc(namespace.getZMap().pack());
            } else {
                end = namespace.getZMap().pack(message.getEnd());
            }
        }

        KeySelector beginKeySelector = RangeKeySelector.getKeySelector(message.getBeginKeySelector(), begin);
        KeySelector endKeySelector = RangeKeySelector.getKeySelector(message.getEndKeySelector(), end);

        AsyncIterable<KeyValue> asyncIterable = getRange(
                tr, beginKeySelector, endKeySelector, message.getLimit(),
                message.getReverse(), TransactionUtils.isSnapshotRead(response.getChannelContext()));

        List<RedisMessage> upperList = new ArrayList<>();
        for (KeyValue keyValue : asyncIterable) {
            ByteBuf keyBuf = response.getChannelContext().alloc().buffer();
            ByteBuf valueBuf = response.getChannelContext().alloc().buffer();

            try {
                List<RedisMessage> pair = new ArrayList<>();
                keyBuf.writeBytes(namespace.getZMap().unpack(keyValue.getKey()).getBytes(0));
                pair.add(new FullBulkStringRedisMessage(keyBuf));

                valueBuf.writeBytes(keyValue.getValue());
                pair.add(new FullBulkStringRedisMessage(valueBuf));

                upperList.add(new ArrayRedisMessage(pair));
            } catch (Exception e) {
                if (keyBuf.refCnt() > 0) {
                    ReferenceCountUtil.release(keyBuf);
                }
                if (valueBuf.refCnt() > 0) {
                    ReferenceCountUtil.release(valueBuf);
                }
                throw e;
            }
        }
        response.writeArray(upperList);
    }
}