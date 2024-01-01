/*
 * Copyright (c) 2023 Kronotop
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
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.ByteArrayUtil;
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
class ZGetRangeHandler extends BaseZMapHandler implements Handler {
    public ZGetRangeHandler(ZMapService service) {
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
        // Validates the request
        ZGetRangeMessage zGetRangeMessage = request.attr(MessageTypes.ZGETRANGE).get();

        Subspace subspace = getSubspace(response, zGetRangeMessage.getNamespace());
        byte[] begin;
        byte[] end;
        if (Arrays.equals(zGetRangeMessage.getBegin(), ZGetRangeMessage.ASTERISK)) {
            begin = subspace.pack();
            end = ByteArrayUtil.strinc(subspace.pack());
        } else {
            begin = subspace.pack(zGetRangeMessage.getBegin());
            if (Arrays.equals(zGetRangeMessage.getEnd(), ZGetRangeMessage.ASTERISK)) {
                end = ByteArrayUtil.strinc(subspace.pack());
            } else {
                end = subspace.pack(zGetRangeMessage.getEnd());
            }
        }

        KeySelector beginKeySelector = RangeKeySelector.getKeySelector(zGetRangeMessage.getBeginKeySelector(), begin);
        KeySelector endKeySelector = RangeKeySelector.getKeySelector(zGetRangeMessage.getEndKeySelector(), end);

        Transaction tr = getOrCreateTransaction(response);
        AsyncIterable<KeyValue> asyncIterable = getRange(
                tr, beginKeySelector, endKeySelector, zGetRangeMessage.getLimit(),
                zGetRangeMessage.getReverse(), isSnapshotRead(response));

        List<RedisMessage> upperList = new ArrayList<>();
        for (KeyValue keyValue : asyncIterable) {
            ByteBuf keyBuf = response.getContext().alloc().buffer();
            ByteBuf valueBuf = response.getContext().alloc().buffer();

            try {
                List<RedisMessage> pair = new ArrayList<>();
                keyBuf.writeBytes(subspace.unpack(keyValue.getKey()).getBytes(0));
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