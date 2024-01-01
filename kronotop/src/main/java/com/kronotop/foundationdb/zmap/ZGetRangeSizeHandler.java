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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.subspace.Subspace;
import com.kronotop.foundationdb.zmap.protocol.ZGetRangeSizeMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.concurrent.CompletableFuture;

@Command(ZGetRangeSizeMessage.COMMAND)
@MinimumParameterCount(ZGetRangeSizeMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(ZGetRangeSizeMessage.MAXIMUM_PARAMETER_COUNT)
class ZGetRangeSizeHandler extends BaseZMapHandler implements Handler {
    public ZGetRangeSizeHandler(ZMapService service) {
        super(service);
    }

    private CompletableFuture<Long> getEstimatedRangeSizeBytes(Transaction tr, Range range, boolean isSnapshot) {
        if (isSnapshot) {
            return tr.snapshot().getEstimatedRangeSizeBytes(range);
        }
        return tr.getEstimatedRangeSizeBytes(range);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ZGETRANGESIZE).set(new ZGetRangeSizeMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        ZGetRangeSizeMessage zGetRangeSizeMessage = request.attr(MessageTypes.ZGETRANGESIZE).get();

        Subspace subspace = getSubspace(response, zGetRangeSizeMessage.getNamespace());
        Transaction tr = getOrCreateTransaction(response);
        byte[] begin = subspace.pack(zGetRangeSizeMessage.getBegin());
        byte[] end = subspace.pack(zGetRangeSizeMessage.getEnd());
        Range range = new Range(begin, end);
        CompletableFuture<Long> future = getEstimatedRangeSizeBytes(tr, range, isSnapshotRead(response));
        Long size = future.join();
        response.writeInteger(size);
    }
}
