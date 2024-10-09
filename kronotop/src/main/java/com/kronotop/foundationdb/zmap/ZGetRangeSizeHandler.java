/*
 * Copyright (c) 2023-2024 Kronotop
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
import com.kronotop.NamespaceUtils;
import com.kronotop.TransactionUtils;
import com.kronotop.foundationdb.BaseHandler;
import com.kronotop.foundationdb.FoundationDBService;
import com.kronotop.foundationdb.namespace.Namespace;
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
public class ZGetRangeSizeHandler extends BaseHandler implements Handler {
    public ZGetRangeSizeHandler(FoundationDBService service) {
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
        ZGetRangeSizeMessage message = request.attr(MessageTypes.ZGETRANGESIZE).get();

        Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), request.getChannelContext());
        Namespace namespace = NamespaceUtils.open(service.getContext(), request.getChannelContext(), tr);

        byte[] begin = namespace.getZMap().pack(message.getBegin());
        byte[] end = namespace.getZMap().pack(message.getEnd());
        Range range = new Range(begin, end);
        Long size = getEstimatedRangeSizeBytes(tr, range, TransactionUtils.isSnapshotRead(response.getChannelContext())).join();
        response.writeInteger(size);
    }
}
