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

import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.kronotop.foundationdb.BaseFoundationDBHandler;
import com.kronotop.foundationdb.FoundationDBService;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.foundationdb.zmap.protocol.ZGetRangeSizeMessage;
import com.kronotop.internal.NamespaceUtils;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.concurrent.CompletableFuture;

@Command(ZGetRangeSizeMessage.COMMAND)
@MinimumParameterCount(ZGetRangeSizeMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(ZGetRangeSizeMessage.MAXIMUM_PARAMETER_COUNT)
public class ZGetRangeSizeHandler extends BaseFoundationDBHandler implements Handler {
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
        CompletableFuture.supplyAsync(() -> {
            ZGetRangeSizeMessage message = request.attr(MessageTypes.ZGETRANGESIZE).get();

            Session session = request.getSession();
            Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), request.getSession());
            Namespace namespace = NamespaceUtils.open(service.getContext(), session, tr);

            byte[] begin = namespace.getZMap().pack(message.getBegin());
            byte[] end = namespace.getZMap().pack(message.getEnd());
            Range range = new Range(begin, end);
            return getEstimatedRangeSizeBytes(tr, range, TransactionUtils.isSnapshotRead(session)).join();
        }, context.getVirtualThreadPerTaskExecutor()).thenAcceptAsync(response::writeInteger, response.getCtx().executor()).exceptionally((ex) -> {
            response.writeError(ex);
            return null;
        });
    }
}
