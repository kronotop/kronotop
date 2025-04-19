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
import com.apple.foundationdb.tuple.ByteArrayUtil;
import com.kronotop.foundationdb.BaseFoundationDBHandler;
import com.kronotop.foundationdb.FoundationDBService;
import com.kronotop.foundationdb.namespace.Namespace;
import com.kronotop.foundationdb.zmap.protocol.ZDelRangeMessage;
import com.kronotop.internal.NamespaceUtils;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;

@Command(ZDelRangeMessage.COMMAND)
@MinimumParameterCount(ZDelRangeMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(ZDelRangeMessage.MAXIMUM_PARAMETER_COUNT)
public class ZDelRangeHandler extends BaseFoundationDBHandler implements Handler {
    public ZDelRangeHandler(FoundationDBService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ZDELRANGE).set(new ZDelRangeMessage(request));
    }

    @Override
    public void execute(Request request, Response response) {
        CompletableFuture.runAsync(() -> {
            ZDelRangeMessage message = request.attr(MessageTypes.ZDELRANGE).get();

            Session session = request.getSession();
            Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), session);
            Namespace namespace = NamespaceUtils.open(service.getContext(), session, tr);

            byte[] begin;
            byte[] end;
            if (Arrays.equals(message.getBegin(), ZDelRangeMessage.ASTERISK)) {
                begin = namespace.getZMap().pack();
            } else {
                begin = namespace.getZMap().pack(message.getBegin());
            }

            if (Arrays.equals(message.getEnd(), ZDelRangeMessage.ASTERISK)) {
                end = ByteArrayUtil.strinc(namespace.getZMap().pack());
            } else {
                end = namespace.getZMap().pack(message.getEnd());
            }

            Range range = new Range(begin, end);
            tr.clear(range);
            TransactionUtils.commitIfAutoCommitEnabled(tr, session);
        }, context.getVirtualThreadPerTaskExecutor()).thenAcceptAsync((v) -> response.writeOK(), response.getCtx().executor()).exceptionally((ex) -> {
            response.writeError(ex);
            return null;
        });
    }
}