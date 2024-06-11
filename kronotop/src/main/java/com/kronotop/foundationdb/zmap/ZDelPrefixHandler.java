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
import com.kronotop.TransactionUtils;
import com.kronotop.foundationdb.BaseHandler;
import com.kronotop.foundationdb.FoundationDBService;
import com.kronotop.foundationdb.zmap.protocol.ZDelPrefixMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.concurrent.ExecutionException;

@Command(ZDelPrefixMessage.COMMAND)
@MinimumParameterCount(ZDelPrefixMessage.MINIMUM_PARAMETER_COUNT)
@MaximumParameterCount(ZDelPrefixMessage.MAXIMUM_PARAMETER_COUNT)
public class ZDelPrefixHandler extends BaseHandler implements Handler {
    public ZDelPrefixHandler(FoundationDBService service) {
        super(service);
    }

    @Override
    public boolean isWatchable() {
        return true;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.ZDELPREFIX).set(new ZDelPrefixMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws ExecutionException, InterruptedException {
        ZDelPrefixMessage zDelPrefixMessage = request.attr(MessageTypes.ZDELPREFIX).get();

        Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), request.getChannelContext());
        //Namespace namespace = NamespaceUtils.open(service.getContext(), request.getChannelContext(), tr);

        Range range = Range.startsWith(zDelPrefixMessage.getPrefix());
        tr.clear(range);
        TransactionUtils.commitIfAutoCommitEnabled(tr, request.getChannelContext());

        response.writeOK();
    }
}