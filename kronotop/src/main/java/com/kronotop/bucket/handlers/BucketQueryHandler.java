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

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.BucketQueryMessage;
import com.kronotop.bucket.pipeline.QueryContext;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketQueryMessage.COMMAND)
@MinimumParameterCount(BucketQueryMessage.MINIMUM_PARAMETER_COUNT)
public class BucketQueryHandler extends AbstractBucketHandler implements Handler {

    public BucketQueryHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETQUERY).set(new BucketQueryMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketQueryMessage message = request.attr(MessageTypes.BUCKETQUERY).get();
            Session session = request.getSession();

            QueryContext ctx = buildQueryContext(request, message.getBucket(), message.getQuery(), message.getArguments());
            Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), session);
            int cursorId = session.nextCursorId();
            session.attr(SessionAttributes.BUCKET_READ_QUERY_CONTEXTS).get().put(cursorId, ctx);

            return new BucketEntriesMapResponse(cursorId, service.getQueryExecutor().read(tr, ctx));
        }, (entryMapResponse) -> {
            RESPVersion protoVer = request.getSession().protocolVersion();
            if (protoVer.equals(RESPVersion.RESP3)) {
                resp3Response(request, response, entryMapResponse);
            } else if (protoVer.equals(RESPVersion.RESP2)) {
                resp2Response(request, response, entryMapResponse);
            } else {
                throw new KronotopException("Unknown protocol version " + protoVer.getValue());
            }
        });
    }
}
