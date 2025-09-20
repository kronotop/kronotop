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
import com.kronotop.bucket.handlers.protocol.BucketAdvanceMessage;
import com.kronotop.bucket.pipeline.QueryContext;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;

import java.util.Map;
import java.util.Objects;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketAdvanceMessage.COMMAND)
public class BucketAdvanceHandler extends BaseBucketHandler {
    public BucketAdvanceHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETADVANCE).set(new BucketAdvanceMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketAdvanceMessage message = request.attr(MessageTypes.BUCKETADVANCE).get();
            Session session = request.getSession();
            Map<Integer, QueryContext> contexts = session.attr(SessionAttributes.BUCKET_QUERY_CONTEXTS).get();
            QueryContext ctx = contexts.get(message.getCursorId());
            if (Objects.isNull(ctx)) {
                throw new KronotopException("No previous query context found with the given cursor id");
            }
            Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), session);
            return new BucketReadResponse(message.getCursorId(), service.getQueryExecutor().read(tr, ctx));
        }, (readResponse) -> {
            RESPVersion protoVer = request.getSession().protocolVersion();
            if (protoVer.equals(RESPVersion.RESP3)) {
                resp3Response(request, response, readResponse);
            } else if (protoVer.equals(RESPVersion.RESP2)) {
                resp2Response(request, response, readResponse);
            } else {
                throw new KronotopException("Unknown protocol version " + protoVer.getValue());
            }
        });
    }
}
