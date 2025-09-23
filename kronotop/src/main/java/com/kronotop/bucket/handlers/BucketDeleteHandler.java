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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BucketVersionstampArrayResponse;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.BucketDeleteMessage;
import com.kronotop.bucket.pipeline.QueryContext;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.List;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketDeleteMessage.COMMAND)
@MaximumParameterCount(BucketDeleteMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(BucketDeleteMessage.MINIMUM_PARAMETER_COUNT)
public class BucketDeleteHandler extends AbstractBucketHandler implements Handler {
    public BucketDeleteHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETDELETE).set(new BucketDeleteMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketDeleteMessage message = request.attr(MessageTypes.BUCKETDELETE).get();

            Session session = request.getSession();
            QueryContext ctx = buildQueryContext(request, message.getBucket(), message.getQuery(), message.getArguments());
            int cursorId = session.nextCursorId();
            session.attr(SessionAttributes.BUCKET_DELETE_QUERY_CONTEXTS).get().put(cursorId, ctx);

            Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), session);
            List<Versionstamp> versionstamps = service.getQueryExecutor().delete(tr, ctx);

            TransactionUtils.addPostCommitHook(new QueryContextCommitHook(ctx), request.getSession());
            TransactionUtils.commitIfAutoCommitEnabled(tr, request.getSession());
            return new BucketVersionstampArrayResponse(cursorId, versionstamps);
        }, (versionstampResponse) -> {
            RESPVersion protoVer = request.getSession().protocolVersion();
            if (protoVer.equals(RESPVersion.RESP3)) {
                resp3VersionstampArrayResponse(response, versionstampResponse);
            } else if (protoVer.equals(RESPVersion.RESP2)) {
                resp2VersionstampArrayResponse(response, versionstampResponse);
            } else {
                throw new KronotopException("Unknown protocol version " + protoVer.getValue());
            }
        });
    }
}
