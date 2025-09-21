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
import com.kronotop.bucket.BucketDeleteResponse;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.BucketDeleteMessage;
import com.kronotop.bucket.pipeline.PipelineNode;
import com.kronotop.bucket.pipeline.QueryContext;
import com.kronotop.bucket.pipeline.QueryOptions;
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
            Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), session);
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, message.getBucket());

            QueryOptions.Builder builder = QueryOptions.builder();
            if (message.getArguments().limit() == 0) {
                builder.limit(session.attr(SessionAttributes.LIMIT).get());
            } else {
                builder.limit(message.getArguments().limit());
            }
            builder.reverse(message.getArguments().reverse());

            QueryOptions options = builder.build();
            PipelineNode plan = service.getPlanner().plan(metadata, message.getQuery());
            QueryContext ctx = new QueryContext(metadata, options, plan);

            int cursorId = session.nextCursorId();
            session.attr(SessionAttributes.BUCKET_DELETE_QUERY_CONTEXTS).get().put(cursorId, ctx);

            List<Versionstamp> versionstamps = service.getQueryExecutor().delete(tr, ctx);

            boolean autoCommit = TransactionUtils.getAutoCommit(request.getSession());
            TransactionUtils.addPostCommitHook(new QueryContextCommitHook(ctx), request.getSession());
            TransactionUtils.commitIfAutoCommitEnabled(tr, request.getSession());
            if (autoCommit) {
                ctx.runPostCommitHooks();
            }
            return new BucketDeleteResponse(cursorId, versionstamps);
        }, (deleteResponse) -> {
            RESPVersion protoVer = request.getSession().protocolVersion();
            if (protoVer.equals(RESPVersion.RESP3)) {
                resp3VersionstampArrayResponse(response, deleteResponse);
            } else if (protoVer.equals(RESPVersion.RESP2)) {
                resp2VersionstampArrayResponse(response, deleteResponse);
            } else {
                throw new KronotopException("Unknown protocol version " + protoVer.getValue());
            }
        });
    }
}
