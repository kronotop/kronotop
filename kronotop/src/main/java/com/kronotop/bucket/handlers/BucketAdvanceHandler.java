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
import com.kronotop.bucket.*;
import com.kronotop.bucket.handlers.protocol.BucketAdvanceMessage;
import com.kronotop.bucket.handlers.protocol.BucketOperation;
import com.kronotop.bucket.pipeline.QueryContext;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketAdvanceMessage.COMMAND)
@MaximumParameterCount(BucketAdvanceMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(BucketAdvanceMessage.MAXIMUM_PARAMETER_COUNT)
public class BucketAdvanceHandler extends AbstractBucketHandler {
    private final EnumMap<BucketOperation, SubcommandHandler> executors = new EnumMap<>(BucketOperation.class);

    public BucketAdvanceHandler(BucketService service) {
        super(service);
        executors.put(BucketOperation.QUERY, new QuerySubcommand());
        executors.put(BucketOperation.UPDATE, new UpdateOrDeleteSubcommand(BucketOperation.UPDATE));
        executors.put(BucketOperation.DELETE, new UpdateOrDeleteSubcommand(BucketOperation.DELETE));
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETADVANCE).set(new BucketAdvanceMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        BucketAdvanceMessage message = request.attr(MessageTypes.BUCKETADVANCE).get();
        SubcommandHandler executor = executors.get(message.getOperation());
        if (executor == null) {
            throw new UnknownSubcommandException(message.getOperation().toString());
        }
        executor.execute(request, response);
    }

    private QueryContext getQueryContextAndValidate(Transaction tr, Request request) {
        BucketAdvanceMessage message = request.attr(MessageTypes.BUCKETADVANCE).get();
        Session session = request.getSession();
        Map<Integer, QueryContext> contexts = findQueryContext(session, message.getOperation());
        QueryContext ctx = contexts.get(message.getCursorId());
        if (Objects.isNull(ctx)) {
            throw new KronotopException("No previous query context found for '" +
                    message.getOperation().name().toLowerCase() + "' operation with the given cursor id");
        }
        BucketMetadata metadata = BucketMetadataUtil.open(context, tr, ctx.metadata().namespace(), ctx.metadata().name());
        ctx.updateMetadata(metadata);
        return ctx;
    }

    class QuerySubcommand implements SubcommandHandler {

        @Override
        public void execute(Request request, Response response) {
            supplyAsync(context, response, () -> {
                BucketAdvanceMessage message = request.attr(MessageTypes.BUCKETADVANCE).get();
                Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), request.getSession());
                QueryContext ctx = getQueryContextAndValidate(tr, request);
                return new BucketEntriesMapResponse(message.getCursorId(), service.getQueryExecutor().read(tr, ctx));
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

    class UpdateOrDeleteSubcommand implements SubcommandHandler {
        private final BucketOperation operation;

        UpdateOrDeleteSubcommand(BucketOperation operation) {
            this.operation = operation;
        }

        @Override
        public void execute(Request request, Response response) {
            supplyAsync(context, response, () -> {
                BucketAdvanceMessage message = request.attr(MessageTypes.BUCKETADVANCE).get();
                Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), request.getSession());
                QueryContext ctx = getQueryContextAndValidate(tr, request);
                List<Versionstamp> versionstamps;
                if (BucketOperation.UPDATE.equals(operation)) {
                    versionstamps = service.getQueryExecutor().update(tr, ctx);
                } else if (BucketOperation.DELETE.equals(operation)) {
                    versionstamps = service.getQueryExecutor().delete(tr, ctx);
                } else {
                    throw new IllegalArgumentException("Unsupported operation: " + operation);
                }

                TransactionUtils.addPostCommitHook(new QueryContextCommitHook(ctx), request.getSession());
                TransactionUtils.commitIfAutoCommitEnabled(tr, request.getSession());
                return new BucketVersionstampArrayResponse(message.getCursorId(), versionstamps);
            }, (readResponse) -> {
                RESPVersion protoVer = request.getSession().protocolVersion();
                if (protoVer.equals(RESPVersion.RESP3)) {
                    resp3VersionstampArrayResponse(response, readResponse);
                } else if (protoVer.equals(RESPVersion.RESP2)) {
                    resp2VersionstampArrayResponse(response, readResponse);
                } else {
                    throw new KronotopException("Unknown protocol version " + protoVer.getValue());
                }
            });
        }
    }
}
