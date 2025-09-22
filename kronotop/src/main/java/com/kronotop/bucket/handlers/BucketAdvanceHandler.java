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
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.BucketVersionstampArrayResponse;
import com.kronotop.bucket.handlers.protocol.BucketAdvanceMessage;
import com.kronotop.bucket.handlers.protocol.BucketAdvanceSubcommand;
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
    private final EnumMap<BucketAdvanceSubcommand, SubcommandHandler> executors = new EnumMap<>(BucketAdvanceSubcommand.class);

    public BucketAdvanceHandler(BucketService service) {
        super(service);
        executors.put(BucketAdvanceSubcommand.QUERY, new QuerySubcommand());
        executors.put(BucketAdvanceSubcommand.UPDATE, new UpdateSubcommand());
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETADVANCE).set(new BucketAdvanceMessage(request));
    }

    private Map<Integer, QueryContext> findQueryContext(Session session, BucketAdvanceSubcommand subcommand) {
        return switch (subcommand) {
            case QUERY -> session.attr(SessionAttributes.BUCKET_READ_QUERY_CONTEXTS).get();
            case DELETE -> session.attr(SessionAttributes.BUCKET_DELETE_QUERY_CONTEXTS).get();
            case UPDATE -> session.attr(SessionAttributes.BUCKET_UPDATE_QUERY_CONTEXTS).get();
        };
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        BucketAdvanceMessage message = request.attr(MessageTypes.BUCKETADVANCE).get();
        SubcommandHandler executor = executors.get(message.getSubcommand());
        if (executor == null) {
            throw new UnknownSubcommandException(message.getSubcommand().toString());
        }
        executor.execute(request, response);
    }

    class QuerySubcommand implements SubcommandHandler {

        @Override
        public void execute(Request request, Response response) {
            supplyAsync(context, response, () -> {
                BucketAdvanceMessage message = request.attr(MessageTypes.BUCKETADVANCE).get();
                Session session = request.getSession();
                Map<Integer, QueryContext> contexts = findQueryContext(session, message.getSubcommand());
                QueryContext ctx = contexts.get(message.getCursorId());
                if (Objects.isNull(ctx)) {
                    throw new KronotopException("No previous query context found for '" +
                            message.getSubcommand().name().toLowerCase() + "' action with the given cursor id");
                }
                Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), session);
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

    class UpdateSubcommand implements SubcommandHandler {

        @Override
        public void execute(Request request, Response response) {
            supplyAsync(context, response, () -> {
                BucketAdvanceMessage message = request.attr(MessageTypes.BUCKETADVANCE).get();
                Session session = request.getSession();
                Map<Integer, QueryContext> contexts = findQueryContext(session, message.getSubcommand());
                QueryContext ctx = contexts.get(message.getCursorId());
                if (Objects.isNull(ctx)) {
                    throw new KronotopException("No previous query context found for '" +
                            message.getSubcommand().name().toLowerCase() + "' action with the given cursor id");
                }
                Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), session);
                List<Versionstamp> versionstamps = service.getQueryExecutor().update(tr, ctx);

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
