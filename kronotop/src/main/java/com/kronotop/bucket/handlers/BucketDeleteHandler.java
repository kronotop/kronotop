/*
 * Copyright (c) 2023-2026 Burak Sezer
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
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketObjectIdArrayResponse;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.BucketDeleteMessage;
import com.kronotop.bucket.pipeline.QueryContext;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.transaction.TransactionUtil;
import org.bson.types.ObjectId;

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
            Transaction tr = TransactionUtil.getOrCreateTransaction(service.getContext(), session);
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, session, message.getBucket());

            if (!metadata.vectorIndexes().isEmpty()) {
                checkBucketOwnership(metadata);
                checkVectorIndexRecoveryState(metadata);
            }

            QueryContext ctx = buildQueryContext(request, metadata, message.getQuery(), message.getArguments());
            int cursorId = session.nextCursorId();
            session.attr(SessionAttributes.BUCKET_DELETE_QUERY_CONTEXTS).get().put(cursorId, ctx);

            List<ObjectId> objectIds = service.getQueryExecutor().delete(tr, ctx);

            TransactionUtil.commitIfAutoCommitEnabled(tr, request.getSession());
            return new BucketObjectIdArrayResponse(cursorId, objectIds);
        }, (objectIdResponse) -> {
            ObjectIdFormat format = request.getSession().attr(SessionAttributes.OBJECT_ID_FORMAT).get();
            RESPVersion protoVer = request.getSession().protocolVersion();
            if (protoVer.equals(RESPVersion.RESP3)) {
                resp3ObjectIdArrayResponse(response, format, objectIdResponse);
            } else if (protoVer.equals(RESPVersion.RESP2)) {
                resp2ObjectIdArrayResponse(response, format, objectIdResponse);
            } else {
                throw new KronotopException("Unknown protocol version " + protoVer.getValue());
            }
        });
    }
}
