/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.bucket.handlers;

import com.apple.foundationdb.Transaction;
import com.kronotop.KronotopException;
import com.kronotop.bucket.*;
import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.handlers.protocol.BucketUpdateMessage;
import com.kronotop.bucket.pipeline.QueryContext;
import com.kronotop.bucket.pipeline.UpdateOptions;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.transaction.TransactionUtil;
import org.bson.BsonDocument;
import org.bson.types.ObjectId;

import java.util.List;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketUpdateMessage.COMMAND)
@MaximumParameterCount(BucketUpdateMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(BucketUpdateMessage.MINIMUM_PARAMETER_COUNT)
public class BucketUpdateHandler extends AbstractBucketHandler implements Handler {
    public BucketUpdateHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETUPDATE).set(new BucketUpdateMessage(request));
    }

    /**
     * Parses the input byte array into a Document object, assuming it is either JSON or BSON format.
     *
     * @param input the input byte array containing the data to be parsed
     * @return a Document object obtained by parsing the input byte array
     */
    private BsonDocument parseUpdateDocument(byte[] input) {
        if (BqlParser.isBSON(input)) {
            return BSONUtil.fromBson(input);
        }
        return BSONUtil.fromJson(input);
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketUpdateMessage message = request.attr(MessageTypes.BUCKETUPDATE).get();
            Session session = request.getSession();

            Transaction tr = TransactionUtil.getOrCreateTransaction(service.getContext(), session);
            BucketMetadata metadata = BucketMetadataUtil.open(context, tr, request.getSession(), message.getBucket());
            checkBucketOwnership(metadata);
            if (!metadata.vectorIndexes().isEmpty()) {
                checkVectorIndexRecoveryState(metadata);
            }

            BsonDocument updateDocument = parseUpdateDocument(message.getUpdate());
            UpdateOptions updateOptions = UpdateOptionsConverter.fromDocument(updateDocument);
            QueryContext ctx = buildQueryContext(
                    request,
                    metadata,
                    message.getQuery(),
                    message.getArguments(),
                    updateOptions,
                    false
            );

            int cursorId = session.nextCursorId();
            session.attr(SessionAttributes.BUCKET_UPDATE_QUERY_CONTEXTS).get().put(cursorId, ctx);

            List<ObjectId> objectIds = service.getQueryExecutor().update(tr, ctx);

            TransactionUtil.commitIfAutoCommitEnabled(tr, request.getSession());

            // Handle upsert: the ObjectId is available immediately
            if (ctx.upsertResult() != null) {
                objectIds = List.of(ctx.upsertResult().getObjectId());
            }

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
