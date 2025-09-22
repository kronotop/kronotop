/*
 * Copyright (c) 2023-2025 Burak Sezer
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
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.BucketVersionstampArrayResponse;
import com.kronotop.bucket.handlers.protocol.BucketUpdateMessage;
import com.kronotop.bucket.pipeline.QueryContext;
import com.kronotop.bucket.pipeline.UpdateOptions;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import org.bson.BsonArray;
import org.bson.BsonString;
import org.bson.BsonValue;
import org.bson.Document;

import java.util.ArrayList;
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

    @Override
    public void execute(Request request, Response response) throws Exception {
        supplyAsync(context, response, () -> {
            BucketUpdateMessage message = request.attr(MessageTypes.BUCKETUPDATE).get();

            Session session = request.getSession();

            InputType inputType = getInputType(request);
            Document updateDoc = parseDocument(inputType, message.getUpdate());
            UpdateOptions updateOptions = convertDocumentToUpdateOptions(updateDoc);

            QueryContext ctx = buildQueryContext(
                    request,
                    message.getBucket(),
                    message.getQuery(),
                    message.getArguments(),
                    updateOptions
            );

            int cursorId = session.nextCursorId();
            session.attr(SessionAttributes.BUCKET_UPDATE_QUERY_CONTEXTS).get().put(cursorId, ctx);

            Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), session);
            List<Versionstamp> versionstamps = service.getQueryExecutor().update(tr, ctx);

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

    private UpdateOptions convertDocumentToUpdateOptions(Document updateDoc) {
        UpdateOptions.Builder builder = UpdateOptions.builder();

        for (String key : updateDoc.keySet()) {
            Object value = updateDoc.get(key);

            switch (key.toLowerCase()) {
                case UpdateOptions.SET -> {
                    if (value instanceof Document setDoc) {
                        // Handle Document format: Document{{likes=2}}
                        for (String field : setDoc.keySet()) {
                            Object fieldValue = setDoc.get(field);
                            if (fieldValue instanceof BsonValue bsonValue) {
                                builder.set(field, bsonValue);
                            } else {
                                builder.set(field, BSONUtil.toBsonValue(fieldValue));
                            }
                        }
                    }
                }
                case UpdateOptions.UNSET -> {
                    if (value instanceof ArrayList<?> unsetKeys) {
                        // Handle ArrayList format: [field1, field2]
                        for (Object unsetKey : unsetKeys) {
                            if (!(unsetKey instanceof String)) {
                                throw new IllegalArgumentException("unset key must be a string");
                            }
                            builder.unset((String) unsetKey);
                        }
                    } else if (value instanceof BsonArray bsonArray) {
                        // Handle BsonArray format: BsonArray{values=[BsonString{value='field1'}]}
                        for (BsonValue bsonValue : bsonArray) {
                            if (bsonValue instanceof BsonString bsonString) {
                                builder.unset(bsonString.getValue());
                            } else {
                                throw new IllegalArgumentException("unset key must be a string, got: " + bsonValue.getClass().getSimpleName());
                            }
                        }
                    }
                }
            }
        }

        return builder.build();
    }
}
