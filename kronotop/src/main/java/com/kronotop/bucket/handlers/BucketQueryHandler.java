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
import com.kronotop.bucket.*;
import com.kronotop.bucket.handlers.protocol.BucketQueryMessage;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

@Command(BucketQueryMessage.COMMAND)
@MinimumParameterCount(BucketQueryMessage.MINIMUM_PARAMETER_COUNT)
public class BucketQueryHandler extends BaseBucketHandler implements Handler {

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
            Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), session);
            BucketMetadata metadata = BucketMetadataUtil.createOrOpen(context, session, message.getBucket());

            QueryExecutorConfig config = new QueryExecutorConfig(metadata, message.getQuery());
            if (message.getArguments().limit() == 0) {
                config.setLimit(session.attr(SessionAttributes.LIMIT).get());
            } else {
                config.setLimit(message.getArguments().limit());
            }
            config.setReverse(message.getArguments().reverse());
            boolean pinReadVersion = session.attr(SessionAttributes.PIN_READ_VERSION).get();
            config.setPinReadVersion(pinReadVersion);
            tr.getReadVersion().thenAccept(config::setReadVersion);

            session.attr(SessionAttributes.BUCKET_QUERY_EXECUTOR_CONFIG).set(config);
            return QueryExecutor.execute(context, tr, config);
        }, (entries) -> {
            RESPVersion protoVer = request.getSession().protocolVersion();
            if (protoVer.equals(RESPVersion.RESP3)) {
                resp3Response(request, response, entries);
            } else if (protoVer.equals(RESPVersion.RESP2)) {
                resp2Response(request, response, entries);
            } else {
                throw new KronotopException("Unknown protocol version " + protoVer.getValue());
            }
        });
    }
}
