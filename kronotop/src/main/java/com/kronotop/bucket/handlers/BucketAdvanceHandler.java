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
import com.kronotop.bucket.QueryExecutor;
import com.kronotop.bucket.QueryExecutorConfig;
import com.kronotop.bucket.handlers.protocol.BucketAdvanceMessage;
import com.kronotop.internal.TransactionUtils;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;

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
            Session session = request.getSession();
            QueryExecutorConfig config = session.attr(SessionAttributes.BUCKET_QUERY_EXECUTOR_CONFIG).get();
            if (Objects.isNull(config)) {
                throw new KronotopException("No previous query state found in this session");
            }
            Transaction tr = TransactionUtils.getOrCreateTransaction(service.getContext(), session);
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
