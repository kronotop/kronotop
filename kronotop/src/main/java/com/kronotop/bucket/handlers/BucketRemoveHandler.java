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
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.BucketRemoveMessage;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;

import static com.kronotop.AsyncCommandExecutor.runAsync;

@Command(BucketRemoveMessage.COMMAND)
@MaximumParameterCount(BucketRemoveMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(BucketRemoveMessage.MINIMUM_PARAMETER_COUNT)
public class BucketRemoveHandler extends AbstractBucketHandler {
    public BucketRemoveHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETREMOVE).set(new BucketRemoveMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        runAsync(context, response, () -> {
            String namespace = request.getSession().attr(SessionAttributes.CURRENT_NAMESPACE).get();
            BucketRemoveMessage message = request.attr(MessageTypes.BUCKETREMOVE).get();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                BucketMetadata metadata = BucketMetadataUtil.openUncached(context, tr, namespace, message.getBucket());
                TransactionalContext tx = new TransactionalContext(context, tr);
                BucketMetadataUtil.setRemoved(tx, metadata);
                tr.commit().join();
            }
        }, response::writeOK);
    }
}
