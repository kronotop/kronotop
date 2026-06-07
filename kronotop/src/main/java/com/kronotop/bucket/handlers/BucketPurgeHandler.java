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
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.BucketMetadataVersionBarrier;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.handlers.protocol.BucketPurgeMessage;
import com.kronotop.bucket.index.maintenance.IndexTaskUtil;
import com.kronotop.journal.JournalName;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MaximumParameterCount;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.transaction.TransactionUtil;
import com.kronotop.volume.PrefixUtil;

import java.time.Duration;

import static com.kronotop.AsyncCommandExecutor.runAsync;

@Command(BucketPurgeMessage.COMMAND)
@MaximumParameterCount(BucketPurgeMessage.MAXIMUM_PARAMETER_COUNT)
@MinimumParameterCount(BucketPurgeMessage.MINIMUM_PARAMETER_COUNT)
public class BucketPurgeHandler extends AbstractBucketHandler {
    public BucketPurgeHandler(BucketService service) {
        super(service);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.BUCKETPURGE).set(new BucketPurgeMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        runAsync(context, response, () -> {
            String namespace = request.getSession().attr(SessionAttributes.CURRENT_NAMESPACE).get();
            BucketPurgeMessage message = request.attr(MessageTypes.BUCKETPURGE).get();
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                BucketMetadata metadata = BucketMetadataUtil.forceOpen(context, tr, namespace, message.getBucket());
                if (!metadata.removed()) {
                    throw new KronotopException(String.format("Bucket '%s' is not removed", message.getBucket()));
                }
                BucketMetadataVersionBarrier barrier = new BucketMetadataVersionBarrier(context, metadata);
                barrier.await(metadata.version(), 20, Duration.ofMillis(250)); // 5000 milliseconds
            }

            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                BucketMetadata metadata = BucketMetadataUtil.forceOpen(context, tr, namespace, message.getBucket());

                // Unregister prefix and publish as disused before purge
                byte[] prefixPointer = BucketMetadataUtil.prefixBindingKey(metadata.pointerSubspace());
                PrefixUtil.unregister(context, tr, prefixPointer, metadata.prefix());
                context.getJournal().getPublisher().publish(tr, JournalName.DISUSED_PREFIXES, metadata.prefix().asBytes());

                IndexTaskUtil.clearBucketTasks(tx, metadata);
                BucketMetadataUtil.purge(tx, namespace, message.getBucket());
                tr.commit().join();

                // Invalidate cache after successful purge to prevent stale metadata on recreation
                context.getBucketMetadataCache().invalidate(namespace, message.getBucket());
            }
        }, response::writeOK);
    }
}