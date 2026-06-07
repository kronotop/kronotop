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
import com.kronotop.Context;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.RetryMethods;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.transaction.TransactionUtil;
import io.github.resilience4j.retry.Retry;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

import static com.kronotop.AsyncCommandExecutor.runAsync;

/**
 * Handles the BUCKET.INDEX CREATE subcommand to create indexes on bucket fields.
 *
 * <p>Index schema format:
 * <pre>
 * {"field_name": {"bson_type": "type", "multi_key": true/false, "name": "optional_name"}}
 * </pre>
 *
 * <h3>Multi-key Index Behavior and Limitations</h3>
 *
 * <p>When {@code multi_key} is set to {@code true}, the index is designed for array fields.
 * Each element in the array creates a separate index entry, allowing queries to match
 * documents where any array element satisfies the condition.</p>
 *
 * <p><b>Limitations of multi-key indexes:</b></p>
 * <ul>
 *   <li><b>Undefined ordering:</b> Result ordering is undefined with multi-key indexes.
 *       Since each document can have multiple index entries (one per array element),
 *       the order in which documents are returned cannot be guaranteed. The {@code reverse}
 *       query option has unpredictable behavior on multi-key indexed fields.</li>
 *   <li><b>Index size:</b> Multi-key indexes can be significantly larger than regular indexes
 *       because each array element creates a separate index entry.</li>
 *   <li><b>Type matching:</b> Only array elements matching the specified {@code bson_type}
 *       are indexed. Mixed-type arrays will only have matching elements indexed.</li>
 * </ul>
 */
class BucketIndexCreateSubcommand implements SubcommandHandler {
    private final Context context;

    BucketIndexCreateSubcommand(Context context) {
        this.context = context;
    }

    @Override
    public void execute(Request request, Response response) {
        CreateParameters parameters = new CreateParameters(request.getParams());
        runAsync(context, response, () -> {

            Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
            retry.executeRunnable(() -> {
                try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                    TransactionalContext tx = new TransactionalContext(context, tr);
                    String namespace = request.getSession().attr(SessionAttributes.CURRENT_NAMESPACE).get();
                    BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, namespace, parameters.getBucket());
                    IndexCreationHelper.createIndexes(tx, metadata, parameters.getPayload(), IndexStatus.WAITING);
                    tr.commit().join();
                }
            });
        }, response::writeOK);
    }

    static class CreateParameters {
        private final ArrayList<ByteBuf> params;

        private String bucket;
        private IndexSchemaPayload payload;

        CreateParameters(ArrayList<ByteBuf> params) {
            this.params = params;
            parse();
        }

        private void parse() {
            if (params.size() != 3) {
                throw new IllegalArgumentException("wrong number of parameters");
            }
            bucket = ProtocolMessageUtil.readAsString(params.get(1));
            payload = IndexCreationHelper.deserializeAndValidate(ProtocolMessageUtil.readAsByteArray(params.get(2)));
        }

        public IndexSchemaPayload getPayload() {
            return payload;
        }

        public String getBucket() {
            return bucket;
        }

    }
}
