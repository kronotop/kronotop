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
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
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
 * {"field_name": {"bson_type": "type", "multi_key": true/false, "unique": true/false, "name": "optional_name"}}
 * </pre>
 *
 * <p>{@code unique} is optional and defaults to {@code false}.</p>
 *
 * <p>When {@code multi_key} is {@code true}, each array element gets its own index entry, so a
 * query matches a document if any element matches. A document can then have many entries, so
 * result order is undefined and the {@code reverse} option is unpredictable on that field. Only
 * elements of the given {@code bson_type} are indexed.</p>
 */
class BucketIndexCreateSubcommand implements SubcommandHandler {
    private final Context context;

    BucketIndexCreateSubcommand(Context context) {
        this.context = context;
    }

    @Override
    public void execute(Request request, Response response) {
        CreateArguments arguments = new CreateArguments(request.getArguments());
        runAsync(context, response, () -> {

            Retry retry = RetryMethods.retry(RetryMethods.TRANSACTION);
            retry.executeRunnable(() -> {
                try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                    TransactionalContext tx = new TransactionalContext(context, tr);
                    String namespace = NamespaceUtil.resolve(request.getSession(), arguments.getNamespace());
                    BucketMetadata metadata = BucketMetadataUtil.reload(context, tr, namespace, arguments.getBucket());
                    IndexCreationHelper.createIndexes(tx, metadata, arguments.getPayload(), IndexStatus.WAITING);
                    tr.commit().join();
                }
            });
        }, response::writeOK);
    }

    static class CreateArguments {
        private final ArrayList<ByteBuf> args;

        private String bucket;
        private IndexSchemaPayload payload;
        private String namespace;

        CreateArguments(ArrayList<ByteBuf> args) {
            this.args = args;
            parse();
        }

        private void parse() {
            if (args.size() < 3 || args.size() > 5) {
                throw new IllegalArgumentException("wrong number of arguments");
            }
            bucket = ProtocolMessageUtil.readAsString(args.get(1));
            payload = IndexCreationHelper.deserializeAndValidate(ProtocolMessageUtil.readAsByteArray(args.get(2)));
            namespace = ProtocolMessageUtil.readTrailingNamespace(args, 3);
        }

        public IndexSchemaPayload getPayload() {
            return payload;
        }

        public String getBucket() {
            return bucket;
        }

        public String getNamespace() {
            return namespace;
        }
    }
}
