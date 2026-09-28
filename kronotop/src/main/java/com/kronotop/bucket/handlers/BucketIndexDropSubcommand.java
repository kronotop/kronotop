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
import com.kronotop.KronotopException;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.*;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.namespace.NamespaceUtil;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.transaction.TransactionUtil;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

import static com.kronotop.AsyncCommandExecutor.runAsync;

public class BucketIndexDropSubcommand implements SubcommandHandler {
    private final Context context;

    public BucketIndexDropSubcommand(Context context) {
        this.context = context;
    }

    @Override
    public void execute(Request request, Response response) {
        DropArguments arguments = new DropArguments(request.getArguments());
        runAsync(context, response, () -> {
            if (arguments.index.equals(PrimaryIndex.NAME)) {
                throw new IllegalArgumentException("Cannot drop the primary index");
            }
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                String namespace = NamespaceUtil.resolve(request.getSession(), arguments.namespace);
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, namespace, arguments.bucket);
                VectorIndex vectorIndex = metadata.vectorIndexes().getIndexByName(arguments.index, IndexSelectionPolicy.ALL);
                if (vectorIndex != null) {
                    VectorIndexUtil.drop(tx, metadata, arguments.index);
                } else {
                    CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(arguments.index, IndexSelectionPolicy.ALL);
                    if (compoundIndex != null) {
                        CompoundIndexUtil.drop(tx, metadata, arguments.index);
                    } else {
                        SingleFieldIndexUtil.drop(tx, metadata, arguments.index);
                    }
                }
                tr.commit().join();
            }
        }, response::writeOK);
    }

    private static class DropArguments {
        private final String bucket;
        private final String index;
        private final String namespace;

        DropArguments(ArrayList<ByteBuf> args) {
            if (args.size() < 3 || args.size() > 5) {
                throw new KronotopException("wrong number of arguments");
            }
            bucket = ProtocolMessageUtil.readAsString(args.get(1));
            index = ProtocolMessageUtil.readAsString(args.get(2));
            namespace = ProtocolMessageUtil.readTrailingNamespace(args, 3);
        }
    }
}
