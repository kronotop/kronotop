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
        DropParameters parameters = new DropParameters(request.getParams());
        runAsync(context, response, () -> {
            if (parameters.index.equals(PrimaryIndex.NAME)) {
                throw new IllegalArgumentException("Cannot drop the primary index");
            }
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, request.getSession(), parameters.bucket);
                VectorIndex vectorIndex = metadata.vectorIndexes().getIndexByName(parameters.index, IndexSelectionPolicy.ALL);
                if (vectorIndex != null) {
                    VectorIndexUtil.drop(tx, metadata, parameters.index);
                } else {
                    CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(parameters.index, IndexSelectionPolicy.ALL);
                    if (compoundIndex != null) {
                        CompoundIndexUtil.drop(tx, metadata, parameters.index);
                    } else {
                        SingleFieldIndexUtil.drop(tx, metadata, parameters.index);
                    }
                }
                tr.commit().join();
            }
        }, response::writeOK);
    }

    private static class DropParameters {
        private final String bucket;
        private final String index;

        DropParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 3) {
                throw new KronotopException("wrong number of parameters");
            }
            bucket = ProtocolMessageUtil.readAsString(params.get(1));
            index = ProtocolMessageUtil.readAsString(params.get(2));
        }
    }
}
