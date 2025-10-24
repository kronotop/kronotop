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

import com.apple.foundationdb.MutationType;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.tuple.Tuple;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.IndexSubspaceMagic;
import com.kronotop.bucket.index.IndexUtil;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Objects;

import static com.kronotop.AsyncCommandExecutor.runAsync;
import static com.kronotop.bucket.handlers.AbstractBucketHandler.NULL_BYTES;

public class BucketIndexDropSubcommand implements SubcommandHandler {
    private final Context context;

    public BucketIndexDropSubcommand(Context context) {
        this.context = context;
    }

    @Override
    public void execute(Request request, Response response) {
        DropParameters parameters = new DropParameters(request.getParams());
        runAsync(context, response, () -> {
            if (parameters.index.equals(DefaultIndexDefinition.ID.name())) {
                throw new IllegalArgumentException("Cannot drop the primary index");
            }
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, request.getSession(), parameters.bucket);
                IndexUtil.drop(tx, metadata, parameters.index);

                // Create the task back pointer for easy inspection
                int userVersion = tx.getUserVersion();
                if (!Objects.equals(parameters.index, DefaultIndexDefinition.ID.name())) {
                    Collection<Index> indexes = metadata.indexes().getIndexes(IndexSelectionPolicy.ALL);
                    for (Index index : indexes) {
                        if (!index.definition().name().equals(parameters.index)) {
                            continue;
                        }
                        byte[] taskId = index.subspace().packWithVersionstamp(
                                Tuple.from(IndexSubspaceMagic.TASKS.getValue(), Versionstamp.incomplete(userVersion))
                        );
                        tr.mutate(MutationType.SET_VERSIONSTAMPED_KEY, taskId, NULL_BYTES);
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
