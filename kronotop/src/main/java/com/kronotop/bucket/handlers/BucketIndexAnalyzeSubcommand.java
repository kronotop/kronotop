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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.AsyncCommandExecutor;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.CompoundIndex;
import com.kronotop.bucket.index.CompoundIndexUtil;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.bucket.index.SingleFieldIndexUtil;
import com.kronotop.bucket.index.maintenance.IndexMaintenanceTask;
import com.kronotop.bucket.index.maintenance.IndexMaintenanceTaskKind;
import com.kronotop.bucket.index.maintenance.IndexTaskUtil;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.transaction.TransactionUtil;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class BucketIndexAnalyzeSubcommand implements SubcommandHandler {
    private final Context context;

    public BucketIndexAnalyzeSubcommand(Context context) {
        this.context = context;
    }

    private boolean isAnalyzeTask(Transaction tr, Versionstamp taskId) {
        for (int shardId : context.getShardRegistry().getShardIds(ShardKind.BUCKET)) {
            DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, shardId);
            byte[] base = TaskStorage.getDefinition(tr, taskSubspace, taskId);
            if (base == null) {
                continue;
            }
            IndexMaintenanceTask baseTask = JSONUtil.readValue(base, IndexMaintenanceTask.class);
            if (baseTask.getKind() == IndexMaintenanceTaskKind.ANALYZE) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void execute(Request request, Response response) {
        AnalyzeParameters parameters = new AnalyzeParameters(request.getParams());
        AsyncCommandExecutor.runAsync(context, response, () -> {
            String namespace = request.getSession().attr(SessionAttributes.CURRENT_NAMESPACE).get();
            try (Transaction tr = TransactionUtil.createInstrumentedTransaction(context)) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                List<Versionstamp> taskIds = IndexTaskUtil.getTaskIds(tx, namespace, parameters.bucket, parameters.index);
                for (Versionstamp taskId : taskIds) {
                    if (isAnalyzeTask(tr, taskId)) {
                        throw new KronotopException("An analyze task has already exist");
                    }
                }
                BucketMetadata metadata = BucketMetadataUtil.open(context, tr, namespace, parameters.bucket);
                CompoundIndex compoundIndex = metadata.compoundIndexes().getIndexByName(parameters.index, IndexSelectionPolicy.ALL);
                if (compoundIndex != null) {
                    CompoundIndexUtil.analyze(tx, metadata, parameters.index);
                } else {
                    SingleFieldIndexUtil.analyze(tx, metadata, parameters.index);
                }
                tr.commit().join();
            }
        }, response::writeOK);
    }

    private static class AnalyzeParameters {
        private final String bucket;
        private final String index;

        AnalyzeParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 3) {
                throw new KronotopException("wrong number of parameters");
            }
            bucket = ProtocolMessageUtil.readAsString(params.get(1));
            index = ProtocolMessageUtil.readAsString(params.get(2));
        }
    }
}
