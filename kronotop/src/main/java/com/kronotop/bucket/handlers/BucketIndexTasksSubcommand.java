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
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.AsyncCommandExecutor;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.TransactionalContext;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.index.maintenance.IndexBuildingTaskState;
import com.kronotop.bucket.index.maintenance.IndexDropTaskState;
import com.kronotop.bucket.index.maintenance.IndexMaintenanceTask;
import com.kronotop.bucket.index.maintenance.IndexTaskUtil;
import com.kronotop.internal.JSONUtil;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.VersionstampUtil;
import com.kronotop.internal.task.TaskStorage;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SessionAttributes;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class BucketIndexTasksSubcommand implements SubcommandHandler {
    private final Context context;
    private final BucketService service;

    BucketIndexTasksSubcommand(Context context) {
        this.context = context;
        this.service = context.getService(BucketService.NAME);
    }

    private Map<RedisMessage, RedisMessage> scanTaskId(Transaction tr, Versionstamp taskId) {
        Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
        for (int shardId = 0; shardId < service.getNumberOfShards(); shardId++) {
            DirectorySubspace taskSubspace = IndexTaskUtil.openTasksSubspace(context, shardId);
            byte[] base = TaskStorage.getDefinition(tr, taskSubspace, taskId);
            if (base == null) {
                continue;
            }
            IndexMaintenanceTask baseTask = JSONUtil.readValue(base, IndexMaintenanceTask.class);
            switch (baseTask.getKind()) {
                case BUILD -> {
                    IndexBuildingTaskState state = IndexBuildingTaskState.load(tr, taskSubspace, taskId);
                    result.put(
                            new SimpleStringRedisMessage("kind"),
                            new SimpleStringRedisMessage(baseTask.getKind().name())
                    );
                    result.put(
                            new SimpleStringRedisMessage("cursor"),
                            new SimpleStringRedisMessage(VersionstampUtil.base32HexEncode(state.cursorVersionstamp()))
                    );
                    result.put(
                            new SimpleStringRedisMessage("highest"),
                            new SimpleStringRedisMessage(VersionstampUtil.base32HexEncode(state.highestVersionstamp()))
                    );
                    result.put(
                            new SimpleStringRedisMessage("status"),
                            new SimpleStringRedisMessage(state.status().name())
                    );
                    result.put(
                            new SimpleStringRedisMessage("error"),
                            new SimpleStringRedisMessage(state.error())
                    );
                }
                case DROP -> {
                    IndexDropTaskState state = IndexDropTaskState.load(tr, taskSubspace, taskId);
                    result.put(
                            new SimpleStringRedisMessage("kind"),
                            new SimpleStringRedisMessage(baseTask.getKind().name())
                    );
                    result.put(
                            new SimpleStringRedisMessage("status"),
                            new SimpleStringRedisMessage(state.status().name())
                    );
                    result.put(
                            new SimpleStringRedisMessage("error"),
                            new SimpleStringRedisMessage(state.error())
                    );
                }
                default -> throw new KronotopException("Unknown task kind: " + baseTask.getKind());
            }
        }
        return result;
    }

    @Override
    public void execute(Request request, Response response) {
        TasksParameters parameters = new TasksParameters(request.getParams());
        AsyncCommandExecutor.supplyAsync(context, response, () -> {
            Map<RedisMessage, RedisMessage> parent = new LinkedHashMap<>();
            String namespace = request.getSession().attr(SessionAttributes.CURRENT_NAMESPACE).get();
            try (Transaction tr = context.getFoundationDB().createTransaction()) {
                TransactionalContext tx = new TransactionalContext(context, tr);
                List<Versionstamp> taskIds = IndexTaskUtil.listTasks(tx, namespace, parameters.bucket, parameters.index);
                for (Versionstamp taskId : taskIds) {
                    Map<RedisMessage, RedisMessage> child = scanTaskId(tr, taskId);
                    parent.put(
                            new SimpleStringRedisMessage(VersionstampUtil.base32HexEncode(taskId)),
                            new MapRedisMessage(child)
                    );
                }
            }
            return parent;
        }, response::writeMap);
    }

    private static class TasksParameters {
        private final String bucket;
        private final String index;

        TasksParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 3) {
                throw new KronotopException("wrong number of parameters");
            }
            bucket = ProtocolMessageUtil.readAsString(params.get(1));
            index = ProtocolMessageUtil.readAsString(params.get(2));
        }
    }
}
