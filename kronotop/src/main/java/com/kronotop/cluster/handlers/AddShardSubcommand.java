/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.cluster.handlers;

import com.apple.foundationdb.FDBException;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.Context;
import com.kronotop.KronotopException;
import com.kronotop.cluster.ClusterConstants;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.ShardUtil;
import com.kronotop.cluster.handlers.protocol.KrAdminMessage;
import com.kronotop.internal.DirectorySubspaceCache;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.WrongNumberOfArgumentsException;

import java.nio.ByteBuffer;
import java.util.concurrent.CompletionException;

import static com.kronotop.AsyncCommandExecutor.supplyAsync;

class AddShardSubcommand extends BaseKrAdminSubcommandHandler implements SubcommandHandler {
    private final Context context;

    AddShardSubcommand(RoutingService service) {
        this.context = service.getContext();
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        if (request.getArguments().size() != 1) {
            throw new WrongNumberOfArgumentsException(KrAdminMessage.COMMAND, KrAdminSubcommand.ADD_SHARD);
        }
        supplyAsync(context, response, () -> {
            DirectorySubspace subspace = context.getDirectorySubspaceCache().get(DirectorySubspaceCache.Key.CLUSTER_METADATA);
            byte[] key = subspace.pack(Tuple.from(ClusterConstants.SHARD_ID_COUNTER_KEY));
            try (Transaction tr = context.getTransactionManager().create()) {
                byte[] data = tr.get(key).join();
                if (data == null) {
                    throw new IllegalStateException("Shard id counter has not been set, cluster state may be corrupt");
                }
                ByteBuffer buffer = ByteBuffer.wrap(data);
                int counter = buffer.getInt();
                int nextShardId = counter + 1;
                ShardUtil.createShard(context, tr, nextShardId);

                buffer.clear();
                buffer.putInt(nextShardId);
                tr.set(key, buffer.array());
                tr.commit().join();

                return nextShardId;
            } catch (CompletionException exp) {
                // 1020 -> not_committed - Transaction not committed due to conflict with another transaction
                if (exp.getCause() instanceof FDBException ex && ex.getCode() == 1020) {
                    throw new KronotopException("Another shard is being added at the same time");
                }
                throw exp;
            }
        }, shardId -> response.writeInteger(shardId));
    }
}
