/*
 * Copyright (c) 2023-2024 Kronotop
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

import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.directory.DirectoryAlreadyExistsException;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.kronotop.cluster.membership.MembershipService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.common.KronotopException;
import com.kronotop.redis.server.SubcommandExecutor;
import com.kronotop.server.Request;
import com.kronotop.server.Response;

import java.util.List;
import java.util.concurrent.CompletionException;

class InitializeClusterSubcommand extends BaseSubCommand implements SubcommandExecutor {

    InitializeClusterSubcommand(MembershipService service) {
        super(service);
    }

    private void initializeRedisSection(Transaction tr, DirectorySubspace subspace) {
        int numberOfRedisShards = service.getContext().getConfig().getInt("redis.shards");
        String kind = ShardKind.REDIS.toString().toLowerCase();
        for (int i = 0; i < numberOfRedisShards; i++) {
            subspace.create(tr, List.of("shards", kind, Integer.toString(i))).join();
        }
    }

    @Override
    public void execute(Request request, Response response) {
        // [kronotop, development, metadata, shards, redis, 0 ]
        // [kronotop, development, metadata, shards, redis, 1 ]
        // [kronotop, development, metadata, shards, redis, 2 ]
        // [kronotop, development, metadata, shards, redis, 3 ]
        DirectorySubspace subspace = createOrOpenClusterMetadataSubspace();
        try (Transaction tr = service.getContext().getFoundationDB().createTransaction()) {
            initializeRedisSection(tr, subspace);
            tr.commit().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                throw new KronotopException("cluster has already been initialized");
            }
        }
        response.writeOK();
    }
}
