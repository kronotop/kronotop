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
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.cluster.membership.MembershipConstants;
import com.kronotop.cluster.membership.impl.BasicMembershipService;
import com.kronotop.common.KronotopException;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;

import java.util.concurrent.CompletionException;

class InitializeClusterSubcommand extends BaseSubCommand implements SubcommandHandler {

    InitializeClusterSubcommand(BasicMembershipService service) {
        super(service);
    }

    private void initializeRedisSection(Transaction tr, DirectorySubspace subspace) {
        int numberOfRedisShards = service.getContext().getConfig().getInt("redis.shards");
        for (int i = 0; i < numberOfRedisShards; i++) {
            KronotopDirectoryNode directory = KronotopDirectory.
                    kronotop().
                    cluster(service.getContext().getClusterName()).
                    metadata().
                    shards().
                    redis().
                    shard(1);
            subspace.create(tr, directory.excludeSubspace(subspace)).join();
        }
    }

    private void setClusterInitializedTrue(Transaction tr, DirectorySubspace subspace) {
        byte[] key = subspace.pack(Tuple.from(MembershipConstants.CLUSTER_INITIALIZED));
        tr.set(key, MembershipConstants.TRUE);
    }

    @Override
    public void execute(Request request, Response response) {
        DirectorySubspace subspace = createOrOpenClusterMetadataSubspace();
        try (Transaction tr = service.getContext().getFoundationDB().createTransaction()) {
            initializeRedisSection(tr, subspace);
            setClusterInitializedTrue(tr, subspace);
            tr.commit().join();
        } catch (CompletionException e) {
            if (e.getCause() instanceof DirectoryAlreadyExistsException) {
                throw new KronotopException("cluster has already been initialized");
            }
        }
        response.writeOK();
    }
}
