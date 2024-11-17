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

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.Transaction;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.directory.DirectoryLayer;
import com.apple.foundationdb.directory.DirectorySubspace;
import com.apple.foundationdb.tuple.Tuple;
import com.kronotop.cluster.MembershipService;
import com.kronotop.directory.KronotopDirectory;
import com.kronotop.directory.KronotopDirectoryNode;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;

import static com.kronotop.volume.Subspaces.REPLICATION_SLOT_SUBSPACE;

public class ListReplicationSlots extends BaseKrAdminSubcommandHandler implements SubcommandHandler {

    public ListReplicationSlots(MembershipService membership) {
        super(membership);
    }

    @Override
    public void execute(Request request, Response response) {
        // ShardKind
        // ShardId
        // SlotId
        // PrimaryMemberId
        // StandbyMemberId
        // ReplicationStage
        // LatestSegmentId
        // LatestVersionstampedKey

        // List Redis Shard replication slots
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            int shards = context.getConfig().getInt("redis.shards");
            for (int shardId = 0; shardId < shards; shardId++) {
                KronotopDirectoryNode directory = KronotopDirectory.
                        kronotop().
                        cluster(context.getClusterName()).
                        metadata().
                        volumes().
                        redis().
                        volume(Integer.toString(shardId));
                DirectorySubspace subspace = DirectoryLayer.getDefault().open(tr, directory.toList()).join();
                Tuple tuple = Tuple.from(REPLICATION_SLOT_SUBSPACE);
                Range range = Range.startsWith(subspace.pack(tuple));
                AsyncIterable<KeyValue> iterable = tr.getRange(range);
                for (KeyValue keyValue : iterable) {
                }
            }
        }
        response.writeOK();
    }
}
