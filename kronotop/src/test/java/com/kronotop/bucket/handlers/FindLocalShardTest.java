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

package com.kronotop.bucket.handlers;

import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketService;
import com.kronotop.bucket.BucketShard;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.cluster.sharding.ShardStatus;
import com.kronotop.server.RejectException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class FindLocalShardTest extends BaseBucketHandlerTest {
    private AbstractBucketHandler handler;
    private BucketMetadata metadata;

    @BeforeEach
    void setUp() {
        BucketService service = context.getService(BucketService.NAME);
        handler = new BucketInsertHandler(service);
        createBucket(TEST_BUCKET, List.of(TEST_SHARD_ID), null);
        metadata = getBucketMetadata(TEST_BUCKET);
    }

    @Test
    void shouldReturnShardWhenMemberIsPrimary() {
        // Behavior: findLocalShard returns the local shard when this member is its primary.
        BucketShard shard = handler.findLocalShard(metadata);
        assertEquals(TEST_SHARD_ID, shard.id());
    }

    @Test
    void shouldRejectWhenMemberIsStandby() throws Exception {
        // Behavior: findLocalShard throws REJECT with the shard id and the address of the primary
        // when this member is a standby of the shard. The shard is still open locally.
        Member primary = createMemberWithEphemeralPort();
        Member standby = context.getMember();

        RoutingService routing = context.getService(RoutingService.NAME);
        setRoute(routing, ShardKind.BUCKET, TEST_SHARD_ID, new Route(primary, Set.of(standby), ShardStatus.READWRITE));

        // primary changed by setRoute method
        RejectException exception = assertThrows(RejectException.class, () -> handler.findLocalShard(metadata));
        assertEquals(TEST_SHARD_ID + " " + primary.getExternalAddress(), exception.getMessage());
    }
}
