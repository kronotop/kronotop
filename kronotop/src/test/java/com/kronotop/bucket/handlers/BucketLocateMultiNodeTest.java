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

import com.kronotop.cluster.Route;
import com.kronotop.cluster.RouteKind;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.commands.KrAdminCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class BucketLocateMultiNodeTest extends BaseBucketMultiNodeTest {

    @Test
    void shouldLocateBucketWithStandbys() {
        // Behavior: LOCATE returns standby addresses when a standby is configured for a shard.

        String node2Id = node2.getContext().getMember().getId();

        // Set node2 as a STANDBY for BUCKET shard 0 (owned by node1 as primary)
        KrAdminCommandBuilder<String, String> adminCmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            adminCmd.route("SET", RouteKind.STANDBY.name(), ShardKind.BUCKET.name(), 0, node2Id).encode(buf);
            Object raw = runCommand(node1.getChannel(), buf);
            if (raw instanceof ErrorRedisMessage err) {
                fail(err.content());
            }
            assertInstanceOf(SimpleStringRedisMessage.class, raw);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) raw).content());
        }

        // Wait for node1's routing table to reflect the standby
        await().atMost(5, TimeUnit.SECONDS).until(() -> {
            RoutingService routing = node1.getContext().getService(RoutingService.NAME);
            Route route = routing.findRoute(ShardKind.BUCKET, 0);
            return route != null && !route.standbys().isEmpty();
        });

        // Create a bucket on node1 with shard [0]
        String bucketName = "locate-standby-bucket";
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.create(bucketName, BucketCreateArgs.Builder.shards(List.of(0))).encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail(err.content());
            }
            assertInstanceOf(SimpleStringRedisMessage.class, response);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) response).content());
        }

        // Call LOCATE on node1
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.locate(bucketName).encode(buf);
            Object response = runCommand(node1.getChannel(), buf);
            if (response instanceof ErrorRedisMessage err) {
                fail("LOCATE failed: " + err.content());
            }
            assertInstanceOf(ArrayRedisMessage.class, response);
            ArrayRedisMessage array = (ArrayRedisMessage) response;
            assertEquals(3, array.children().size());

            // shard id
            assertInstanceOf(IntegerRedisMessage.class, array.children().get(0));
            assertEquals(0, ((IntegerRedisMessage) array.children().get(0)).value());

            // primary address (host:port)
            assertInstanceOf(FullBulkStringRedisMessage.class, array.children().get(1));
            String primaryAddress = ((FullBulkStringRedisMessage) array.children().get(1)).content().toString(StandardCharsets.US_ASCII);
            String expectedPrimary = String.format("%s:%s",
                    node1.getContext().getMember().getExternalAddress().getHost(),
                    node1.getContext().getMember().getExternalAddress().getPort());
            assertEquals(expectedPrimary, primaryAddress);

            // standbys array with 1 entry (node2)
            assertInstanceOf(ArrayRedisMessage.class, array.children().get(2));
            ArrayRedisMessage standbys = (ArrayRedisMessage) array.children().get(2);
            assertEquals(1, standbys.children().size());

            assertInstanceOf(FullBulkStringRedisMessage.class, standbys.children().get(0));
            String standbyAddress = ((FullBulkStringRedisMessage) standbys.children().get(0)).content().toString(StandardCharsets.US_ASCII);
            String expectedStandby = String.format("%s:%s",
                    node2.getContext().getMember().getExternalAddress().getHost(),
                    node2.getContext().getMember().getExternalAddress().getPort());
            assertEquals(expectedStandby, standbyAddress);
        }
    }
}
