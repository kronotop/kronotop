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

import com.kronotop.cluster.Member;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RouteKind;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.commands.BucketCreateArgs;
import com.kronotop.commands.KrAdminCommandBuilder;
import com.kronotop.network.Address;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.*;

class BucketLocateMultiNodeTest extends BaseBucketMultiNodeTest {

    private String readString(RedisMessage message) {
        assertInstanceOf(FullBulkStringRedisMessage.class, message);
        return ((FullBulkStringRedisMessage) message).content().toString(StandardCharsets.UTF_8);
    }

    private List<String> readStrings(RedisMessage message) {
        assertInstanceOf(ArrayRedisMessage.class, message);
        List<String> result = new ArrayList<>();
        for (RedisMessage child : ((ArrayRedisMessage) message).children()) {
            result.add(readString(child));
        }
        return result;
    }

    private List<String> advertiseOf(Member member) {
        List<String> result = new ArrayList<>();
        for (Address address : member.getExternalAdvertise()) {
            result.add(address.toString());
        }
        return result;
    }

    @Test
    void shouldLocateBucketWithStandbys() {
        // Behavior: LOCATE reports the standby member id in the route table and its advertise addresses
        // in the member table when a standby is configured for a shard.

        Member node1Member = node1.getContext().getMember();
        Member node2Member = node2.getContext().getMember();

        // Set node2 as a STANDBY for BUCKET shard 0 (owned by node1 as primary)
        KrAdminCommandBuilder<String, String> adminCmd = new KrAdminCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            adminCmd.route("SET", RouteKind.STANDBY.name(), ShardKind.BUCKET.name(), 0, node2Member.getId()).encode(buf);
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
            assertEquals(2, array.children().size());

            // route table
            assertInstanceOf(ArrayRedisMessage.class, array.children().get(0));
            ArrayRedisMessage routes = (ArrayRedisMessage) array.children().get(0);
            assertEquals(3, routes.children().size());

            assertInstanceOf(IntegerRedisMessage.class, routes.children().get(0));
            assertEquals(0, ((IntegerRedisMessage) routes.children().get(0)).value());

            assertEquals(node1Member.getId(), readString(routes.children().get(1)));
            assertEquals(List.of(node2Member.getId()), readStrings(routes.children().get(2)));

            // member table: primary first, then its standby
            assertInstanceOf(ArrayRedisMessage.class, array.children().get(1));
            ArrayRedisMessage members = (ArrayRedisMessage) array.children().get(1);
            assertEquals(4, members.children().size());

            assertEquals(node1Member.getId(), readString(members.children().get(0)));
            assertEquals(advertiseOf(node1Member), readStrings(members.children().get(1)));

            assertEquals(node2Member.getId(), readString(members.children().get(2)));
            assertEquals(advertiseOf(node2Member), readStrings(members.children().get(3)));
        }
    }
}
