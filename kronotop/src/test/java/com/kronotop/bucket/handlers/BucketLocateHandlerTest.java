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

import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.network.Address;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class BucketLocateHandlerTest extends BaseBucketHandlerTest {

    @BeforeEach
    public void createTestBucket() {
        createBucket(TEST_BUCKET);
    }

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

    private List<String> expectedAdvertise() {
        List<String> result = new ArrayList<>();
        for (Address address : context.getMember().getExternalAdvertise()) {
            result.add(address.toString());
        }
        return result;
    }

    private Object locate(String bucket) {
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.locate(bucket).encode(buf);
        return runCommand(channel, buf);
    }

    @Test
    void shouldLocateBucketWithSingleShard() {
        // Behavior: LOCATE returns a route table and a member table. The route table holds
        // [shardId, primaryMemberId, standbyMemberIds] and the member table maps the id to its advertise addresses.
        Object response = locate(TEST_BUCKET);
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(2, array.children().size());

        // route table
        assertInstanceOf(ArrayRedisMessage.class, array.children().get(0));
        ArrayRedisMessage routes = (ArrayRedisMessage) array.children().get(0);
        assertEquals(3, routes.children().size());

        assertInstanceOf(IntegerRedisMessage.class, routes.children().get(0));
        assertEquals(TEST_SHARD_ID, ((IntegerRedisMessage) routes.children().get(0)).value());

        String memberId = context.getMember().getId();
        assertEquals(memberId, readString(routes.children().get(1)));

        assertInstanceOf(ArrayRedisMessage.class, routes.children().get(2));
        assertEquals(0, ((ArrayRedisMessage) routes.children().get(2)).children().size());

        // member table
        assertInstanceOf(ArrayRedisMessage.class, array.children().get(1));
        ArrayRedisMessage members = (ArrayRedisMessage) array.children().get(1);
        assertEquals(2, members.children().size());

        assertEquals(memberId, readString(members.children().get(0)));
        assertEquals(expectedAdvertise(), readStrings(members.children().get(1)));
    }

    @Test
    void shouldLocateBucketWithMultipleShards() {
        // Behavior: LOCATE returns 3 route entries per shard, but lists a member only once even when it owns
        // more than one shard of the bucket.
        String bucketName = "multi-shard-bucket";
        createBucket(bucketName, List.of(0, 1), null);

        Object response = locate(bucketName);
        assertInstanceOf(ArrayRedisMessage.class, response);
        ArrayRedisMessage array = (ArrayRedisMessage) response;
        assertEquals(2, array.children().size());

        assertInstanceOf(ArrayRedisMessage.class, array.children().get(0));
        ArrayRedisMessage routes = (ArrayRedisMessage) array.children().get(0);
        assertEquals(6, routes.children().size());

        String memberId = context.getMember().getId();
        Set<Long> shardIds = new HashSet<>();
        for (int i = 0; i < routes.children().size(); i += 3) {
            assertInstanceOf(IntegerRedisMessage.class, routes.children().get(i));
            shardIds.add(((IntegerRedisMessage) routes.children().get(i)).value());

            assertEquals(memberId, readString(routes.children().get(i + 1)));

            assertInstanceOf(ArrayRedisMessage.class, routes.children().get(i + 2));
            assertEquals(0, ((ArrayRedisMessage) routes.children().get(i + 2)).children().size());
        }
        assertTrue(shardIds.contains(0L));
        assertTrue(shardIds.contains(1L));

        // the single member owns both shards, so it appears once
        assertInstanceOf(ArrayRedisMessage.class, array.children().get(1));
        ArrayRedisMessage members = (ArrayRedisMessage) array.children().get(1);
        assertEquals(2, members.children().size());
        assertEquals(memberId, readString(members.children().get(0)));
        assertEquals(expectedAdvertise(), readStrings(members.children().get(1)));
    }

    @Test
    void shouldReturnErrorForNonExistentBucket() {
        // Behavior: LOCATE on a bucket that doesn't exist returns an error.
        Object response = locate("non-existent-bucket");
        assertInstanceOf(ErrorRedisMessage.class, response);
        ErrorRedisMessage errorMessage = (ErrorRedisMessage) response;
        assertTrue(errorMessage.content().contains("No such bucket: 'non-existent-bucket'"));
    }
}
