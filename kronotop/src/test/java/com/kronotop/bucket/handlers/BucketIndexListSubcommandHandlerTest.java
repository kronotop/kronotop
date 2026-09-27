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

import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.ErrorRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class BucketIndexListSubcommandHandlerTest extends BaseBucketHandlerTest {

    @BeforeEach
    void beforeEach() {
        createBucket(TEST_BUCKET);
    }

    @Test
    void shouldReturnErrorIfBucketDoesNotExist() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        cmd.indexList("non-existing-bucket").encode(buf);
        Object msg = runCommand(channel, buf);
        ErrorRedisMessage actualMessage = (ErrorRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals("NOSUCHBUCKET No such bucket: 'non-existing-bucket'", actualMessage.content());
    }

    @Test
    void shouldListIndexes() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"selector\": {\"bson_type\": \"int32\"}, \"username\": {\"bson_type\": \"string\"}}").encode(buf);
            runCommand(channel, buf);
        }

        List<String> expectedNames = List.of(
                "primary-index",
                "selector:selector.bsonType:INT32",
                "selector:username.bsonType:STRING"
        );

        ByteBuf buf = Unpooled.buffer();
        cmd.indexList(TEST_BUCKET).encode(buf);
        Object msg = runCommand(channel, buf);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertNotNull(actualMessage);
        assertEquals(3, actualMessage.children().size());

        List<String> names = new ArrayList<>();
        for (RedisMessage message : actualMessage.children()) {
            FullBulkStringRedisMessage actual = (FullBulkStringRedisMessage) message;
            names.add(actual.content().toString(StandardCharsets.UTF_8));
        }
        assertEquals(expectedNames, names);
    }

    @Test
    void shouldListIndexesInGivenNamespace() {
        // Behavior: NAMESPACE lists the indexes of the bucket in the given namespace. Without it,
        // the bucket is looked up in the session's current namespace.
        String otherNamespace = newNamespaceWithBucket("only-in-other", List.of());

        assertEquals(List.of("primary-index"), readBulkStrings(run(bucketCmd.indexList("only-in-other", otherNamespace))));
        assertErrorReply("NOSUCHBUCKET No such bucket: 'only-in-other'", run(bucketCmd.indexList("only-in-other")));
    }

    @Test
    void shouldRejectListWhenNamespaceDoesNotExist() {
        // Behavior: NAMESPACE with an unknown namespace returns NOSUCHNAMESPACE.
        assertNoSuchNamespace(run(bucketCmd.indexList(TEST_BUCKET, MISSING_NAMESPACE)));
    }

    @Test
    void shouldRejectInvalidNamespaceArgument() {
        // Behavior: BUCKET.INDEX LIST rejects NAMESPACE without a value, an unknown keyword and an argument after the namespace.
        assertErrorReply("ERR NAMESPACE argument must be followed by a namespace",
                runRaw(channel, BucketCommandBuilder.CommandType.BUCKET_INDEX, List.of("LIST", "test-bucket", "NAMESPACE")));
        assertErrorReply("ERR Unknown 'BOGUS' argument",
                runRaw(channel, BucketCommandBuilder.CommandType.BUCKET_INDEX, List.of("LIST", "test-bucket", "BOGUS")));
        assertErrorReply("ERR wrong number of parameters",
                runRaw(channel, BucketCommandBuilder.CommandType.BUCKET_INDEX, List.of("LIST", "test-bucket", "NAMESPACE", "ns", "EXTRA")));
    }
}
