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

import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.BucketMetadata;
import com.kronotop.bucket.BucketMetadataUtil;
import com.kronotop.bucket.index.Index;
import com.kronotop.bucket.index.IndexSelectionPolicy;
import com.kronotop.commands.BucketCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.*;
import com.kronotop.transaction.TransactionUtil;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class BucketIndexStatisticsEventTest extends BaseIndexHandlerTest {

    @Test
    void shouldInvalidatePlanCacheAfterAnalyze() {
        // Behavior: ANALYZE completes → INDEX_STATISTICS_UPDATED_EVENT → plan cache invalidated

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);

        // Create index
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexCreate(TEST_BUCKET, "{\"age\": {\"bson_type\": \"int32\", \"name\": \"idx_age\"}}").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }

        // Wait for index readiness
        {
            BucketMetadata metadata = TransactionUtil.execute(context,
                    tr -> BucketMetadataUtil.reload(context, tr, TEST_NAMESPACE, TEST_BUCKET)
            );
            Index index = metadata.indexes().getIndex("age", IndexSelectionPolicy.ALL);
            waitForIndexReadiness(index.subspace());
        }

        context.getBucketMetadataCache().invalidate(TEST_NAMESPACE, TEST_BUCKET);

        // Insert documents
        List<byte[]> documents = List.of(
                BSONUtil.jsonToDocumentThenBytes("{\"age\": 25}"),
                BSONUtil.jsonToDocumentThenBytes("{\"age\": 30}")
        );
        insertDocumentsAndGetObjectIds(documents);

        switchProtocol(cmd, RESPVersion.RESP3);

        // Run QUERY to populate plan cache
        String query = "{\"age\": {\"$eq\": 25}}";
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(TEST_BUCKET, query).encode(buf);
            runCommand(channel, buf);
        }

        // Verify plan is cached
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.explain(TEST_BUCKET, query).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            assertEquals(true, getBooleanValue((MapRedisMessage) msg, "is_cached"));
        }

        // Run ANALYZE
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.indexAnalyze(TEST_BUCKET, "idx_age").encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            assertEquals(Response.OK, ((SimpleStringRedisMessage) msg).content());
        }

        // Wait until the plan cache is invalidated by the INDEX_STATISTICS_UPDATED_EVENT event
        await().atMost(Duration.ofSeconds(15)).until(() -> {
            ByteBuf buf = Unpooled.buffer();
            cmd.explain(TEST_BUCKET, query).encode(buf);
            Object msg = runCommand(channel, buf);
            if (!(msg instanceof MapRedisMessage mapMessage)) {
                return false;
            }
            Boolean isCached = getBooleanValue(mapMessage, "is_cached");
            return isCached != null && !isCached;
        });
    }

    private Boolean getBooleanValue(MapRedisMessage map, String key) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : map.children().entrySet()) {
            if (entry.getKey() instanceof FullBulkStringRedisMessage keyMsg
                    && keyMsg.content().toString(StandardCharsets.UTF_8).equals(key)) {
                if (entry.getValue() instanceof BooleanRedisMessage boolValue) {
                    return boolValue.value();
                }
            }
        }
        return null;
    }
}
