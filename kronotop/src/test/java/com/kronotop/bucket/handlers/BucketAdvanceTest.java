/*
 * Copyright (c) 2023-2025 Burak Sezer
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

import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.commandbuilder.kronotop.BucketQueryArgs;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

public class BucketAdvanceTest extends BaseBucketHandlerTest {

    private void appendIds(MapRedisMessage mapRedisMessage, List<String> result) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : mapRedisMessage.children().entrySet()) {
            SimpleStringRedisMessage id = (SimpleStringRedisMessage) entry.getKey();
            result.add(id.content());
        }
    }

    @Test
    void shouldAdvanceCursorPosition() {
        List<byte[]> documents = new ArrayList<>();
        for (int j = 0; j < 10; j++) {
            documents.add(DOCUMENT);
        }
        List<String> ids = insertManyDocumentsWithSingleTransaction(BUCKET_NAME, documents);
        String id = ids.getFirst();

        // Find the document in the middle
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        // BUCKET.QUERY
        List<String> result = new ArrayList<>();
        {
            ByteBuf buf = Unpooled.buffer();
            cmd.query(BUCKET_NAME, String.format("{_id: {$gte: \"%s\"}}", id), BucketQueryArgs.Builder.limit(2).shard(SHARD_ID)).encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;
            assertEquals(2, mapRedisMessage.children().size());
            appendIds(mapRedisMessage, result);
        }

        // BUCKET.ADVANCE
        while (true) {
            ByteBuf buf = Unpooled.buffer();
            cmd.advance().encode(buf);
            Object msg = runCommand(channel, buf);
            assertInstanceOf(MapRedisMessage.class, msg);
            MapRedisMessage mapRedisMessage = (MapRedisMessage) msg;
            if (mapRedisMessage.children().isEmpty()) {
                break;
            }
            assertEquals(2, mapRedisMessage.children().size());
            appendIds(mapRedisMessage, result);
        }

        assertThat(ids).usingRecursiveComparison().isEqualTo(result);
    }
}
