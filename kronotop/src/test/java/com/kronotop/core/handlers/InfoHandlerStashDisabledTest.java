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

package com.kronotop.core.handlers;

import com.kronotop.BaseHandlerTest;
import com.kronotop.cluster.sharding.ShardKind;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class InfoHandlerStashDisabledTest extends BaseHandlerTest {

    @Override
    protected String getConfigFileName() {
        return "test-stash-disabled.conf";
    }

    @Test
    void shouldOmitClientsInMultiWhenStashDisabled() {
        // Behavior: with stash.enabled=false the Clients section has no clients_in_multi
        // field because MULTI is a stash feature
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$4\r\nINFO\r\n$7\r\nclients\r\n".getBytes(StandardCharsets.US_ASCII));

        Object response = runCommand(getChannel(), buf);
        assertInstanceOf(FullBulkStringRedisMessage.class, response);
        String info = ((FullBulkStringRedisMessage) response).content().toString(StandardCharsets.US_ASCII);

        assertTrue(info.contains("connected_clients:"));
        assertFalse(info.contains("clients_in_multi:"));
    }

    @Test
    void shouldOmitStashFieldsWhenStashDisabled() {
        // Behavior: with stash.enabled=false the Kronotop section has no stash_shards
        // field and stash shards are not counted in primary_shards
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes("*2\r\n$4\r\nINFO\r\n$8\r\nkronotop\r\n".getBytes(StandardCharsets.US_ASCII));

        Object response = runCommand(getChannel(), buf);
        assertInstanceOf(FullBulkStringRedisMessage.class, response);
        String info = ((FullBulkStringRedisMessage) response).content().toString(StandardCharsets.US_ASCII);

        int bucketShards = context.getShardRegistry().getShardIds(ShardKind.BUCKET).size();
        assertFalse(info.contains("stash_shards:"));
        assertTrue(info.contains("bucket_shards:" + bucketShards + "\r\n"));
        assertTrue(info.contains("primary_shards:" + bucketShards + "\r\n"));
    }
}
