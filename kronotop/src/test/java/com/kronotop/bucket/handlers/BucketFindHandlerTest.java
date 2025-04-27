// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.*;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class BucketFindHandlerTest extends BaseBucketHandlerTest {

    @Test
    void shouldDoPhysicalFullScanWithoutOperator() {
        Map<String, byte[]> expectedDocument = insertDocuments(List.of(DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP3);

        ByteBuf buf = Unpooled.buffer();
        cmd.find(BUCKET_NAME, "{}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(MapRedisMessage.class, msg);

        MapRedisMessage actualMessage = (MapRedisMessage) msg;
        for (Map.Entry<RedisMessage, RedisMessage> entry : actualMessage.children().entrySet()) {
            // Check key
            SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry.getKey();
            String id = keyMessage.content();
            assertNotNull(id);
            assertNotNull(expectedDocument.get(id));

            // Check value
            FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) entry.getValue();
            assertArrayEquals(expectedDocument.get(id), ByteBufUtil.getBytes(value.content()));
        }
    }

    @Test
    void shouldDoPhysicalFullScanWithoutOperator_RESP2() {
        Map<String, byte[]> expectedDocument = insertDocuments(List.of(DOCUMENT));

        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        switchProtocol(cmd, RESPVersion.RESP2);

        ByteBuf buf = Unpooled.buffer();
        cmd.find(BUCKET_NAME, "{}").encode(buf);
        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);

        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        int index = 0;
        String latestId = "";
        for (RedisMessage entry : actualMessage.children()) {
            if (index % 2 == 0) {
                // Check key
                SimpleStringRedisMessage keyMessage = (SimpleStringRedisMessage) entry;
                String id = keyMessage.content();
                assertNotNull(id);
                assertNotNull(expectedDocument.get(id));
                latestId = id;
            } else {
                // Check value
                FullBulkStringRedisMessage value = (FullBulkStringRedisMessage) entry;
                assertArrayEquals(expectedDocument.get(latestId), ByteBufUtil.getBytes(value.content()));
            }
            index++;
        }
    }
}