// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
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
}