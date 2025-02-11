// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class BucketInsertHandlerTest extends BaseHandlerTest {

    @Test
    public void test_insert_single_document_with_oneOff_transaction() {
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert("test-bucket", "{\"one\": \"two\"}").encode(buf);
        instance.getChannel().writeInbound(buf);
        Object msg = instance.getChannel().readOutbound();
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        assertEquals(1, actualMessage.children().size());
        SimpleStringRedisMessage message = (SimpleStringRedisMessage) actualMessage.children().getFirst();
        assertNotNull(message.content());
    }

    @Test
    public void test_insert_documents_with_oneOff_transaction() {
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.insert("test-bucket", "{\"one\": \"two\"}", "{\"three\": \"four\"}").encode(buf);
        instance.getChannel().writeInbound(buf);
        Object msg = instance.getChannel().readOutbound();
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        assertEquals(2, actualMessage.children().size());
        for (int i = 0; i < actualMessage.children().size(); i++) {
            SimpleStringRedisMessage message = (SimpleStringRedisMessage) actualMessage.children().get(i);
            assertNotNull(message.content());
        }
    }

    @Test
    public void test_insert() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            // Create a new transaction
            cmd.begin().encode(buf);
            instance.getChannel().writeInbound(buf);
            instance.getChannel().readOutbound(); // Returns OK
        }

        // BUCKET.INSERT <bucket-name> <document> <document>
        {
            ByteBuf buf = Unpooled.buffer();
            BucketCommandBuilder<String, String> bucketCommandBuilder = new BucketCommandBuilder<>(StringCodec.UTF8);
            bucketCommandBuilder.insert("test-bucket", "{\"one\": \"two\"}", "{\"three\": \"four\"}").encode(buf);
            instance.getChannel().writeInbound(buf);
            Object msg = instance.getChannel().readOutbound();
            assertInstanceOf(ArrayRedisMessage.class, msg);

            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(0, actualMessage.children().size());
        }

        {
            // Commit the changes
            ByteBuf buf = Unpooled.buffer();
            cmd.commit().encode(buf);
            instance.getChannel().writeInbound(buf);
            instance.getChannel().readOutbound(); // Returns OK
        }
    }
}
