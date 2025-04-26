// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.kronotop.bucket.BSONUtils;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.protocol.KronotopCommandBuilder;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class BucketInsertHandlerTest extends BaseBucketHandlerTest {

    @Test
    void test_insert_single_document_with_oneOff_transaction() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(List.of(DOCUMENT));
        cmd.insert(BUCKET_NAME, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        assertEquals(1, actualMessage.children().size());
        SimpleStringRedisMessage message = (SimpleStringRedisMessage) actualMessage.children().getFirst();
        assertNotNull(message.content());
    }

    @Test
    void test_insert_documents_with_oneOff_transaction() {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(
                List.of(
                        BSONUtils.jsonToDocumentThenBytes("{\"one\": \"two\"}"),
                        BSONUtils.jsonToDocumentThenBytes("{\"three\": \"four\"}")
                ));
        cmd.insert(BUCKET_NAME, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;

        assertEquals(2, actualMessage.children().size());
        for (int i = 0; i < actualMessage.children().size(); i++) {
            SimpleStringRedisMessage message = (SimpleStringRedisMessage) actualMessage.children().get(i);
            assertNotNull(message.content());
        }
    }

    @Test
    void test_insert() {
        KronotopCommandBuilder<String, String> cmd = new KronotopCommandBuilder<>(StringCodec.ASCII);
        {
            ByteBuf buf = Unpooled.buffer();
            // Create a new transaction
            cmd.begin().encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(SimpleStringRedisMessage.class, msg);
            SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            assertEquals(Response.OK, actualMessage.content());
        }

        // BUCKET.INSERT <bucket-name> <document> <document>
        {
            ByteBuf buf = Unpooled.buffer();
            BucketCommandBuilder<byte[], byte[]> bucketCommandBuilder = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
            byte[][] docs = makeDocumentsArray(
                    List.of(
                            BSONUtils.jsonToDocumentThenBytes("{\"one\": \"two\"}"),
                            BSONUtils.jsonToDocumentThenBytes("{\"three\": \"four\"}")
                    ));
            bucketCommandBuilder.insert(BUCKET_NAME, docs).encode(buf);

            Object msg = runCommand(channel, buf);
            assertInstanceOf(ArrayRedisMessage.class, msg);

            ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
            assertEquals(2, actualMessage.children().size());

            // User versions have returned
            for (RedisMessage redisMessage : actualMessage.children()) {
                assertInstanceOf(IntegerRedisMessage.class, redisMessage);
            }
        }

        {
            // Commit the changes
            ByteBuf buf = Unpooled.buffer();
            cmd.commit().encode(buf);

            // TODO: Enable this block after revisiting Transaction lifecycle improvements.
            //Object msg = runCommand(channel, buf);
            //assertInstanceOf(SimpleStringRedisMessage.class, msg);
            //SimpleStringRedisMessage actualMessage = (SimpleStringRedisMessage) msg;
            //assertEquals(Response.OK, actualMessage.content());
        }
    }
}
