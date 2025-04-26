// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.kronotop.BaseHandlerTest;
import com.kronotop.bucket.BSONUtils;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.lettuce.core.codec.ByteArrayCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

public class BaseBucketHandlerTest extends BaseHandlerTest {
    protected final String BUCKET_NAME = "test-bucket";
    protected final byte[] DOCUMENT = BSONUtils.jsonToDocumentThenBytes("{\"one\": \"two\"}");

    protected byte[][] makeDocumentsArray(List<byte[]> documents) {
        byte[][] result = new byte[documents.size()][];
        for (int i = 0; i < documents.size(); i++) {
            byte[] document = documents.get(i);
            result[i] = document;
        }
        documents.toArray(result);
        return result;
    }

    protected Map<String, byte[]> insertDocuments(List<byte[]> documents) {
        BucketCommandBuilder<byte[], byte[]> cmd = new BucketCommandBuilder<>(ByteArrayCodec.INSTANCE);
        ByteBuf buf = Unpooled.buffer();
        byte[][] docs = makeDocumentsArray(documents);
        cmd.insert(BUCKET_NAME, docs).encode(buf);

        Object msg = runCommand(channel, buf);
        assertInstanceOf(ArrayRedisMessage.class, msg);
        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        assertEquals(documents.size(), actualMessage.children().size());
        Map<String, byte[]> result = new LinkedHashMap<>();
        for (int index = 0; index < actualMessage.children().size(); index++) {
            SimpleStringRedisMessage message = (SimpleStringRedisMessage) actualMessage.children().get(index);
            assertNotNull(message.content());
            result.put(message.content(), documents.get(index));
        }
        return result;
    }
}
