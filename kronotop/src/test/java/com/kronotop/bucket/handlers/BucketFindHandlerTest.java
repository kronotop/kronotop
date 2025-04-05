// Copyright (C) 2025 Burak Sezer
// Use of this software is governed by the Business Source License included
// in the LICENSE.TXT file and at www.mariadb.com/bsl11.

// Change Date: 5 years after release

// On the date above, in accordance with the Business Source License,
// use of this software will be governed by the open source license specified
// in the LICENSE.TXT file.

package com.kronotop.bucket.handlers;

import com.kronotop.BaseHandlerTest;
import com.kronotop.commandbuilder.kronotop.BucketCommandBuilder;
import com.kronotop.server.resp3.ArrayRedisMessage;
import io.lettuce.core.codec.StringCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class BucketFindHandlerTest extends BaseHandlerTest {

    @Test
    void test_bucket_find_handler() {
        BucketCommandBuilder<String, String> cmd = new BucketCommandBuilder<>(StringCodec.UTF8);
        ByteBuf buf = Unpooled.buffer();
        cmd.find("test-bucket", "{}").encode(buf);
        instance.getChannel().writeInbound(buf);
        Object msg = instance.getChannel().readOutbound();
        assertInstanceOf(ArrayRedisMessage.class, msg);

        ArrayRedisMessage actualMessage = (ArrayRedisMessage) msg;
        System.out.println(actualMessage.children());
    }
}