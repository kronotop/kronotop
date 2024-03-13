/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.foundationdb.protocol;

import com.kronotop.server.MockChannelHandlerContext;
import com.kronotop.server.Request;
import com.kronotop.server.impl.RespRequest;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.sql.BaseHandlerTest;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CommitMessageTest extends BaseHandlerTest {
    @Test
    public void test_COMMIT() {
        List<RedisMessage> messages = new ArrayList<>();

        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes("COMMIT".getBytes())));

        ChannelHandlerContext context = new MockChannelHandlerContext(channel);
        Request request = new RespRequest(context, new ArrayRedisMessage(messages));
        CommitMessage commitMessage = new CommitMessage(request);
        assertEquals(0, commitMessage.getReturning().size());
    }

    @Test
    public void test_COMMIT_RETURNING() {
        List<RedisMessage> messages = new ArrayList<>();

        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes("COMMIT".getBytes())));
        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes("RETURNING".getBytes())));
        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes("FUTURES".getBytes())));
        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes("VERSIONSTAMP".getBytes())));
        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes("COMMITTED-VERSION".getBytes())));

        ChannelHandlerContext context = new MockChannelHandlerContext(channel);
        Request request = new RespRequest(context, new ArrayRedisMessage(messages));
        CommitMessage commitMessage = new CommitMessage(request);
        assertEquals(3, commitMessage.getReturning().size());
    }

    @Test
    public void test_COMMIT_RETURNING_when_invalid_parameters() {
        List<RedisMessage> messages = new ArrayList<>();

        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes("COMMIT".getBytes())));
        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes("RETURNING".getBytes())));
        messages.add(new FullBulkStringRedisMessage(Unpooled.buffer().writeBytes("foobar".getBytes())));

        ChannelHandlerContext context = new MockChannelHandlerContext(channel);
        Request request = new RespRequest(context, new ArrayRedisMessage(messages));
        assertThrows(IllegalArgumentException.class, () -> {
            new CommitMessage(request);
        });
    }
}