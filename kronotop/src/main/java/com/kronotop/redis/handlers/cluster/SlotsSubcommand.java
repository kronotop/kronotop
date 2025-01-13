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

package com.kronotop.redis.handlers.cluster;

import com.kronotop.cluster.Member;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.SlotRange;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;

import java.util.ArrayList;
import java.util.List;

class SlotsSubcommand implements SubcommandHandler {
    private final RedisService service;

    SlotsSubcommand(RedisService service) {
        this.service = service;
    }

    private List<RedisMessage> prepareMember(ChannelHandlerContext context, Member member) {
        List<RedisMessage> result = new ArrayList<>();
        // HOST
        ByteBuf hostBuf = context.alloc().buffer();
        hostBuf.writeBytes(member.getExternalAddress().getHost().getBytes());
        FullBulkStringRedisMessage fullBulkStringRedisMessage = new FullBulkStringRedisMessage(hostBuf);
        result.add(fullBulkStringRedisMessage);

        // PORT
        IntegerRedisMessage integerRedisMessage = new IntegerRedisMessage(member.getExternalAddress().getPort());
        result.add(integerRedisMessage);

        // ID
        ByteBuf idBuf = context.alloc().buffer();
        idBuf.writeBytes(member.getId().getBytes());
        FullBulkStringRedisMessage idMessage = new FullBulkStringRedisMessage(idBuf);
        result.add(idMessage);

        // Replicas, empty.
        result.add(new ArrayRedisMessage(new ArrayList<>()));

        return result;
    }

    @Override
    public void execute(Request request, Response response) {
        List<SlotRange> slotRanges = service.getSlotRanges();
        List<RedisMessage> root = new ArrayList<>();
        for (SlotRange range : slotRanges) {
            List<RedisMessage> children = new ArrayList<>();
            IntegerRedisMessage beginSection = new IntegerRedisMessage(range.getBegin());
            IntegerRedisMessage endSection = new IntegerRedisMessage(range.getEnd());
            ArrayRedisMessage ownerSection = new ArrayRedisMessage(prepareMember(request.getChannelContext(), range.getPrimary()));
            children.add(beginSection);
            children.add(endSection);
            children.add(ownerSection);
            root.add(new ArrayRedisMessage(children));
        }
        response.writeArray(root);
    }
}
