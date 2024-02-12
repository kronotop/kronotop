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

package com.kronotop.redis.cluster;

import com.kronotop.core.cluster.Member;
import com.kronotop.redis.RedisService;
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

class SlotsSubcommand implements SubcommandExecutor {
    private final RedisService service;

    SlotsSubcommand(RedisService service) {
        this.service = service;
    }

    private List<RedisMessage> prepareMember(ChannelHandlerContext context, Member member) {
        List<RedisMessage> result = new ArrayList<>();
        // HOST
        ByteBuf hostBuf = context.alloc().buffer();
        hostBuf.writeBytes(member.getAddress().getHost().getBytes());
        FullBulkStringRedisMessage fullBulkStringRedisMessage = new FullBulkStringRedisMessage(hostBuf);
        result.add(fullBulkStringRedisMessage);

        // PORT
        IntegerRedisMessage integerRedisMessage = new IntegerRedisMessage(member.getAddress().getPort());
        result.add(integerRedisMessage);

        // ID
        ByteBuf idBuf = context.alloc().buffer();
        idBuf.writeBytes(member.getId().getBytes());
        FullBulkStringRedisMessage idMessage = new FullBulkStringRedisMessage(idBuf);
        result.add(idMessage);

        return result;
    }

    @Override
    public void execute(Request request, Response response) {
        List<SlotRange> ranges = new ArrayList<>();
        SlotRange currentRange = new SlotRange(0);
        Integer currentShardId = null;
        int lastHashSlot = 0;
        for (int hashSlot = 0; hashSlot < service.NUM_HASH_SLOTS; hashSlot++) {
            int shardId = service.getHashSlots().get(hashSlot);
            if (currentShardId != null && shardId != currentShardId) {
                currentRange.setEnd(hashSlot - 1);
                Member owner = service.getClusterService().getRoutingTable().getRoute(currentShardId).getMember();
                currentRange.setOwner(owner);
                ranges.add(currentRange);
                currentRange = new SlotRange(hashSlot);
            }
            currentShardId = shardId;
            lastHashSlot = hashSlot;
        }

        currentRange.setEnd(lastHashSlot + 1);
        Member owner = service.getClusterService().getRoutingTable().getRoute(currentShardId).getMember();
        currentRange.setOwner(owner);
        ranges.add(currentRange);

        List<RedisMessage> root = new ArrayList<>();
        for (SlotRange r : ranges) {
            List<RedisMessage> children = new ArrayList<>();
            IntegerRedisMessage beginSection = new IntegerRedisMessage(r.begin);
            IntegerRedisMessage endSection = new IntegerRedisMessage(r.end);
            ArrayRedisMessage ownerSection = new ArrayRedisMessage(prepareMember(request.getChannelContext(), r.owner));
            children.add(beginSection);
            children.add(endSection);
            children.add(ownerSection);
            root.add(new ArrayRedisMessage(children));
        }

        response.writeArray(root);
    }
}
