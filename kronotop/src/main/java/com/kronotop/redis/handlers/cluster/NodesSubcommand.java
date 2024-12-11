/*
 * Copyright (c) 2023-2024 Kronotop
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

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.MembershipService;
import com.kronotop.network.Address;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.SlotRange;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.*;

class NodesSubcommand implements SubcommandHandler {
    private final RedisService service;

    NodesSubcommand(RedisService service) {
        this.service = service;
    }

    @Override
    public void execute(Request request, Response response) {
        // <id> <ip:port@cport[,hostname]> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
        List<String> result = new ArrayList<>();
        MembershipService membership = service.getContext().getService(MembershipService.NAME);
        TreeSet<Member> members = membership.listMembers();

        // We don't have a version yet.
        Long configEpoch = 0L;

        Map<Member, Long> latestHeartbeats = new HashMap<>();
        for (Member member : members) {
            latestHeartbeats.put(member, membership.getLatestHeartbeat(member));
        }
        List<SlotRange> slotRanges = service.getSlotRanges();
        for (SlotRange slotRange : slotRanges) {
            long latestHeartbeat = latestHeartbeats.get(slotRange.getPrimary());
            HashCode hashCode = Hashing.sha1().newHasher().
                    putString(slotRange.getPrimary().getId(), StandardCharsets.UTF_8).
                    hash();
            result.add(getLine(hashCode.toString(), slotRange, configEpoch, latestHeartbeat));
        }

        ByteBuf buf = response.getChannelContext().alloc().buffer();
        buf.writeBytes(String.join("\n", result).getBytes());
        response.writeFullBulkString(new FullBulkStringRedisMessage(buf));
    }

    private String getLine(String id, SlotRange range, Long configEpoch, Long latestHeartbeat) {
        List<String> items = new ArrayList<>();

        items.add(id);

        Address address = range.getPrimary().getExternalAddress();
        items.add(String.format("%s:%d@%d,%s",
                address.getHost(),
                address.getPort(),
                address.getPort(),
                address.getHost())
        );
        if (range.getPrimary().equals(service.getContext().getMember())) {
            items.add("myself,master");
        } else {
            items.add("master");
        }
        items.add("-");
        items.add("0");
        items.add(latestHeartbeat.toString());
        items.add(configEpoch.toString());
        items.add("connected");
        items.add(String.format("%d-%d", range.getBegin(), range.getEnd()));

        return String.join(" ", items);
    }
}
