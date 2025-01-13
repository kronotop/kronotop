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
import java.util.ArrayList;
import java.util.List;

class NodesSubcommand implements SubcommandHandler {
    private final RedisService service;
    private final MembershipService membership;

    NodesSubcommand(RedisService service) {
        this.service = service;
        this.membership = service.getContext().getService(MembershipService.NAME);
    }

    private String generateNodeId(Member member) {
        return Hashing.sha1().newHasher().
                putString(member.getId(), StandardCharsets.UTF_8).
                hash().toString();
    }

    @Override
    public void execute(Request request, Response response) {
        // <id> <ip:port@cport[,hostname]> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slot> <slot> ... <slot>
        List<String> result = new ArrayList<>();
        List<SlotRange> slotRanges = service.getSlotRanges();
        for (SlotRange slotRange : slotRanges) {
            result.add(prepareLineForPrimary(slotRange));
            result.addAll(prepareLineForStandby(slotRange));
        }

        ByteBuf buf = response.getChannelContext().alloc().buffer();
        buf.writeBytes(String.join("\n", result).getBytes());
        response.writeFullBulkString(new FullBulkStringRedisMessage(buf));
    }

    private String prepareLineForPrimary(SlotRange range) {
        List<String> items = new ArrayList<>();

        items.add(generateNodeId(range.getPrimary()));

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
        long latestHeartbeat = membership.getLatestHeartbeat(range.getPrimary());
        items.add(Long.toString(latestHeartbeat));
        items.add("0"); // config-epoch: we don't have such a thing.
        items.add("connected");
        items.add(String.format("%d-%d", range.getBegin(), range.getEnd()));

        return String.join(" ", items);
    }

    private List<String> prepareLineForStandby(SlotRange range) {
        List<String> result = new ArrayList<>();
        range.getStandbys().forEach(standby -> {
            List<String> items = new ArrayList<>();
            items.add(generateNodeId(standby));

            Address address = standby.getExternalAddress();
            items.add(String.format("%s:%d@%d,%s",
                    address.getHost(),
                    address.getPort(),
                    address.getPort(),
                    address.getHost())
            );

            if (standby.equals(service.getContext().getMember())) {
                items.add("myself,slave");
            } else {
                items.add("slave");
            }

            items.add(generateNodeId(range.getPrimary()));

            long latestHeartbeat = membership.getLatestHeartbeat(standby);
            items.add("0");
            items.add(Long.toString(latestHeartbeat));
            items.add("0"); // config-epoch: we don't have such a thing.
            items.add("connected");

            result.add(String.join(" ", items));
        });
        return result;
    }
}
