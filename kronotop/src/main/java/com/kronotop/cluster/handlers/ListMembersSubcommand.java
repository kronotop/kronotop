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

package com.kronotop.cluster.handlers;

import com.kronotop.VersionstampUtils;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.membership.MembershipService;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.TreeSet;

class ListMembersSubcommand implements SubcommandHandler {
    private final MembershipService service;

    ListMembersSubcommand(MembershipService service) {
        this.service = service;
    }

    @Override
    public void execute(Request request, Response response) {
        TreeSet<Member> sortedMembers = service.getSortedMembers();
        Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
        for (Member member : sortedMembers) {
            Map<RedisMessage, RedisMessage> current = new LinkedHashMap<>();

            String processId = VersionstampUtils.base64Encode(member.getProcessId());
            current.put(new SimpleStringRedisMessage("process_id"), new SimpleStringRedisMessage(processId));

            current.put(new SimpleStringRedisMessage("external_host"), new SimpleStringRedisMessage(member.getExternalAddress().getHost()));
            current.put(new SimpleStringRedisMessage("external_port"), new IntegerRedisMessage(member.getExternalAddress().getPort()));

            current.put(new SimpleStringRedisMessage("internal_host"), new SimpleStringRedisMessage(member.getInternalAddress().getHost()));
            current.put(new SimpleStringRedisMessage("internal_port"), new IntegerRedisMessage(member.getInternalAddress().getPort()));

            Map<Member, Long> latestHeartbeats = service.getLatestHeartbeats(member);
            current.put(new SimpleStringRedisMessage("latest_heartbeat"), new IntegerRedisMessage(latestHeartbeats.get(member)));

            result.put(new SimpleStringRedisMessage(member.getId()), new MapRedisMessage(current));
        }
        response.writeMap(result);
    }
}
