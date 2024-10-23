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
import com.kronotop.cluster.MembershipService;
import com.kronotop.common.KronotopException;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

public class BaseKrAdminSubcommandHandler {
    protected final MembershipService service;

    public BaseKrAdminSubcommandHandler(MembershipService service) {
        this.service = service;
    }

    protected String readMemberId(ByteBuf memberIdBuf) {
        byte[] rawMemberId = new byte[memberIdBuf.readableBytes()];
        memberIdBuf.readBytes(rawMemberId);
        String memberId = new String(rawMemberId);
        try {
            // Validate the member id.
            return UUID.fromString(memberId).toString();
        } catch (IllegalArgumentException e) {
            throw new KronotopException("Invalid memberId: " + memberId);
        }
    }

    protected Map<RedisMessage, RedisMessage> memberToRedisMessage(Member member) {
        Map<RedisMessage, RedisMessage> current = new LinkedHashMap<>();

        current.put(new SimpleStringRedisMessage("status"), new SimpleStringRedisMessage(member.getStatus().toString()));

        String processId = VersionstampUtils.base64Encode(member.getProcessId());
        current.put(new SimpleStringRedisMessage("process_id"), new SimpleStringRedisMessage(processId));

        current.put(new SimpleStringRedisMessage("external_host"), new SimpleStringRedisMessage(member.getExternalAddress().getHost()));
        current.put(new SimpleStringRedisMessage("external_port"), new IntegerRedisMessage(member.getExternalAddress().getPort()));

        current.put(new SimpleStringRedisMessage("internal_host"), new SimpleStringRedisMessage(member.getInternalAddress().getHost()));
        current.put(new SimpleStringRedisMessage("internal_port"), new IntegerRedisMessage(member.getInternalAddress().getPort()));

        long latestHeartbeat = service.getLatestHeartbeat(member);
        current.put(new SimpleStringRedisMessage("latest_heartbeat"), new IntegerRedisMessage(latestHeartbeat));

        return current;
    }
}
