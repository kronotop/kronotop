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

package com.kronotop.cluster.handlers;

import com.kronotop.AsyncCommandExecutor;
import com.kronotop.KronotopException;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.RoutingService;
import com.kronotop.internal.ByteBufUtils;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

class FindMemberSubcommand extends BaseKrAdminSubcommandHandler implements SubcommandHandler {

    FindMemberSubcommand(RoutingService routing) {
        super(routing);
    }

    @Override
    public void execute(Request request, Response response) {
        FindMemberParameters parameters = new FindMemberParameters(request.getParams());
        AsyncCommandExecutor.supplyAsync(context, response, () -> {
            Member member = membership.findMember(parameters.memberId);
            Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
            memberToRedisMessage(member, result);
            return result;
        }, response::writeMap);
    }

    private class FindMemberParameters {
        private final String memberId;

        FindMemberParameters(ArrayList<ByteBuf> params) {
            if (params.size() < 2) {
                throw new KronotopException("member id is required");
            }
            memberId = ByteBufUtils.readMemberId(context, params.get(1));
        }
    }
}
