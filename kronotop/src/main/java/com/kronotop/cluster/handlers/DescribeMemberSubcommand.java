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

import com.kronotop.cluster.Member;
import com.kronotop.cluster.RoutingService;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.resp3.RedisMessage;
import com.kronotop.server.resp3.SimpleStringRedisMessage;

import java.util.LinkedHashMap;
import java.util.Map;

class DescribeMemberSubcommand extends BaseKrAdminSubcommandHandler implements SubcommandHandler {

    public DescribeMemberSubcommand(RoutingService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        if (request.getParams().size() > 1) {
            throw new InvalidNumberOfParametersException();
        }

        Member member = context.getMember();
        Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
        result.put(new SimpleStringRedisMessage("member_id"), new SimpleStringRedisMessage(member.getId()));
        memberToRedisMessage(member, result);
        response.writeMap(result);
    }
}
