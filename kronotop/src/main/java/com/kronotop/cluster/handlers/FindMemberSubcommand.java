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
import com.kronotop.common.KronotopException;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

class FindMemberSubcommand extends BaseKrAdminSubcommandHandler implements SubcommandHandler {

    FindMemberSubcommand(RoutingService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        FindMemberParameters parameters = new FindMemberParameters(request.getParams());
        Member member = membership.findMember(parameters.memberId);
        response.writeMap(memberToRedisMessage(member));
    }

    private class FindMemberParameters {
        private final String memberId;

        FindMemberParameters(ArrayList<ByteBuf> params) {
            if (params.size() < 2) {
                throw new KronotopException("member id is required");
            }
            memberId = readMemberId(params.get(1));
        }
    }
}
