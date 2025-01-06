/*
 * Copyright (c) 2023-2025 Kronotop
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

import com.apple.foundationdb.Transaction;
import com.kronotop.cluster.Member;
import com.kronotop.cluster.MemberStatus;
import com.kronotop.cluster.RoutingService;
import com.kronotop.common.KronotopException;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

class SetMemberStatusSubcommand extends BaseKrAdminSubcommandHandler implements SubcommandHandler {

    SetMemberStatusSubcommand(RoutingService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        SetMemberStatusParameters parameters = new SetMemberStatusParameters(request.getParams());
        try (Transaction tr = context.getFoundationDB().createTransaction()) {
            Member member = membership.findMember(tr, parameters.memberId);
            member.setStatus(parameters.memberStatus);
            membership.updateMember(tr, member);
            membership.triggerClusterTopologyWatcher(tr);
            tr.commit().join();
        }
        response.writeOK();
    }

    private class SetMemberStatusParameters {
        private final String memberId;
        private final MemberStatus memberStatus;

        private SetMemberStatusParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 3) {
                throw new KronotopException("Invalid number of parameters");
            }
            memberId = readMemberId(params.get(1));
            memberStatus = readMemberStatus(params.get(2));
        }
    }
}
