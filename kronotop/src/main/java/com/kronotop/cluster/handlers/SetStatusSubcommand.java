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

import com.kronotop.cluster.Member;
import com.kronotop.cluster.MemberStatus;
import com.kronotop.cluster.MembershipService;
import com.kronotop.common.KronotopException;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

class SetStatusSubcommand extends BaseKrAdminSubcommandHandler implements SubcommandHandler {

    SetStatusSubcommand(MembershipService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        SetStatusParameters parameters = new SetStatusParameters(request.getParams());
        Member member = service.findMember(parameters.memberId);
        member.setStatus(parameters.status);
        service.updateMember(member);
        response.writeOK();
    }

    private class SetStatusParameters {
        private final String memberId;
        private final MemberStatus status;

        private SetStatusParameters(ArrayList<ByteBuf> params) {
            if (params.size() != 3) {
                throw new KronotopException("Invalid number of parameters");
            }
            memberId = readMemberId(params.get(1));

            ByteBuf statusBuf = params.get(2);
            byte[] rawStatus = new byte[statusBuf.readableBytes()];
            statusBuf.readBytes(rawStatus);
            String stringStatus = new String(rawStatus);

            try {
                status = MemberStatus.valueOf(stringStatus.toUpperCase());
            } catch (IllegalArgumentException e) {
                throw new KronotopException("Invalid member status " + stringStatus);
            }
        }
    }
}
