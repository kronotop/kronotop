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

import com.kronotop.cluster.MembershipService;
import com.kronotop.cluster.handlers.protocol.KrAdminMessage;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.EnumMap;

@Command(KrAdminMessage.COMMAND)
@MinimumParameterCount(KrAdminMessage.MINIMUM_PARAMETER_COUNT)
public class KrAdminHandler implements Handler {

    private final EnumMap<KrAdminSubcommand, SubcommandHandler> handlers = new EnumMap<>(KrAdminSubcommand.class);

    public KrAdminHandler(MembershipService service) {
        handlers.put(KrAdminSubcommand.LIST_MEMBERS, new ListMembersSubcommand(service));
        handlers.put(KrAdminSubcommand.INITIALIZE_CLUSTER, new InitializeClusterSubcommand(service));
        handlers.put(KrAdminSubcommand.DESCRIBE_CLUSTER, new DescribeClusterSubcommand(service));
        handlers.put(KrAdminSubcommand.SET_MEMBER_STATUS, new SetMemberStatusSubcommand(service));
        handlers.put(KrAdminSubcommand.FIND_MEMBER, new FindMemberSubcommand(service));
        handlers.put(KrAdminSubcommand.REMOVE_MEMBER, new RemoveMemberSubcommand(service));
        handlers.put(KrAdminSubcommand.LIST_SILENT_MEMBERS, new ListSilentMembers(service));
        handlers.put(KrAdminSubcommand.SET_ROUTE, new SetRouteHandler(service));
        handlers.put(KrAdminSubcommand.SET_SHARD_STATUS, new SetShardStatusSubcommand(service));
        handlers.put(KrAdminSubcommand.DESCRIBE_SHARD, new DescribeShardSubcommand(service));
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.KRADMIN).set(new KrAdminMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        KrAdminMessage message = request.attr(MessageTypes.KRADMIN).get();
        SubcommandHandler executor = handlers.get(message.getSubcommand());
        if (executor == null) {
            throw new UnknownSubcommandException(message.getSubcommand().toString());
        }
        executor.execute(request, response);
    }
}
