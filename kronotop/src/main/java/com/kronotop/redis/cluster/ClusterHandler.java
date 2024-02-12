/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.redis.cluster;

import com.kronotop.redis.BaseHandler;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.cluster.protocol.ClusterMessage;
import com.kronotop.redis.cluster.protocol.ClusterSubcommand;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;

import java.util.EnumMap;

@Command(ClusterMessage.COMMAND)
@MinimumParameterCount(ClusterMessage.MINIMUM_PARAMETER_COUNT)
public class ClusterHandler extends BaseHandler implements Handler {
    private final EnumMap<ClusterSubcommand, SubcommandExecutor> executors = new EnumMap<>(ClusterSubcommand.class);

    public ClusterHandler(RedisService service) {
        super(service);

        executors.put(ClusterSubcommand.NODES, new NodesSubcommand(service));
        executors.put(ClusterSubcommand.SLOTS, new SlotsSubcommand(service));
        executors.put(ClusterSubcommand.MYID, new MyIdSubcommand(service));
        executors.put(ClusterSubcommand.KEYSLOT, new KeySlotSubcommand());
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.CLUSTER).set(new ClusterMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        ClusterMessage clusterMessage = request.attr(MessageTypes.CLUSTER).get();
        SubcommandExecutor executor = executors.get(clusterMessage.getSubcommand());
        if (executor == null) {
            throw new UnknownSubcommandException(clusterMessage.getSubcommand().toString());
        }
        executor.execute(request, response);
    }
}
