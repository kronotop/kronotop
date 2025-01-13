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

import com.kronotop.redis.handlers.cluster.protocol.ClusterMessage;
import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import io.lettuce.core.cluster.SlotHash;

class KeySlotSubcommand implements SubcommandHandler {

    @Override
    public void execute(Request request, Response response) {
        ClusterMessage clusterMessage = request.attr(MessageTypes.CLUSTER).get();
        response.writeInteger(SlotHash.getSlot(clusterMessage.getKey()));
    }
}
