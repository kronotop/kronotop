/*
 * Copyright (c) 2023-2025 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.volume.handlers;

import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.handlers.protocol.VolumeInspectMessage;
import com.kronotop.volume.handlers.protocol.VolumeInspectSubcommand;

import java.util.EnumMap;

@Command(VolumeInspectMessage.COMMAND)
@MinimumParameterCount(VolumeInspectMessage.MINIMUM_PARAMETER_COUNT)
public class VolumeInspectHandler extends BaseSubcommandHandler implements Handler {

    private final EnumMap<VolumeInspectSubcommand, SubcommandHandler> handlers = new EnumMap<>(VolumeInspectSubcommand.class);

    public VolumeInspectHandler(VolumeService service) {
        super(service);

        handlers.put(VolumeInspectSubcommand.CURSOR, new CursorSubcommand(service));
        handlers.put(VolumeInspectSubcommand.REPLICATION, new ReplicationSubcommand(service));
    }

    @Override
    public boolean isRedisCompatible() {
        return false;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.VOLUMEINSPECT).set(new VolumeInspectMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        VolumeInspectMessage message = request.attr(MessageTypes.VOLUMEINSPECT).get();
        SubcommandHandler executor = handlers.get(message.getSubcommand());
        if (executor == null) {
            throw new UnknownSubcommandException(message.getSubcommand().toString());
        }
        executor.execute(request, response);
    }
}
