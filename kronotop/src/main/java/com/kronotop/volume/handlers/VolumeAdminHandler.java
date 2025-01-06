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

package com.kronotop.volume.handlers;

import com.kronotop.redis.server.SubcommandHandler;
import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.handlers.protocol.VolumeAdminMessage;
import com.kronotop.volume.handlers.protocol.VolumeAdminSubcommand;

import java.util.EnumMap;

@Command(VolumeAdminMessage.COMMAND)
@MinimumParameterCount(VolumeAdminMessage.MINIMUM_PARAMETER_COUNT)
public class VolumeAdminHandler extends BaseHandler implements Handler {

    private final EnumMap<VolumeAdminSubcommand, SubcommandHandler> handlers = new EnumMap<>(VolumeAdminSubcommand.class);


    public VolumeAdminHandler(VolumeService service) {
        super(service);

        handlers.put(VolumeAdminSubcommand.LIST, new ListSubcommand(service));
        handlers.put(VolumeAdminSubcommand.DESCRIBE, new DescribeSubcommand(service));
        handlers.put(VolumeAdminSubcommand.SET_STATUS, new SetStatusSubcommand(service));
        handlers.put(VolumeAdminSubcommand.REPLICATIONS, new ReplicationsSubcommand(service));
        handlers.put(VolumeAdminSubcommand.VACUUM, new VacuumSubcommand(service));
        handlers.put(VolumeAdminSubcommand.STOP_VACUUM, new StopVacuumSubcommand(service));
        handlers.put(VolumeAdminSubcommand.CLEANUP_ORPHAN_FILES, new CleanupOrphanFilesSubcommand(service));
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.VOLUMEADMIN).set(new VolumeAdminMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        VolumeAdminMessage message = request.attr(MessageTypes.VOLUMEADMIN).get();
        SubcommandHandler executor = handlers.get(message.getSubcommand());
        if (executor == null) {
            throw new UnknownSubcommandException(message.getSubcommand().toString());
        }
        executor.execute(request, response);
    }
}
