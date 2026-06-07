/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import com.kronotop.server.*;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.annotation.MinimumParameterCount;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.handlers.protocol.VolumeStatsMessage;
import com.kronotop.volume.handlers.protocol.VolumeStatsSubcommand;

import java.util.EnumMap;

@Command(VolumeStatsMessage.COMMAND)
@MinimumParameterCount(VolumeStatsMessage.MINIMUM_PARAMETER_COUNT)
public class VolumeStatsHandler extends BaseSubcommandHandler implements Handler {

    private final EnumMap<VolumeStatsSubcommand, SubcommandHandler> handlers = new EnumMap<>(VolumeStatsSubcommand.class);
    private final OverviewStatsSubcommand overviewHandler;

    public VolumeStatsHandler(VolumeService service) {
        super(service);

        overviewHandler = new OverviewStatsSubcommand(service);
        handlers.put(VolumeStatsSubcommand.OPCOUNTERS, new OpCountersStatsSubcommand(service));
        handlers.put(VolumeStatsSubcommand.REPLICATION, new ReplicationStatsSubcommand(service));
        handlers.put(VolumeStatsSubcommand.SEGMENTS, new SegmentsStatsSubcommand(service));
        handlers.put(VolumeStatsSubcommand.RESET, new ResetStatsSubcommand(service));
    }

    @Override
    public boolean isRedisCompatible() {
        return false;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.VOLUMESTATS).set(new VolumeStatsMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        VolumeStatsMessage message = request.attr(MessageTypes.VOLUMESTATS).get();
        VolumeStatsSubcommand subcommand = message.getSubcommand();

        if (subcommand == null) {
            overviewHandler.execute(request, response);
            return;
        }

        SubcommandHandler executor = handlers.get(subcommand);
        if (executor == null) {
            throw new UnknownSubcommandException(subcommand.toString());
        }
        executor.execute(request, response);
    }
}
