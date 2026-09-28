/*
 * Copyright (c) 2023-2026 Burak Sezer
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

import com.kronotop.KronotopException;
import com.kronotop.cluster.Route;
import com.kronotop.cluster.RoutingService;
import com.kronotop.cluster.handlers.InvalidNumberOfArgumentsException;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.StringUtil;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.SubcommandHandler;
import com.kronotop.server.UnknownSubcommandException;
import com.kronotop.volume.VolumeNames;
import com.kronotop.volume.VolumeService;
import com.kronotop.volume.replication.ReplicationService;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

import static com.kronotop.AsyncCommandExecutor.runAsync;

class ReplicationSubcommand extends BaseSubcommandHandler implements SubcommandHandler {
    ReplicationSubcommand(VolumeService service) {
        super(service);
    }

    @Override
    public void execute(Request request, Response response) {
        ArrayList<ByteBuf> args = request.getArguments();
        if (args.size() < 2) {
            throw new InvalidNumberOfArgumentsException();
        }
        String operation = StringUtil.toUpperCaseAscii(ProtocolMessageUtil.readAsString(args.get(1)));
        switch (operation) {
            case "START" -> startReplication(request, response);
            case "STOP" -> stopReplication(request, response);
            default -> throw new UnknownSubcommandException(operation);
        }
    }

    private void startReplication(Request request, Response response) {
        Arguments arguments = new Arguments(request.getArguments());
        RoutingService routing = service.getContext().getService(RoutingService.NAME);
        ReplicationService replications = service.getContext().getService(ReplicationService.NAME);

        runAsync(context, response, () -> {
            VolumeNames.Parsed parsed = VolumeNames.parse(arguments.volumeName);
            Route route = routing.findRoute(parsed.shardKind(), parsed.shardId());
            if (route == null) {
                throw new KronotopException(
                        String.format("No route found for %s", arguments.volumeName)
                );
            }
            if (!route.standbys().contains(service.getContext().getMember())) {
                throw new KronotopException(
                        String.format("This node is not a standby for %s", arguments.volumeName)
                );
            }
            replications.startReplication(parsed.shardKind(), parsed.shardId(), true);
        }, response::writeOK);
    }

    private void stopReplication(Request request, Response response) {
        Arguments arguments = new Arguments(request.getArguments());
        RoutingService routing = service.getContext().getService(RoutingService.NAME);
        ReplicationService replications = service.getContext().getService(ReplicationService.NAME);

        runAsync(context, response, () -> {
            VolumeNames.Parsed parsed = VolumeNames.parse(arguments.volumeName);
            Route route = routing.findRoute(parsed.shardKind(), parsed.shardId());
            if (route == null) {
                throw new KronotopException(
                        String.format("No route found for %s", arguments.volumeName)
                );
            }
            if (!route.standbys().contains(service.getContext().getMember())) {
                throw new KronotopException(
                        String.format("This node is not a standby for %s", arguments.volumeName)
                );
            }
            replications.stopReplication(parsed.shardKind(), parsed.shardId(), true);
        }, response::writeOK);
    }

    private static class Arguments {
        private final String volumeName;

        private Arguments(ArrayList<ByteBuf> args) {
            if (args.size() != 3) {
                throw new InvalidNumberOfArgumentsException();
            }

            volumeName = ProtocolMessageUtil.readAsString(args.get(2));
        }
    }
}
