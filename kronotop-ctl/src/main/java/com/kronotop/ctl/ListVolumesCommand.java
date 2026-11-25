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

package com.kronotop.ctl;

import com.kronotop.resp.RespConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import java.io.PrintWriter;
import java.util.concurrent.Callable;

@Command(
        name = "list-volumes",
        description = "List the volumes of a cluster.",
        separator = " "
)
public class ListVolumesCommand implements Callable<Integer> {
    private static final Logger logger = LoggerFactory.getLogger(ListVolumesCommand.class);
    @Spec
    private CommandSpec spec;
    @Option(names = "--address", paramLabel = "<host:port>", defaultValue = "localhost:3320",
            description = "Internal address of a cluster member. Default: ${DEFAULT-VALUE}")
    private String address;
    @Option(names = "--shard", paramLabel = "<id>", description = "Show only the volume of this shard.")
    private Integer shard;
    @Option(names = "--member", paramLabel = "<id>",
            description = "Show only the volumes on this member. Takes a full member ID or its first four characters.")
    private String member;
    @Option(names = "--timeout", paramLabel = "<seconds>", defaultValue = "10",
            description = "Max seconds to wait for each member to connect and reply. Default: ${DEFAULT-VALUE}")
    private int timeout;
    @Option(names = "--json", description = "Print the output as JSON.")
    private boolean json;
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show help.")
    private boolean helpRequested;

    @Override
    public Integer call() throws Exception {
        try {
            if (timeout < 1) {
                throw new IllegalArgumentException("--timeout must be a positive integer");
            }
            ClusterDescription description;
            try (RespConnection conn = ControlUtil.connect(address, timeout)) {
                description = ClusterDescription.fetch(conn);
            }
            VolumeList volumes = VolumeList.fetch(description, VolumeList.select(description, shard, member), timeout);

            PrintWriter out = spec.commandLine().getOut();
            if (json) {
                out.println(volumes.toJson());
            } else {
                out.print(volumes.toTable());
            }
            out.flush();
            return volumes.hasErrors() ? 1 : 0;
        } catch (Exception e) {
            PrintWriter err = spec.commandLine().getErr();
            err.println(ControlUtil.errorMessage(e));
            err.flush();
            return 1;
        }
    }
}
