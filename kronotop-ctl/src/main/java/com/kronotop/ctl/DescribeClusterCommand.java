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

package com.kronotop.ctl;

import com.kronotop.resp.RespConnection;
import picocli.CommandLine.Command;
import picocli.CommandLine.Model.CommandSpec;
import picocli.CommandLine.Option;
import picocli.CommandLine.Spec;

import java.io.PrintWriter;
import java.util.concurrent.Callable;

@Command(
        name = "describe-cluster",
        description = "Show the shards of a cluster with their status and owners.",
        separator = " "
)
public class DescribeClusterCommand implements Callable<Integer> {

    @Spec
    private CommandSpec spec;
    @Option(names = "--address", paramLabel = "<host:port>", defaultValue = "localhost:3320",
            description = "Internal address of a cluster member. Default: ${DEFAULT-VALUE}")
    private String address;
    @Option(names = "--json", description = "Print the output as JSON.")
    private boolean json;
    @Option(names = {"-h", "--help"}, usageHelp = true, description = "Show help.")
    private boolean helpRequested;

    @Override
    public Integer call() {
        try (RespConnection conn = ControlUtil.connect(address)) {
            ClusterDescription description = ClusterDescription.fetch(conn);
            PrintWriter out = spec.commandLine().getOut();
            if (json) {
                out.println(description.toJson());
            } else {
                out.print(description.toTable());
            }
            out.flush();
            return 0;
        } catch (Exception e) {
            PrintWriter err = spec.commandLine().getErr();
            err.println(ControlUtil.errorMessage(e));
            err.flush();
            return 1;
        }
    }
}
