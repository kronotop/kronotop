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

import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.net.ServerSocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DescribeClusterCommandTest {

    @Test
    void shouldExitWithErrorWhenServerIsUnreachable() throws IOException {
        // Behavior: describe-cluster runs with --address set to a local port where nothing listens. It exits
        // with 1, prints "Could not connect to '<address>'" to stderr and prints nothing to stdout.
        String address;
        try (ServerSocket socket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress())) {
            address = socket.getInetAddress().getHostAddress() + ":" + socket.getLocalPort();
        }

        StringWriter out = new StringWriter();
        StringWriter err = new StringWriter();
        CommandLine commandLine = new CommandLine(new KronotopControl());
        commandLine.setOut(new PrintWriter(out));
        commandLine.setErr(new PrintWriter(err));

        int exitCode = commandLine.execute("describe-cluster", "--address", address);

        assertEquals(1, exitCode);
        assertTrue(err.toString().startsWith("Could not connect to '" + address + "'"));
        assertEquals("", out.toString());
    }
}
