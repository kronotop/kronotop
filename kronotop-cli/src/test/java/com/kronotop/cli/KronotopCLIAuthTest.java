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

package com.kronotop.cli;

import com.kronotop.resp.RespReader;
import com.kronotop.resp.RespValue;
import com.kronotop.resp.RespWriter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import picocli.CommandLine;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class KronotopCLIAuthTest {
    private static final RespValue OK = new RespValue.SimpleString("OK");

    private final List<List<String>> received = new CopyOnWriteArrayList<>();
    private volatile Function<List<String>, RespValue> replies = command -> OK;
    private ServerSocket serverSocket;
    private Thread serverThread;

    @BeforeEach
    void startServer() throws IOException {
        serverSocket = new ServerSocket(0, 1, InetAddress.getLoopbackAddress());
        serverThread = new Thread(this::serve);
        serverThread.setDaemon(true);
        serverThread.start();
    }

    @AfterEach
    void stopServer() throws IOException, InterruptedException {
        serverSocket.close();
        serverThread.join(5000);
    }

    // Serves one connection: records every command and answers it with the configured reply.
    private void serve() {
        try (Socket socket = serverSocket.accept()) {
            RespReader reader = new RespReader(socket.getInputStream());
            RespWriter writer = new RespWriter(socket.getOutputStream());
            while (true) {
                RespValue.Array command = (RespValue.Array) reader.read();
                List<String> args = command.values().stream()
                        .map(value -> ((RespValue.BlobString) value).value())
                        .toList();
                received.add(args);
                writer.write(replies.apply(args));
                writer.flush();
            }
        } catch (IOException ignored) {
            // The client closed the connection, or the test stopped the server.
        }
    }

    private Result run(String... options) {
        String[] args = new String[options.length + 4];
        args[0] = "-h";
        args[1] = serverSocket.getInetAddress().getHostAddress();
        args[2] = "-p";
        args[3] = String.valueOf(serverSocket.getLocalPort());
        System.arraycopy(options, 0, args, 4, options.length);

        PrintStream originalErr = System.err;
        ByteArrayOutputStream err = new ByteArrayOutputStream();
        System.setErr(new PrintStream(err, true));
        try {
            int exitCode = new CommandLine(new KronotopCLI()).execute(args);
            return new Result(exitCode, err.toString());
        } finally {
            System.setErr(originalErr);
        }
    }

    @Test
    void shouldAuthenticateBeforeHelloWhenPasswordIsGiven() {
        // Behavior: with -a secret, the CLI sends AUTH secret first, then HELLO 3, then the three session
        // attributes, and only then the PING command. It exits with 0, and --no-auth-warning keeps stderr empty.
        Result result = run("--no-auth-warning", "-a", "secret", "PING");

        assertEquals(0, result.exitCode());
        assertEquals("", result.stderr());
        assertEquals(List.of(
                List.of("AUTH", "secret"),
                List.of("HELLO", "3"),
                List.of("SESSION.ATTRIBUTE", "SET", "input_type", "JSON"),
                List.of("SESSION.ATTRIBUTE", "SET", "reply_type", "JSON"),
                List.of("SESSION.ATTRIBUTE", "SET", "object_id_format", "hex"),
                List.of("PING")
        ), received);
    }

    @Test
    void shouldSendUserAndPasswordWhenUserIsGiven() {
        // Behavior: with --user admin --pass secret, the first command is AUTH admin secret. Without
        // --no-auth-warning, a warning about the password on the command line is printed to stderr.
        Result result = run("--user", "admin", "--pass", "secret", "PING");

        assertEquals(0, result.exitCode());
        assertEquals(List.of("AUTH", "admin", "secret"), received.getFirst());
        assertTrue(result.stderr().contains(
                "Warning: Using a password with '-a' option on the command line interface may not be safe."));
    }

    @Test
    void shouldNotSendAuthWhenNoPasswordIsGiven() {
        // Behavior: without a password, the CLI does not send AUTH, so the first command is HELLO 3.
        Result result = run("PING");

        assertEquals(0, result.exitCode());
        assertEquals(List.of("HELLO", "3"), received.getFirst());
    }

    @Test
    void shouldFailWhenAuthIsRejected() {
        // Behavior: when the server answers AUTH with a WRONGPASS error, the CLI prints "AUTH failed" with the
        // server error to stderr, exits with 1 and sends no other command.
        replies = command -> new RespValue.SimpleError(
                "WRONGPASS", "invalid username-password pair or user is disabled.");

        Result result = run("--no-auth-warning", "-a", "wrong", "PING");

        assertEquals(1, result.exitCode());
        assertEquals(List.of(List.of("AUTH", "wrong")), received);
        assertTrue(result.stderr().contains(
                "AUTH failed: WRONGPASS invalid username-password pair or user is disabled."));
    }

    @Test
    void shouldFailWhenHelloIsRejected() {
        // Behavior: when no password is given and the server answers HELLO 3 with a NOAUTH error, the CLI
        // prints "HELLO 3 failed" with the server error to stderr, exits with 1 and sends no other command.
        replies = command -> new RespValue.SimpleError("NOAUTH", "HELLO must be called with the client already authenticated");

        Result result = run("PING");

        assertEquals(1, result.exitCode());
        assertEquals(List.of(List.of("HELLO", "3")), received);
        assertTrue(result.stderr().contains(
                "HELLO 3 failed: NOAUTH HELLO must be called with the client already authenticated"));
    }

    private record Result(int exitCode, String stderr) {
    }
}
