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

package com.kronotop.cli;

import com.kronotop.cli.resp.RespReader;
import com.kronotop.cli.resp.RespValue;
import com.kronotop.cli.resp.RespWriter;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@Command(
        name = "kronotop-cli",
        description = "Command-line interface for Kronotop.%nWhen no command is given, kronotop-cli starts in interactive mode.",
        mixinStandardHelpOptions = false,
        sortOptions = false,
        version = "kronotop-cli 0.13-SNAPSHOT"
)
public class KronotopCLI implements Callable<Integer> {

    private final PrintWriter out = new PrintWriter(System.out, true);

    @Parameters(description = "Command to execute.", arity = "0..*")
    private List<String> command;

    @Option(names = {"-h", "--host"}, description = "Server hostname.", defaultValue = "127.0.0.1")
    private String host;

    @Option(names = {"-p", "--port"}, description = "Server port.", defaultValue = "5484")
    private int port;

    @Option(names = {"-t", "--timeout"}, description = "Server connection timeout in seconds (decimals allowed). Default is 0, meaning no limit.", defaultValue = "0")
    private double timeout;

    @Option(names = "-r", description = "Execute specified command N times.", defaultValue = "1")
    private int repeat;

    @Option(names = "-i", description = "When -r is used, waits <interval> seconds per command. Sub-second times allowed (e.g., -i 0.1).", defaultValue = "0")
    private double interval;

    @Option(names = {"-2", "--resp-2"}, description = "Start session in RESP2 protocol mode.")
    private boolean resp2;

    @Option(names = {"-3", "--resp-3"}, description = "Start session in RESP3 protocol mode.")
    private boolean resp3;

    @Option(names = "--json", description = "Output in JSON format (default RESP3, use -2 if you want to use with RESP2).")
    private boolean json;

    @Option(names = "--quoted-json", description = "Same as --json, but produce ASCII-safe quoted strings, not Unicode.")
    private boolean quotedJson;

    @Option(names = "--csv", description = "Output in CSV format.")
    private boolean csv;

    @Option(names = "--raw", description = "Use raw formatting for replies (default when STDOUT is not a tty).")
    private boolean raw;

    @Option(names = "--no-raw", description = "Force formatted output even when STDOUT is not a tty.")
    private boolean noRaw;

    @Option(names = "-d", description = "Delimiter between response bulks for raw formatting.", defaultValue = "\\n")
    private String delimiter;

    @Option(names = "-D", description = "Delimiter between responses for raw formatting.", defaultValue = "\\n")
    private String responseDelimiter;

    @Option(names = "--quoted-input", description = "Force input to be handled as quoted strings.")
    private boolean quotedInput;

    @Option(names = {"-v", "--version"}, versionHelp = true, description = "Print version information and quit.")
    private boolean versionRequested;

    @Option(names = "--help", usageHelp = true, description = "Show help.")
    private boolean helpRequested;

    private Socket socket;
    private RespReader respReader;
    private RespWriter respWriter;
    private ResponseFormatter responseFormatter;
    private JsonResponseFormatter jsonFormatter;
    private RawResponseFormatter rawFormatter;
    private CsvResponseFormatter csvFormatter;
    private CommandLineParser commandLineParser;
    private boolean useRawOutput;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new KronotopCLI()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        try {
            connect();
            // --json defaults to RESP3 unless -2 is explicitly set
            if ((json || quotedJson) && !resp2) {
                resp3 = true;
            }
            if (resp2 || resp3) {
                switchProtocol();
            }
            initializeFormatter();
            commandLineParser = new CommandLineParser(quotedInput);
        } catch (IOException e) {
            System.err.printf("Could not connect to Kronotop at %s:%d: %s%n", host, port, e.getMessage());
            return 1;
        }

        try {
            if (command != null && !command.isEmpty()) {
                long intervalMs = (long) (interval * 1000);
                String parsedResponseDelimiter = CommandLineParser.parseDelimiter(responseDelimiter);
                for (int i = 0; i < repeat; i++) {
                    executeCommand(command);
                    if (i < repeat - 1) {
                        out.print(parsedResponseDelimiter);
                        if (intervalMs > 0) {
                            LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(intervalMs));
                        }
                    }
                }
                return 0;
            }

            return runInteractiveMode();
        } finally {
            disconnect();
        }
    }

    private void initializeFormatter() throws IOException {
        if (json || quotedJson) {
            jsonFormatter = new JsonResponseFormatter(quotedJson);
            return;
        }

        if (csv) {
            csvFormatter = new CsvResponseFormatter();
            return;
        }

        // Determine if we should use raw output
        if (raw) {
            useRawOutput = true;
        } else if (noRaw) {
            useRawOutput = false;
        } else {
            // Default: raw when not a TTY (System.console() is null when not interactive)
            useRawOutput = System.console() == null;
        }

        if (useRawOutput) {
            rawFormatter = new RawResponseFormatter(CommandLineParser.parseDelimiter(delimiter));
        } else {
            responseFormatter = new ResponseFormatter(fetchJsonReplyType());
        }
    }

    private boolean fetchJsonReplyType() throws IOException {
        respWriter.writeCommand(List.of("SESSION.ATTRIBUTE", "list"));
        RespValue response = respReader.read();

        if (response instanceof RespValue.Array array) {
            List<RespValue> values = array.values();
            for (int i = 0; i < values.size() - 1; i += 2) {
                RespValue key = values.get(i);
                RespValue value = values.get(i + 1);

                if (key instanceof RespValue.SimpleString ss && ss.value().equals("reply_type")) {
                    if (value instanceof RespValue.SimpleString vs) {
                        return vs.value().equals("json");
                    }
                    break;
                }
            }
        }
        return false;
    }

    private void connect() throws IOException {
        socket = new Socket();
        int timeoutMs = (int) (timeout * 1000);
        socket.connect(new InetSocketAddress(host, port), timeoutMs);
        if (timeoutMs > 0) {
            socket.setSoTimeout(timeoutMs);
        }
        respReader = new RespReader(socket.getInputStream());
        respWriter = new RespWriter(socket.getOutputStream());
    }

    private void switchProtocol() throws IOException {
        int protocolVersion = resp2 ? 2 : 3;
        respWriter.writeCommand(List.of("HELLO", String.valueOf(protocolVersion)));
        respReader.read();
    }

    private void disconnect() {
        if (socket != null && !socket.isClosed()) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private int runInteractiveMode() {
        Path historyFile = Path.of(System.getProperty("user.home"), ".kronotopcli_history");

        try (Terminal terminal = TerminalBuilder.builder()
                .system(true)
                .build()) {

            DefaultHistory history = new DefaultHistory();

            LineReader reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .history(history)
                    .variable(LineReader.HISTORY_FILE, historyFile)
                    .build();

            history.attach(reader);

            String prompt = String.format("%s:%d> ", host, port);

            while (true) {
                String line;
                try {
                    line = reader.readLine(prompt);
                } catch (UserInterruptException e) {
                    continue;
                } catch (EndOfFileException e) {
                    break;
                }

                if (line == null || line.trim().isEmpty()) {
                    continue;
                }

                String trimmedLine = line.trim();
                if (trimmedLine.equalsIgnoreCase("quit") || trimmedLine.equalsIgnoreCase("exit")) {
                    break;
                }

                List<String> args = commandLineParser.parse(trimmedLine);
                if (!args.isEmpty()) {
                    executeCommand(args);
                }
            }

            return 0;
        } catch (IOException e) {
            System.err.println("Error initializing terminal: " + e.getMessage());
            return 1;
        }
    }

    private void executeCommand(List<String> args) {
        try {
            respWriter.writeCommand(args);
            RespValue response = respReader.read();
            if (json || quotedJson) {
                out.println(jsonFormatter.format(response));
            } else if (csv) {
                out.println(csvFormatter.format(response));
            } else if (useRawOutput) {
                out.println(rawFormatter.format(response));
                out.println();
            } else {
                out.println(responseFormatter.format(response));
            }
        } catch (IOException e) {
            System.err.println("Error: " + e.getMessage());
        }
    }
}
