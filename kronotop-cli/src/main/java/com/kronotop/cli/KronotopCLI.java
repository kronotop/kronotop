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

import com.kronotop.resp.RespReader;
import com.kronotop.resp.RespValue;
import com.kronotop.resp.RespWriter;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.reader.impl.history.DefaultHistory;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.jline.widget.TailTipWidgets;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Command(
        name = "kronotop-cli",
        description = "Command-line interface for Kronotop.%nWhen no command is given, kronotop-cli starts in interactive mode.",
        sortOptions = false,
        versionProvider = VersionProvider.class
)
public class KronotopCLI implements Callable<Integer> {

    private static final String TIMESTAMP_VAR = "$TIMESTAMP";
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

    @Option(names = {"-2", "--resp-2"}, description = "Start session in RESP2 protocol mode (default: RESP3).")
    private boolean resp2;

    @Option(names = {"-3", "--resp-3"}, description = "Start session in RESP3 protocol mode (default).")
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

    @Option(names = {"-f", "--file"}, description = "Execute commands from a script file.")
    private String scriptFile;

    @Option(names = "--no-color", description = "Disable colorized output.")
    private boolean noColor;

    @Option(names = {"-s", "--silent"}, description = "Suppress command output in script mode (-f). Errors are still printed to stderr.")
    private boolean silent;

    @Option(names = "--input-type", description = "Session input type. Valid values: JSON, BSON. Default: JSON.", defaultValue = "JSON")
    private String inputType;

    @Option(names = "--reply-type", description = "Session reply type. Valid values: JSON, BSON. Default: JSON.", defaultValue = "JSON")
    private String replyType;

    @Option(names = "--object-id-format", description = "ObjectId format. Valid values: hex, bytes. Default: hex.", defaultValue = "hex")
    private String objectIdFormat;

    @Option(names = "--tls", description = "Establish a secure TLS connection.")
    private boolean tls;

    @Option(names = "--sni", description = "Server name indication for TLS.")
    private String sni;

    @Option(names = "--cacert", description = "CA certificate file to verify with (PEM format).")
    private File cacert;

    @Option(names = "--cacertdir", description = "Directory of trusted CA certificate files (PEM format).")
    private File cacertdir;

    @Option(names = "--insecure", description = "Allow insecure TLS connection by skipping cert validation.")
    private boolean insecure;

    @Option(names = "--tls-ciphers", description = "Colon-separated list of TLSv1.2 cipher suites.")
    private String tlsCiphers;

    @Option(names = "--tls-ciphersuites", description = "Colon-separated list of TLSv1.3 cipher suites.")
    private String tlsCiphersuites;

    private Socket socket;
    private Terminal terminal;
    private RespReader respReader;
    private RespWriter respWriter;
    private ResponseFormatter responseFormatter;
    private JsonResponseFormatter jsonFormatter;
    private RawResponseFormatter rawFormatter;
    private CsvResponseFormatter csvFormatter;
    private CommandLineParser commandLineParser;
    private boolean useRawOutput;
    private boolean connected;
    private String connectionError;
    private CommandCompleter completer;
    private CommandDocsCatalog commandDocs;
    private TailTipWidgets argumentHints;

    public static void main(String[] args) {
        int exitCode = new CommandLine(new KronotopCLI()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        commandLineParser = new CommandLineParser(quotedInput);

        try {
            connect();
            connected = true;
            // Default to RESP3 unless -2 is explicitly set
            if (!resp2) {
                resp3 = true;
            }
            switchProtocol();
            setSessionAttributes();
            initializeFormatter();
        } catch (IOException e) {
            connectionError = String.format("Could not connect to Kronotop at %s:%d: %s", host, port, e.getMessage());
            System.err.println(connectionError);
            return 1;
        }

        try {
            if (scriptFile != null) {
                try {
                    return executeScript(scriptFile);
                } catch (IOException e) {
                    System.err.println("Error reading script file: " + e.getMessage());
                    return 1;
                }
            }

            if (command != null && !command.isEmpty()) {
                List<String> processedCommand = command.stream()
                        .map(CommandLineParser::processEscapes)
                        .toList();
                long intervalMs = (long) (interval * 1000);
                String parsedResponseDelimiter = CommandLineParser.parseDelimiter(responseDelimiter);
                for (int i = 0; i < repeat; i++) {
                    if (executeDotCommand(processedCommand) == null) {
                        executeCommand(processedCommand, true);
                    }
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
            boolean jsonReplyType = fetchJsonReplyType();
            if (!noColor && terminal != null) {
                responseFormatter = new ResponseFormatter(jsonReplyType, terminal);
            } else {
                responseFormatter = new ResponseFormatter(jsonReplyType);
            }
        }
    }

    private boolean fetchJsonReplyType() throws IOException {
        respWriter.writeCommand(List.of("SESSION.ATTRIBUTE", "list"));
        RespValue response = respReader.read();

        if (response instanceof RespValue.Array(List<RespValue> values)) {
            for (int i = 0; i < values.size() - 1; i += 2) {
                RespValue key = values.get(i);
                RespValue value = values.get(i + 1);

                if (key instanceof RespValue.SimpleString(String value1) && value1.equals("reply_type")) {
                    if (value instanceof RespValue.SimpleString(String value2)) {
                        return value2.equals("json");
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

        if (tls) {
            try {
                CLITLSConfig tlsConfig = new CLITLSConfig(
                        insecure, sni, cacert, cacertdir,
                        tlsCiphers, tlsCiphersuites);
                socket = tlsConfig.wrapSocket(socket, host, port);
            } catch (GeneralSecurityException | IllegalArgumentException e) {
                throw new IOException("TLS setup failed: " + e.getMessage(), e);
            }
        }

        respReader = new RespReader(socket.getInputStream());
        respWriter = new RespWriter(socket.getOutputStream());
    }

    private void switchProtocol() throws IOException {
        int protocolVersion = resp2 ? 2 : 3;
        respWriter.writeCommand(List.of("HELLO", String.valueOf(protocolVersion)));
        respReader.read();
    }

    private void setSessionAttributes() throws IOException {
        // Pipeline all session attribute commands
        respWriter.writeCommandNoFlush(List.of("SESSION.ATTRIBUTE", "SET", "input_type", inputType));
        respWriter.writeCommandNoFlush(List.of("SESSION.ATTRIBUTE", "SET", "reply_type", replyType));
        respWriter.writeCommandNoFlush(List.of("SESSION.ATTRIBUTE", "SET", "object_id_format", objectIdFormat));
        respWriter.flush();

        // Read all responses
        RespValue response = respReader.read();
        if (response instanceof RespValue.SimpleError error) {
            throw new IOException("Failed to set input_type: " + error.message());
        }

        response = respReader.read();
        if (response instanceof RespValue.SimpleError error) {
            throw new IOException("Failed to set reply_type: " + error.message());
        }

        response = respReader.read();
        if (response instanceof RespValue.SimpleError error) {
            throw new IOException("Failed to set object_id_format: " + error.message());
        }
    }

    /**
     * Fetches COMMAND DOCS and builds the catalog. Returns null when the server does not answer.
     */
    private CommandDocsCatalog fetchCommandDocs() throws IOException {
        respWriter.writeCommand(List.of("COMMAND", "DOCS"));
        RespValue reply = respReader.read();
        if (!(reply instanceof RespValue.RespMap) && !(reply instanceof RespValue.Array)) {
            return null;
        }
        return new CommandDocsCatalog(reply);
    }

    /**
     * Loads the catalog into the completer. Runs after every successful connection in interactive mode.
     */
    private void loadCommandDocs() throws IOException {
        if (completer == null) {
            return;
        }
        commandDocs = fetchCommandDocs();
        completer.setCatalog(commandDocs);
    }

    /**
     * Prints help. Runs locally, nothing is sent to the server.
     * Without arguments it prints the generic help. "@group" lists a group, any other
     * words select every command whose name starts with them.
     */
    private void printHelp(List<String> args) {
        if (args.size() < 2) {
            out.println(HelpFormatter.generic(versionLine()));
            return;
        }
        if (commandDocs == null) {
            out.println("Command docs are not available. Connect to a server first.");
            return;
        }
        List<String> words = args.subList(1, args.size());
        Terminal colors = noColor ? null : terminal;
        String topic = words.getFirst();
        if (topic.startsWith("@")) {
            for (CommandDocsCatalog.CommandDoc doc : commandDocs.byGroup(topic.substring(1))) {
                out.print(HelpFormatter.format(doc, colors, false));
            }
        } else {
            for (CommandDocsCatalog.CommandDoc doc : commandDocs.find(words)) {
                out.print(HelpFormatter.format(doc, colors, true));
            }
        }
        out.println();
    }

    private String versionLine() {
        try {
            return new VersionProvider().getVersion()[0];
        } catch (Exception e) {
            return "kronotop-cli";
        }
    }

    /**
     * Handles ":set hints" and ":set nohints". Returns false for any other value.
     */
    private boolean setPreference(List<String> args) {
        if (args.size() != 2) {
            return false;
        }
        switch (args.get(1).toLowerCase()) {
            case "hints" -> argumentHints.enable();
            case "nohints" -> argumentHints.disable();
            default -> {
                return false;
            }
        }
        return true;
    }

    /**
     * Replaces the JLine completion colors with the terminal default.
     * JLine falls back to its own colors when a style resolves to the empty style,
     * so "fg:default" and "bg:default" are used instead of an empty value.
     */
    private static void disableCompletionColors(LineReaderBuilder builder) {
        for (String name : List.of(
                LineReader.COMPLETION_STYLE_STARTING,
                LineReader.COMPLETION_STYLE_LIST_STARTING,
                LineReader.COMPLETION_STYLE_DESCRIPTION,
                LineReader.COMPLETION_STYLE_LIST_DESCRIPTION,
                LineReader.COMPLETION_STYLE_GROUP,
                LineReader.COMPLETION_STYLE_LIST_GROUP)) {
            builder.variable(name, "fg:default");
        }
        builder.variable(LineReader.COMPLETION_STYLE_LIST_BACKGROUND, "bg:default");
    }

    /**
     * Shows argument hints after the cursor. Hints appear once the catalog is loaded.
     */
    private void enableArgumentHints(LineReader reader) {
        argumentHints = new TailTipWidgets(reader, completer::lookup, 0, TailTipWidgets.TipType.TAIL_TIP);
        argumentHints.setDescriptionCache(false);
        argumentHints.enable();
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

        try (Terminal term = TerminalBuilder.builder()
                .system(true)
                .build()) {

            this.terminal = term;

            // Reinitialize formatter with terminal for colorized output
            if (connected && responseFormatter != null && !noColor) {
                try {
                    initializeFormatter();
                } catch (IOException ignored) {
                    // Keep the existing formatter if reinitialization fails
                }
            }

            DefaultHistory history = new DefaultHistory();
            completer = new CommandCompleter();

            LineReaderBuilder readerBuilder = LineReaderBuilder.builder()
                    .terminal(term)
                    .parser(new MultiLineParser())
                    .completer(completer);

            if (noColor) {
                disableCompletionColors(readerBuilder);
            } else {
                readerBuilder.highlighter(new CommandHighlighter());
            }

            LineReader reader = readerBuilder
                    .option(LineReader.Option.DISABLE_EVENT_EXPANSION, true)
                    .option(LineReader.Option.CASE_INSENSITIVE, true)
                    .history(history)
                    .variable(LineReader.HISTORY_FILE, historyFile)
                    .variable(LineReader.SECONDARY_PROMPT_PATTERN, "...> ")
                    .build();

            history.attach(reader);
            enableArgumentHints(reader);
            if (connected) {
                loadCommandDocs();
            }

            while (true) {
                String prompt = connected ? String.format("%s:%d> ", host, port) : "not connected> ";
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
                    if (args.getFirst().equalsIgnoreCase("help")) {
                        printHelp(args);
                    } else if (args.getFirst().equalsIgnoreCase(":set")) {
                        if (!setPreference(args)) {
                            System.err.println("Usage: :set hints|nohints");
                        }
                    } else if (executeDotCommand(args) == null) {
                        executeCommand(args, true);
                    }
                }
            }

            return 0;
        } catch (IOException e) {
            System.err.println("Error initializing terminal: " + e.getMessage());
            return 1;
        }
    }

    private int executeScript(String filePath) throws IOException {
        List<String> lines = Files.readAllLines(Path.of(filePath));
        boolean inTransaction = false;

        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).strip();

            // Skip blank lines and comments
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }

            List<String> args = commandLineParser.parse(line);
            if (args.isEmpty()) {
                continue;
            }

            // Handle dot commands locally
            Boolean dotResult = executeDotCommand(args);
            if (dotResult != null) {
                if (!dotResult) {
                    System.err.println("Script aborted at line " + (i + 1));
                    return 1;
                }
                continue;
            }

            // Track transaction state
            String cmd = args.getFirst().toUpperCase();
            if (cmd.equals("BEGIN")) {
                inTransaction = true;
            }

            // Execute command (prints formatted response unless silent)
            RespValue response = executeCommand(args, !silent);

            // Update transaction state after successful COMMIT/ROLLBACK
            if (cmd.equals("COMMIT") || cmd.equals("ROLLBACK")) {
                inTransaction = false;
            }

            // Check for errors
            if (response instanceof RespValue.SimpleError || response instanceof RespValue.BlobError) {
                if (inTransaction) {
                    System.err.println("Auto-rolling back transaction due to error.");
                    executeCommand(List.of("ROLLBACK"), true);
                }
                System.err.println("Script aborted at line " + (i + 1));
                return 1;
            }

            // Connection lost
            if (response == null) {
                System.err.println("Script aborted at line " + (i + 1));
                return 1;
            }
        }
        return 0;
    }

    private long parseDuration(String input) {
        Matcher m = Pattern.compile("^(\\d+)(ms|s|m|h)$").matcher(input);
        if (!m.matches()) return -1;
        long value = Long.parseLong(m.group(1));
        return switch (m.group(2)) {
            case "ms" -> value;
            case "s" -> value * 1000;
            case "m" -> value * 60_000;
            case "h" -> value * 3_600_000;
            default -> -1;
        };
    }

    /**
     * Returns null if not a dot command, true on success, false on error.
     */
    private Boolean executeDotCommand(List<String> args) {
        String cmd = args.getFirst();
        if (!cmd.startsWith(".")) return null;

        switch (cmd.toLowerCase()) {
            case ".echo" -> {
                String text = String.join(" ", args.subList(1, args.size()));
                text = text.replace(TIMESTAMP_VAR, Instant.now().toString());
                out.println(text);
            }
            case ".sleep" -> {
                if (args.size() < 2) {
                    System.err.println("Usage: .sleep <duration> (e.g., 1s, 100ms, 1h)");
                    return false;
                }
                long ms = parseDuration(args.get(1));
                if (ms < 0) {
                    System.err.println("Invalid duration: " + args.get(1) + " (e.g., 1s, 100ms, 1h)");
                    return false;
                }
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(ms));
            }
            default -> {
                System.err.println("Unknown dot command: " + cmd);
                return false;
            }
        }
        return true;
    }

    private RespValue executeCommand(List<String> args, boolean printOutput) {
        if (!connected) {
            try {
                connect();
                connected = true;
                if (resp2 || resp3) {
                    switchProtocol();
                }
                setSessionAttributes();
                initializeFormatter();
                loadCommandDocs();
            } catch (IOException e) {
                connected = false;
                connectionError = String.format("Could not connect to Kronotop at %s:%d: %s", host, port, e.getMessage());
                System.err.println(connectionError);
                return null;
            }
        }

        try {
            respWriter.writeCommand(args);
            RespValue response = respReader.read();
            if (args.getFirst().equalsIgnoreCase("SESSION.CLOSE")
                    && response instanceof RespValue.SimpleString(String s) && s.equals("OK")) {
                setSessionAttributes();
            }
            if (printOutput) {
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
            }
            return response;
        } catch (IOException e) {
            connected = false;
            connectionError = String.format("Could not connect to Kronotop at %s:%d: %s", host, port, e.getMessage());
            System.err.println(connectionError);
            return null;
        }
    }
}
