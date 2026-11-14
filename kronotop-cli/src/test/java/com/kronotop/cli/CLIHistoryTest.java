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

import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.impl.DumbTerminal;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CLIHistoryTest {

    @Test
    void shouldTreatAuthAsSensitive() {
        // Behavior: AUTH is sensitive with a password alone and with a user and a password, in any letter case.
        assertTrue(CLIHistory.isSensitive(List.of("AUTH", "secret")));
        assertTrue(CLIHistory.isSensitive(List.of("auth", "devuser", "secret")));
    }

    @Test
    void shouldTreatHelloWithAuthOptionAsSensitive() {
        // Behavior: HELLO is sensitive when AUTH appears anywhere among its arguments, in any letter case. This
        // covers the complete form, AUTH after the SETNAME option, and an incomplete AUTH option with one value.
        assertTrue(CLIHistory.isSensitive(List.of("HELLO", "3", "AUTH", "devuser", "secret")));
        assertTrue(CLIHistory.isSensitive(List.of("hello", "3", "SETNAME", "app", "auth", "devuser", "secret")));
        assertTrue(CLIHistory.isSensitive(List.of("hello", "3", "auth", "secret")));
    }

    @Test
    void shouldNotTreatCommandsWithoutPasswordAsSensitive() {
        // Behavior: HELLO without the AUTH option, HELLO with only SETNAME, and other commands are not sensitive.
        assertFalse(CLIHistory.isSensitive(List.of("HELLO", "3")));
        assertFalse(CLIHistory.isSensitive(List.of("HELLO", "3", "SETNAME", "app")));
        assertFalse(CLIHistory.isSensitive(List.of("PING")));
        assertFalse(CLIHistory.isSensitive(List.of()));
    }

    @Test
    void shouldKeepSensitiveCommandsOutOfHistoryFile(@TempDir Path dir) throws IOException {
        // Behavior: AUTH secret, HELLO 3 AUTH devuser secret and PING are added to the history and saved. All
        // three stay in the history of the session, but the history file holds only the PING line.
        Path historyFile = dir.resolve("history");
        try (Terminal terminal = new DumbTerminal(new ByteArrayInputStream(new byte[0]), new ByteArrayOutputStream())) {
            CLIHistory history = new CLIHistory(new CommandLineParser());
            LineReader reader = LineReaderBuilder.builder()
                    .terminal(terminal)
                    .history(history)
                    .variable(LineReader.HISTORY_FILE, historyFile)
                    .build();
            history.attach(reader);

            history.add("AUTH secret");
            history.add("HELLO 3 AUTH devuser secret");
            history.add("PING");
            history.save();

            assertEquals(3, history.size());
            List<String> saved = Files.readAllLines(historyFile);
            assertEquals(1, saved.size());
            assertTrue(saved.getFirst().endsWith("PING"));
        }
    }
}
