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

import org.jline.console.CmdDesc;
import org.jline.console.CmdLine;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;

import java.util.List;
import java.util.Locale;

/**
 * Completes command names and subcommand names from the COMMAND DOCS catalog.
 * Candidates are uppercase. The catalog can be set after the reader is built.
 */
public class CommandCompleter implements Completer {

    private volatile CommandDocsCatalog catalog;

    public void setCatalog(CommandDocsCatalog catalog) {
        this.catalog = catalog;
    }

    /**
     * Returns the argument hint for the line, or null when no catalog is loaded or the command is unknown.
     */
    public CmdDesc lookup(CmdLine line) {
        CommandDocsCatalog current = catalog;
        return current == null ? null : current.lookup(line);
    }

    @Override
    public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
        CommandDocsCatalog current = catalog;
        if (current == null) {
            return;
        }
        if (line.wordIndex() == 0) {
            addAll(candidates, current.commandNames());
        } else if (line.wordIndex() == 1) {
            addAll(candidates, current.subcommandNames(line.words().get(0)));
        }
    }

    private static void addAll(List<Candidate> candidates, List<String> names) {
        for (String name : names) {
            candidates.add(new Candidate(name.toUpperCase(Locale.ROOT)));
        }
    }
}
