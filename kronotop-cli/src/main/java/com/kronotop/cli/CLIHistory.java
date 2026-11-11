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

import org.jline.reader.impl.history.DefaultHistory;

import java.util.List;

/**
 * Keeps commands that carry a password out of the history file. They stay in the history of the
 * running session.
 */
class CLIHistory extends DefaultHistory {
    private final CommandLineParser parser;

    CLIHistory(CommandLineParser parser) {
        this.parser = parser;
    }

    // AUTH always carries a password. HELLO is sensitive as soon as AUTH appears among its arguments,
    // even when the rest of the line is incomplete or mistyped.
    static boolean isSensitive(List<String> args) {
        if (args.isEmpty()) {
            return false;
        }
        if (args.getFirst().equalsIgnoreCase("AUTH")) {
            return true;
        }
        if (args.getFirst().equalsIgnoreCase("HELLO")) {
            for (int i = 1; i < args.size(); i++) {
                if (args.get(i).equalsIgnoreCase("AUTH")) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean isPersistable(Entry entry) {
        return !isSensitive(parser.parse(entry.line()));
    }
}
