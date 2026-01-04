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

import java.util.ArrayList;
import java.util.List;

/**
 * Parses command line input into a list of arguments.
 */
public class CommandLineParser {

    private final boolean quotedInput;

    public CommandLineParser() {
        this(false);
    }

    public CommandLineParser(boolean quotedInput) {
        this.quotedInput = quotedInput;
    }

    public List<String> parse(String line) {
        List<String> args = new ArrayList<>();
        StringBuilder current = new StringBuilder();
        boolean inQuotes = false;
        char quoteChar = 0;

        for (int i = 0; i < line.length(); i++) {
            char c = line.charAt(i);

            if (inQuotes) {
                if (c == quoteChar) {
                    inQuotes = false;
                } else if (c == '\\' && i + 1 < line.length()) {
                    i = parseEscape(line, i, current);
                } else {
                    current.append(c);
                }
            } else if (c == '"' || c == '\'') {
                inQuotes = true;
                quoteChar = c;
            } else if (Character.isWhitespace(c)) {
                if (!current.isEmpty()) {
                    args.add(current.toString());
                    current = new StringBuilder();
                }
            } else if (quotedInput && c == '\\') {
                if (i + 1 < line.length()) {
                    i = parseEscape(line, i, current);
                }
                // Trailing backslash is ignored
            } else {
                current.append(c);
            }
        }

        if (!current.isEmpty()) {
            args.add(current.toString());
        }

        return args;
    }

    public static String parseDelimiter(String input) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);
            if (c == '\\' && i + 1 < input.length()) {
                char next = input.charAt(++i);
                switch (next) {
                    case 'n' -> sb.append('\n');
                    case 'r' -> sb.append('\r');
                    case 't' -> sb.append('\t');
                    case '\\' -> sb.append('\\');
                    default -> sb.append(next);
                }
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private int parseEscape(String line, int i, StringBuilder current) {
        char next = line.charAt(++i);
        switch (next) {
            case 'n' -> current.append('\n');
            case 'r' -> current.append('\r');
            case 't' -> current.append('\t');
            case 'x' -> {
                if (i + 2 < line.length()) {
                    String hex = line.substring(i + 1, i + 3);
                    try {
                        current.append((char) Integer.parseInt(hex, 16));
                        i += 2;
                    } catch (NumberFormatException e) {
                        // Invalid hex: keep original \x as literal
                        current.append('\\').append(next);
                    }
                } else {
                    // Incomplete hex: keep original \x as literal
                    current.append('\\').append(next);
                }
            }
            default -> current.append(next);
        }
        return i;
    }
}
