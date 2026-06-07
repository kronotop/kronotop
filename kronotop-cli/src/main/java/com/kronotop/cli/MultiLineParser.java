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

import org.jline.reader.EOFError;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.jline.reader.SyntaxError;
import org.jline.reader.impl.DefaultParser;

/**
 * A JLine parser that supports multi-line JSON input.
 * When brackets or quotes are unbalanced, signals JLine to continue reading.
 */
public class MultiLineParser implements Parser {

    private final DefaultParser defaultParser = new DefaultParser();

    @Override
    public ParsedLine parse(String line, int cursor, ParseContext context) throws SyntaxError {
        if (context == ParseContext.ACCEPT_LINE) {
            if (!JsonSyntaxUtil.isJsonComplete(line)) {
                throw new EOFError(-1, cursor, "Incomplete JSON", "...> ");
            }
        }
        return defaultParser.parse(line, cursor, context);
    }

    @Override
    public boolean isEscapeChar(char ch) {
        return defaultParser.isEscapeChar(ch);
    }

    @Override
    public boolean validCommandName(String name) {
        return defaultParser.validCommandName(name);
    }

    @Override
    public boolean validVariableName(String name) {
        return defaultParser.validVariableName(name);
    }

    @Override
    public String getCommand(String line) {
        return defaultParser.getCommand(line);
    }

    @Override
    public String getVariable(String line) {
        return defaultParser.getVariable(line);
    }
}
