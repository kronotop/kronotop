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

import org.jline.terminal.Terminal;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

/**
 * Colorizes JSON strings for terminal output using ANSI colors.
 */
public class JsonColorizer {

    private static final AttributedStyle KEY_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.BLUE);
    private static final AttributedStyle STRING_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN);
    private static final AttributedStyle NUMBER_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW);
    private static final AttributedStyle BOOLEAN_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.MAGENTA);
    private static final AttributedStyle NULL_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.RED);
    private static final AttributedStyle BRACE_STYLE = AttributedStyle.DEFAULT;
    private static final AttributedStyle DEFAULT_STYLE = AttributedStyle.DEFAULT;

    private final Terminal terminal;
    private String json;
    private int pos;

    public JsonColorizer(Terminal terminal) {
        this.terminal = terminal;
    }

    public String colorize(String json) {
        if (json == null || json.isEmpty()) {
            return json;
        }

        this.json = json;
        this.pos = 0;

        AttributedStringBuilder builder = new AttributedStringBuilder();
        colorizeValue(builder);
        return builder.toAnsi(terminal);
    }

    private void colorizeValue(AttributedStringBuilder builder) {
        skipWhitespace(builder);
        if (pos >= json.length()) {
            return;
        }

        char c = json.charAt(pos);
        switch (c) {
            case '{' -> colorizeObject(builder);
            case '[' -> colorizeArray(builder);
            case '"', '\'' -> colorizeString(builder, STRING_STYLE);
            case 't' -> {
                if (json.startsWith("true", pos) && JsonSyntaxUtil.isValidTokenEnd(json, pos + 4)) {
                    colorizeBoolean(builder);
                } else {
                    colorizeUnquotedIdentifier(builder);
                }
            }
            case 'f' -> {
                if (json.startsWith("false", pos) && JsonSyntaxUtil.isValidTokenEnd(json, pos + 5)) {
                    colorizeBoolean(builder);
                } else {
                    colorizeUnquotedIdentifier(builder);
                }
            }
            case 'n' -> {
                if (json.startsWith("null", pos) && JsonSyntaxUtil.isValidTokenEnd(json, pos + 4)) {
                    colorizeNull(builder);
                } else {
                    colorizeUnquotedIdentifier(builder);
                }
            }
            default -> {
                if (c == '-' || Character.isDigit(c)) {
                    int numEnd = JsonSyntaxUtil.scanNumberEnd(json, pos);
                    if (JsonSyntaxUtil.isValidTokenEnd(json, numEnd)) {
                        colorizeNumber(builder);
                    } else {
                        colorizeUnquotedIdentifier(builder);
                    }
                } else {
                    builder.append(c);
                    pos++;
                }
            }
        }
    }

    private void colorizeUnquotedIdentifier(AttributedStringBuilder builder) {
        int endPos = JsonSyntaxUtil.scanIdentifierEnd(json, pos);
        if (endPos > pos) {
            builder.styled(DEFAULT_STYLE, json.substring(pos, endPos));
            pos = endPos;
        }
    }

    private void colorizeObject(AttributedStringBuilder builder) {
        builder.styled(BRACE_STYLE, "{");
        pos++;

        boolean first = true;
        while (pos < json.length()) {
            skipWhitespace(builder);
            if (pos >= json.length()) break;

            char c = json.charAt(pos);
            if (c == '}') {
                builder.styled(BRACE_STYLE, "}");
                pos++;
                return;
            }

            if (!first) {
                if (c == ',') {
                    builder.styled(DEFAULT_STYLE, ",");
                    pos++;
                    skipWhitespace(builder);
                }
            }
            first = false;

            // Key
            if (pos < json.length() && (json.charAt(pos) == '"' || json.charAt(pos) == '\'')) {
                colorizeString(builder, KEY_STYLE);
            }

            skipWhitespace(builder);

            // Colon
            if (pos < json.length() && json.charAt(pos) == ':') {
                builder.styled(DEFAULT_STYLE, ":");
                pos++;
            }

            skipWhitespace(builder);

            // Value
            colorizeValue(builder);
        }
    }

    private void colorizeArray(AttributedStringBuilder builder) {
        builder.styled(BRACE_STYLE, "[");
        pos++;

        boolean first = true;
        while (pos < json.length()) {
            skipWhitespace(builder);
            if (pos >= json.length()) break;

            char c = json.charAt(pos);
            if (c == ']') {
                builder.styled(BRACE_STYLE, "]");
                pos++;
                return;
            }

            if (!first) {
                if (c == ',') {
                    builder.styled(DEFAULT_STYLE, ",");
                    pos++;
                    skipWhitespace(builder);
                }
            }
            first = false;

            colorizeValue(builder);
        }
    }

    private void colorizeString(AttributedStringBuilder builder, AttributedStyle style) {
        JsonSyntaxUtil.ParsedString parsed = JsonSyntaxUtil.parseString(json, pos);
        builder.styled(style, parsed.content());
        pos = parsed.endPos();
    }

    private void colorizeNumber(AttributedStringBuilder builder) {
        int endPos = JsonSyntaxUtil.scanNumberEnd(json, pos);
        builder.styled(NUMBER_STYLE, json.substring(pos, endPos));
        pos = endPos;
    }

    private void colorizeBoolean(AttributedStringBuilder builder) {
        if (json.startsWith("true", pos)) {
            builder.styled(BOOLEAN_STYLE, "true");
            pos += 4;
        } else if (json.startsWith("false", pos)) {
            builder.styled(BOOLEAN_STYLE, "false");
            pos += 5;
        }
    }

    private void colorizeNull(AttributedStringBuilder builder) {
        if (json.startsWith("null", pos)) {
            builder.styled(NULL_STYLE, "null");
            pos += 4;
        }
    }

    private void skipWhitespace(AttributedStringBuilder builder) {
        while (pos < json.length() && Character.isWhitespace(json.charAt(pos))) {
            builder.append(json.charAt(pos));
            pos++;
        }
    }
}
