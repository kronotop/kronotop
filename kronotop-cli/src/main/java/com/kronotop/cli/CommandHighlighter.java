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

import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;

import java.util.Set;
import java.util.regex.Pattern;

/**
 * Highlights command input with syntax coloring for commands and JSON arguments.
 */
public class CommandHighlighter implements Highlighter {

    private static final AttributedStyle COMMAND_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.BLUE).bold();
    private static final AttributedStyle ARGUMENT_STYLE = AttributedStyle.DEFAULT;
    private static final AttributedStyle KEY_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.BLUE);
    private static final AttributedStyle STRING_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN);
    private static final AttributedStyle NUMBER_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW);
    private static final AttributedStyle BOOLEAN_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.MAGENTA);
    private static final AttributedStyle NULL_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.RED);
    private static final AttributedStyle BRACE_STYLE = AttributedStyle.DEFAULT;
    private static final AttributedStyle OPERATOR_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.MAGENTA).bold();
    private static final AttributedStyle DEFAULT_STYLE = AttributedStyle.DEFAULT;

    private static final Set<String> JSON_COMMANDS = Set.of(
            "bucket.insert", "bucket.query", "query", "bucket.update", "bucket.delete", "bucket.explain",
            "bucket.createindex", "bucket.vector", "set", "setx", "zset", "mset", "hset", "hsetnx",
            "hmset"
    );

    @Override
    public AttributedString highlight(LineReader reader, String buffer) {
        if (buffer.isEmpty()) {
            return new AttributedString(buffer);
        }

        AttributedStringBuilder builder = new AttributedStringBuilder();
        int pos = 0;

        // Skip leading whitespace
        while (pos < buffer.length() && Character.isWhitespace(buffer.charAt(pos))) {
            builder.append(buffer.charAt(pos));
            pos++;
        }

        // Extract and highlight command
        int cmdStart = pos;
        while (pos < buffer.length() && !Character.isWhitespace(buffer.charAt(pos))) {
            pos++;
        }

        String command = buffer.substring(cmdStart, pos).toLowerCase();
        builder.styled(COMMAND_STYLE, buffer.substring(cmdStart, pos));

        boolean isJsonCommand = JSON_COMMANDS.contains(command);

        // Process arguments
        while (pos < buffer.length()) {
            char c = buffer.charAt(pos);

            if (Character.isWhitespace(c)) {
                builder.append(c);
                pos++;
            } else if (isJsonCommand && (c == '{' || c == '[' || c == '\'' || c == '"')) {
                pos = highlightJsonLike(buffer, pos, builder);
            } else {
                // Regular argument
                int argStart = pos;
                while (pos < buffer.length() && !Character.isWhitespace(buffer.charAt(pos))) {
                    char ch = buffer.charAt(pos);
                    if (isJsonCommand && (ch == '{' || ch == '[' || ch == '\'' || ch == '"')) {
                        break;
                    }
                    pos++;
                }
                if (pos > argStart) {
                    builder.styled(ARGUMENT_STYLE, buffer.substring(argStart, pos));
                }
            }
        }

        return builder.toAttributedString();
    }

    private int highlightJsonLike(String buffer, int pos, AttributedStringBuilder builder) {
        char startChar = buffer.charAt(pos);

        // Handle quoted JSON strings
        if (startChar == '"' || startChar == '\'') {
            builder.styled(DEFAULT_STYLE, String.valueOf(startChar));
            pos++;

            int jsonStart = pos;
            int depth = 0;
            boolean inString = false;
            char stringChar = 0;

            while (pos < buffer.length()) {
                char c = buffer.charAt(pos);

                if (inString) {
                    if (c == '\\' && pos + 1 < buffer.length()) {
                        pos += 2;
                        continue;
                    }
                    if (c == stringChar) {
                        inString = false;
                    }
                    pos++;
                } else {
                    if (c == '"' || c == '\'') {
                        if (depth == 0 && c == startChar) {
                            // End of quoted JSON
                            highlightJsonContent(buffer.substring(jsonStart, pos), builder);
                            builder.styled(DEFAULT_STYLE, String.valueOf(startChar));
                            return pos + 1;
                        }
                        inString = true;
                        stringChar = c;
                    } else if (c == '{' || c == '[') {
                        depth++;
                    } else if (c == '}' || c == ']') {
                        depth--;
                    }
                    pos++;
                }
            }

            // Unclosed quote - highlight what we have
            highlightJsonContent(buffer.substring(jsonStart, pos), builder);
            return pos;
        }

        // Handle unquoted JSON object/array
        return highlightUnquotedJson(buffer, pos, builder);
    }

    private int highlightUnquotedJson(String buffer, int pos, AttributedStringBuilder builder) {
        int start = pos;
        int depth = 0;
        boolean inString = false;
        char stringChar = 0;

        while (pos < buffer.length()) {
            char c = buffer.charAt(pos);

            if (inString) {
                if (c == '\\' && pos + 1 < buffer.length()) {
                    pos += 2;
                    continue;
                }
                if (c == stringChar) {
                    inString = false;
                }
                pos++;
            } else {
                if (c == '"' || c == '\'') {
                    inString = true;
                    stringChar = c;
                    pos++;
                } else if (c == '{' || c == '[') {
                    depth++;
                    pos++;
                } else if (c == '}' || c == ']') {
                    depth--;
                    pos++;
                    if (depth == 0) {
                        break;
                    }
                } else if (Character.isWhitespace(c) && depth == 0) {
                    break;
                } else {
                    pos++;
                }
            }
        }

        highlightJsonContent(buffer.substring(start, pos), builder);
        return pos;
    }

    private void highlightJsonContent(String json, AttributedStringBuilder builder) {
        int pos = 0;

        while (pos < json.length()) {
            char c = json.charAt(pos);

            if (Character.isWhitespace(c)) {
                builder.append(c);
                pos++;
            } else if (c == '{' || c == '}' || c == '[' || c == ']') {
                builder.styled(BRACE_STYLE, String.valueOf(c));
                pos++;
            } else if (c == ':' || c == ',') {
                builder.styled(DEFAULT_STYLE, String.valueOf(c));
                pos++;
            } else if (c == '"' || c == '\'') {
                pos = highlightString(json, pos, builder);
            } else if (c == '$') {
                pos = highlightOperator(json, pos, builder);
            } else if (c == '-' || Character.isDigit(c)) {
                int numEnd = JsonSyntaxUtil.scanNumberEnd(json, pos);
                if (JsonSyntaxUtil.isValidTokenEnd(json, numEnd)) {
                    builder.styled(NUMBER_STYLE, json.substring(pos, numEnd));
                    pos = numEnd;
                } else {
                    pos = highlightUnquotedIdentifier(json, pos, builder);
                }
            } else if (json.startsWith("true", pos) && JsonSyntaxUtil.isValidTokenEnd(json, pos + 4)) {
                builder.styled(BOOLEAN_STYLE, "true");
                pos += 4;
            } else if (json.startsWith("false", pos) && JsonSyntaxUtil.isValidTokenEnd(json, pos + 5)) {
                builder.styled(BOOLEAN_STYLE, "false");
                pos += 5;
            } else if (json.startsWith("null", pos) && JsonSyntaxUtil.isValidTokenEnd(json, pos + 4)) {
                builder.styled(NULL_STYLE, "null");
                pos += 4;
            } else {
                // Unquoted key or identifier
                int oldPos = pos;
                pos = highlightUnquotedIdentifier(json, pos, builder);
                // If no progress was made, append the character and move on
                if (pos == oldPos) {
                    builder.append(c);
                    pos++;
                }
            }
        }
    }

    private int highlightString(String json, int pos, AttributedStringBuilder builder) {
        JsonSyntaxUtil.ParsedString parsed = JsonSyntaxUtil.parseString(json, pos);
        String content = parsed.content();

        // Check if string content starts with $ (operator in value position)
        if (content.length() > 2 && content.charAt(1) == '$') {
            builder.styled(OPERATOR_STYLE, content);
        } else if (JsonSyntaxUtil.isFollowedByColon(json, parsed.endPos())) {
            builder.styled(KEY_STYLE, content);
        } else {
            builder.styled(STRING_STYLE, content);
        }

        return parsed.endPos();
    }

    private int highlightOperator(String json, int pos, AttributedStringBuilder builder) {
        int endPos = JsonSyntaxUtil.scanOperatorEnd(json, pos);
        builder.styled(OPERATOR_STYLE, json.substring(pos, endPos));
        return endPos;
    }

    private int highlightUnquotedIdentifier(String json, int pos, AttributedStringBuilder builder) {
        int endPos = JsonSyntaxUtil.scanIdentifierEnd(json, pos);

        if (endPos > pos) {
            String content = json.substring(pos, endPos);
            if (JsonSyntaxUtil.isFollowedByColon(json, endPos)) {
                builder.styled(KEY_STYLE, content);
            } else {
                builder.styled(DEFAULT_STYLE, content);
            }
        }

        return endPos;
    }

    @Override
    public void setErrorPattern(Pattern errorPattern) {
        // Not used
    }

    @Override
    public void setErrorIndex(int errorIndex) {
        // Not used
    }
}
