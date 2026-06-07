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

/**
 * Utility methods for JSON syntax parsing shared by CommandHighlighter and JsonColorizer.
 */
public final class JsonSyntaxUtil {

    private JsonSyntaxUtil() {
    }

    /**
     * Checks if the position is at a valid JSON token boundary.
     */
    public static boolean isValidTokenEnd(String json, int pos) {
        if (pos >= json.length()) {
            return true;
        }
        char c = json.charAt(pos);
        return Character.isWhitespace(c) || c == ',' || c == '}' || c == ']' || c == ':';
    }

    /**
     * Scans a number and returns the end position.
     */
    public static int scanNumberEnd(String json, int pos) {
        while (pos < json.length()) {
            char c = json.charAt(pos);
            if (c == '-' || c == '+' || c == '.' || c == 'e' || c == 'E' || Character.isDigit(c)) {
                pos++;
            } else {
                break;
            }
        }
        return pos;
    }

    /**
     * Parses a quoted string (single or double quotes) with escape handling.
     */
    public static ParsedString parseString(String json, int pos) {
        char quote = json.charAt(pos);
        StringBuilder sb = new StringBuilder();
        sb.append(quote);
        pos++;

        while (pos < json.length()) {
            char c = json.charAt(pos);
            sb.append(c);
            pos++;

            if (c == '\\' && pos < json.length()) {
                sb.append(json.charAt(pos));
                pos++;
            } else if (c == quote) {
                break;
            }
        }

        return new ParsedString(sb.toString(), pos);
    }

    /**
     * Checks if the position (after skipping whitespace) is followed by a colon.
     */
    public static boolean isFollowedByColon(String json, int pos) {
        while (pos < json.length() && Character.isWhitespace(json.charAt(pos))) {
            pos++;
        }
        return pos < json.length() && json.charAt(pos) == ':';
    }

    /**
     * Scans an unquoted identifier and returns the end position.
     */
    public static int scanIdentifierEnd(String json, int pos) {
        while (pos < json.length()) {
            char c = json.charAt(pos);
            if (Character.isLetterOrDigit(c) || c == '_' || c == '.' || c == '-') {
                pos++;
            } else {
                break;
            }
        }
        return pos;
    }

    /**
     * Scans an operator (starting with $) and returns the end position.
     */
    public static int scanOperatorEnd(String json, int pos) {
        if (pos < json.length() && json.charAt(pos) == '$') {
            pos++;
        }
        while (pos < json.length()) {
            char c = json.charAt(pos);
            if (Character.isLetterOrDigit(c) || c == '_') {
                pos++;
            } else {
                break;
            }
        }
        return pos;
    }

    /**
     * Checks if the input is complete JSON (all brackets and quotes are balanced).
     *
     * @param input the input string to check
     * @return true if input is complete, false if more input is needed
     */
    public static boolean isJsonComplete(String input) {
        int braceCount = 0;
        int bracketCount = 0;
        boolean inString = false;
        char stringChar = 0;

        for (int i = 0; i < input.length(); i++) {
            char c = input.charAt(i);

            if (inString) {
                if (c == '\\' && i + 1 < input.length()) {
                    i++; // Skip escaped character
                } else if (c == stringChar) {
                    inString = false;
                }
            } else {
                switch (c) {
                    case '"', '\'' -> {
                        inString = true;
                        stringChar = c;
                    }
                    case '{' -> braceCount++;
                    case '}' -> braceCount--;
                    case '[' -> bracketCount++;
                    case ']' -> bracketCount--;
                }
            }
        }

        return !inString && braceCount == 0 && bracketCount == 0;
    }

    /**
     * Result of parsing a quoted string.
     */
    public record ParsedString(String content, int endPos) {
    }
}
