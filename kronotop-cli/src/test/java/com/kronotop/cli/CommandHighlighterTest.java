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

import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStyle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class CommandHighlighterTest {

    // Expected styles matching CommandHighlighter
    private static final AttributedStyle COMMAND_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.BLUE).bold();
    private static final AttributedStyle KEY_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.BLUE);
    private static final AttributedStyle STRING_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN);
    private static final AttributedStyle NUMBER_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW);
    private static final AttributedStyle BOOLEAN_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.MAGENTA);
    private static final AttributedStyle NULL_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.RED);
    private static final AttributedStyle OPERATOR_STYLE = AttributedStyle.DEFAULT.foreground(AttributedStyle.MAGENTA).bold();

    private final CommandHighlighter highlighter = new CommandHighlighter();

    private AttributedString highlight(String input) {
        return highlighter.highlight(null, input);
    }

    private void assertStyleAt(AttributedString str, int index, AttributedStyle expected) {
        AttributedStyle actual = str.styleAt(index);
        assertEquals(expected, actual, "Style mismatch at index " + index + " (char: '" + str.charAt(index) + "')");
    }

    // ==================== Command Style Tests ====================

    @Test
    void shouldColorCommandBlueAndBold() {
        // Behavior: Commands are styled with blue foreground and bold
        AttributedString result = highlight("GET key");

        // "GET" at indices 0,1,2 should be COMMAND_STYLE
        assertStyleAt(result, 0, COMMAND_STYLE); // G
        assertStyleAt(result, 1, COMMAND_STYLE); // E
        assertStyleAt(result, 2, COMMAND_STYLE); // T
    }

    @Test
    void shouldColorLowercaseCommandBlueAndBold() {
        // Behavior: Lowercase commands are also styled blue and bold
        AttributedString result = highlight("set key value");

        assertStyleAt(result, 0, COMMAND_STYLE); // s
        assertStyleAt(result, 1, COMMAND_STYLE); // e
        assertStyleAt(result, 2, COMMAND_STYLE); // t
    }

    // ==================== JSON Key Style Tests ====================

    @Test
    void shouldColorJsonKeyBlue() {
        // Behavior: JSON keys are styled with blue foreground (no bold)
        AttributedString result = highlight("set k {\"name\": \"value\"}");
        // Position: set k {"name": "value"}
        //           0123456789...
        // "name" starts at index 7 (including the quote)

        assertStyleAt(result, 7, KEY_STYLE);  // "
        assertStyleAt(result, 8, KEY_STYLE);  // n
        assertStyleAt(result, 9, KEY_STYLE);  // a
        assertStyleAt(result, 10, KEY_STYLE); // m
        assertStyleAt(result, 11, KEY_STYLE); // e
        assertStyleAt(result, 12, KEY_STYLE); // "
    }

    @Test
    void shouldColorSingleQuotedKeyBlue() {
        // Behavior: Single-quoted keys are also styled blue
        AttributedString result = highlight("set k {'name': 'value'}");

        assertStyleAt(result, 7, KEY_STYLE);  // '
        assertStyleAt(result, 8, KEY_STYLE);  // n
    }

    // ==================== JSON String Value Tests ====================

    @Test
    void shouldColorStringValueGreen() {
        // Behavior: JSON string values are styled green
        AttributedString result = highlight("set k {\"key\": \"value\"}");
        // Position: set k {"key": "value"}
        //           01234567890123456789012
        // "value" starts at index 14

        assertStyleAt(result, 14, STRING_STYLE); // "
        assertStyleAt(result, 15, STRING_STYLE); // v
        assertStyleAt(result, 16, STRING_STYLE); // a
        assertStyleAt(result, 17, STRING_STYLE); // l
        assertStyleAt(result, 18, STRING_STYLE); // u
        assertStyleAt(result, 19, STRING_STYLE); // e
        assertStyleAt(result, 20, STRING_STYLE); // "
    }

    // ==================== JSON Number Tests ====================

    @Test
    void shouldColorNumberYellow() {
        // Behavior: Numeric values are styled yellow
        AttributedString result = highlight("set k {\"count\": 42}");
        // Position: set k {"count": 42}
        //           0123456789012345678
        // 42 starts at index 16

        assertStyleAt(result, 16, NUMBER_STYLE); // 4
        assertStyleAt(result, 17, NUMBER_STYLE); // 2
    }

    @Test
    void shouldColorNegativeNumberYellow() {
        // Behavior: Negative numbers are styled yellow
        AttributedString result = highlight("set k {\"n\": -5}");
        // -5 starts at index 12

        assertStyleAt(result, 12, NUMBER_STYLE); // -
        assertStyleAt(result, 13, NUMBER_STYLE); // 5
    }

    // ==================== JSON Boolean Tests ====================

    @Test
    void shouldColorBooleanTrueMagenta() {
        // Behavior: Boolean true is styled magenta
        AttributedString result = highlight("set k {\"active\": true}");
        // Position: set k {"active": true}
        //           01234567890123456789012
        // true starts at index 17

        assertStyleAt(result, 17, BOOLEAN_STYLE); // t
        assertStyleAt(result, 18, BOOLEAN_STYLE); // r
        assertStyleAt(result, 19, BOOLEAN_STYLE); // u
        assertStyleAt(result, 20, BOOLEAN_STYLE); // e
    }

    @Test
    void shouldColorBooleanFalseMagenta() {
        // Behavior: Boolean false is styled magenta
        AttributedString result = highlight("set k {\"x\": false}");
        // false starts at index 12

        assertStyleAt(result, 12, BOOLEAN_STYLE); // f
        assertStyleAt(result, 13, BOOLEAN_STYLE); // a
        assertStyleAt(result, 14, BOOLEAN_STYLE); // l
        assertStyleAt(result, 15, BOOLEAN_STYLE); // s
        assertStyleAt(result, 16, BOOLEAN_STYLE); // e
    }

    // ==================== JSON Null Tests ====================

    @Test
    void shouldColorNullRed() {
        // Behavior: Null values are styled red
        AttributedString result = highlight("set k {\"x\": null}");
        // null starts at index 12

        assertStyleAt(result, 12, NULL_STYLE); // n
        assertStyleAt(result, 13, NULL_STYLE); // u
        assertStyleAt(result, 14, NULL_STYLE); // l
        assertStyleAt(result, 15, NULL_STYLE); // l
    }

    // ==================== Operator Tests ====================

    @Test
    void shouldColorOperatorMagentaBold() {
        // Behavior: Query operators like $gt are styled magenta and bold
        AttributedString result = highlight("bucket.query u {\"age\": {\"$gt\": 5}}");
        // Position: bucket.query u {"age": {"$gt": 5}}
        //           0         1         2         3
        //           0123456789012345678901234567890123456
        // "$gt" starts at index 24

        assertStyleAt(result, 24, OPERATOR_STYLE); // "
        assertStyleAt(result, 25, OPERATOR_STYLE); // $
        assertStyleAt(result, 26, OPERATOR_STYLE); // g
        assertStyleAt(result, 27, OPERATOR_STYLE); // t
        assertStyleAt(result, 28, OPERATOR_STYLE); // "
    }

    // ==================== Regression Tests (No Hangs) ====================

    @Test
    @Timeout(value = 1, unit = TimeUnit.SECONDS)
    void shouldHandleBackslashWithoutHanging() {
        // Behavior: Backslash character does not cause infinite loop
        AttributedString result = highlight("set key \\");
        assertNotNull(result);
        assertEquals("set key \\", result.toString());
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.SECONDS)
    void shouldHandleBackslashInJson() {
        // Behavior: Backslash in JSON context does not cause infinite loop
        AttributedString result = highlight("set key {\\}");
        assertNotNull(result);
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.SECONDS)
    void shouldHandleMultipleBackslashes() {
        // Behavior: Multiple backslashes do not cause infinite loop
        AttributedString result = highlight("set key \\\\\\");
        assertNotNull(result);
    }

    @Test
    @Timeout(value = 1, unit = TimeUnit.SECONDS)
    void shouldHandleSpecialCharacters() {
        // Behavior: Special characters like @#%^& do not cause hangs
        AttributedString result = highlight("set key {@#%^&}");
        assertNotNull(result);
    }

    // ==================== Content Preservation Tests ====================

    @Test
    void shouldPreserveTextContent() {
        // Behavior: All text content is preserved after highlighting
        String input = "bucket.query users {\"age\": {\"$gt\": 25}, \"active\": true}";
        AttributedString result = highlight(input);
        assertEquals(input, result.toString());
    }

    @Test
    void shouldHandleEmptyInput() {
        // Behavior: Empty input returns empty AttributedString
        AttributedString result = highlight("");
        assertEquals("", result.toString());
    }

    @Test
    void shouldPreserveLeadingWhitespace() {
        // Behavior: Leading whitespace is preserved
        AttributedString result = highlight("  GET key");
        assertEquals("  GET key", result.toString());
    }

    // ==================== Malformed Token Tests ====================

    @Test
    void shouldTreatNumberFollowedByLettersAsIdentifier() {
        // Behavior: 100abc is treated as identifier, not number + identifier
        AttributedString result = highlight("set k {\"x\": 100abc}");
        // The whole "100abc" should be DEFAULT_STYLE (unquoted identifier)
        // Position: set k {"x": 100abc}
        //           01234567890123456789
        // 100abc starts at index 12
        assertStyleAt(result, 12, AttributedStyle.DEFAULT); // 1
        assertStyleAt(result, 15, AttributedStyle.DEFAULT); // a
        assertEquals("set k {\"x\": 100abc}", result.toString());
    }

    @Test
    void shouldTreatTrueFollowedByLettersAsIdentifier() {
        // Behavior: trueXYZ is treated as identifier, not boolean + identifier
        AttributedString result = highlight("set k {\"x\": trueXYZ}");
        assertStyleAt(result, 12, AttributedStyle.DEFAULT); // t
        assertStyleAt(result, 15, AttributedStyle.DEFAULT); // X
        assertEquals("set k {\"x\": trueXYZ}", result.toString());
    }

    @Test
    void shouldTreatFalseFollowedByLettersAsIdentifier() {
        // Behavior: falseABC is treated as identifier, not boolean + identifier
        AttributedString result = highlight("set k {\"x\": falseABC}");
        assertStyleAt(result, 12, AttributedStyle.DEFAULT); // f
        assertStyleAt(result, 17, AttributedStyle.DEFAULT); // A
        assertEquals("set k {\"x\": falseABC}", result.toString());
    }

    @Test
    void shouldTreatNullFollowedByLettersAsIdentifier() {
        // Behavior: nullXYZ is treated as identifier, not null + identifier
        AttributedString result = highlight("set k {\"x\": nullXYZ}");
        assertStyleAt(result, 12, AttributedStyle.DEFAULT); // n
        assertStyleAt(result, 16, AttributedStyle.DEFAULT); // X
        assertEquals("set k {\"x\": nullXYZ}", result.toString());
    }

    @Test
    void shouldStillColorValidNumber() {
        // Behavior: Valid number followed by delimiter is still colored
        AttributedString result = highlight("set k {\"x\": 100}");
        assertStyleAt(result, 12, NUMBER_STYLE); // 1
        assertStyleAt(result, 14, NUMBER_STYLE); // 0
    }

    @Test
    void shouldStillColorValidBoolean() {
        // Behavior: Valid boolean followed by delimiter is still colored
        AttributedString result = highlight("set k {\"x\": true}");
        assertStyleAt(result, 12, BOOLEAN_STYLE); // t
    }

    @Test
    void shouldStillColorValidNull() {
        // Behavior: Valid null followed by delimiter is still colored
        AttributedString result = highlight("set k {\"x\": null}");
        assertStyleAt(result, 12, NULL_STYLE); // n
    }
}
