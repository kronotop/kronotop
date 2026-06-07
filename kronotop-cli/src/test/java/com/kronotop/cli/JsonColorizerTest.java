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
import org.jline.terminal.TerminalBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.*;

class JsonColorizerTest {

    // ANSI escape code patterns for colors (SGR codes)
    private static final String ANSI_BLUE = "34";
    private static final String ANSI_GREEN = "32";
    private static final String ANSI_YELLOW = "33";
    private static final String ANSI_MAGENTA = "35";
    private static final String ANSI_RED = "31";

    private JsonColorizer colorizer;
    private Terminal terminal;

    @BeforeEach
    void setUp() throws IOException {
        terminal = TerminalBuilder.builder()
                .type("xterm-256color")
                .streams(System.in, System.out)
                .build();
        colorizer = new JsonColorizer(terminal);
    }

    private boolean containsColorCode(String result, String colorCode) {
        // Check for ANSI SGR sequence containing the color code
        // Format: ESC[...;Nm or ESC[Nm
        return result.contains("\u001B[" + colorCode + "m") ||
                result.contains(";" + colorCode + "m") ||
                result.contains("\u001B[0;" + colorCode + "m");
    }

    private String stripAnsi(String s) {
        return s.replaceAll("\u001B\\[[;\\d]*m", "");
    }

    // ==================== Null/Empty Input Tests ====================

    @Test
    void shouldReturnNullForNullInput() {
        // Behavior: Null input returns null
        assertNull(colorizer.colorize(null));
    }

    @Test
    void shouldReturnEmptyForEmptyInput() {
        // Behavior: Empty input returns empty string
        assertEquals("", colorizer.colorize(""));
    }

    // ==================== Content Preservation Tests ====================

    @Test
    void shouldPreserveSimpleObject() {
        // Behavior: JSON object content is preserved after stripping ANSI codes
        String input = "{\"name\": \"value\"}";
        String result = stripAnsi(colorizer.colorize(input));
        assertEquals(input, result);
    }

    @Test
    void shouldPreserveNestedObject() {
        // Behavior: Nested JSON object content is preserved
        String input = "{\"user\": {\"name\": \"alice\", \"age\": 30}}";
        String result = stripAnsi(colorizer.colorize(input));
        assertEquals(input, result);
    }

    @Test
    void shouldPreserveArray() {
        // Behavior: JSON array content is preserved
        String input = "[1, 2, 3]";
        String result = stripAnsi(colorizer.colorize(input));
        assertEquals(input, result);
    }

    @Test
    void shouldPreserveArrayOfObjects() {
        // Behavior: Array of objects content is preserved
        String input = "[{\"a\": 1}, {\"b\": 2}]";
        String result = stripAnsi(colorizer.colorize(input));
        assertEquals(input, result);
    }

    @Test
    void shouldPreserveWhitespace() {
        // Behavior: Whitespace in JSON is preserved
        String input = "{ \"key\" : \"value\" }";
        String result = stripAnsi(colorizer.colorize(input));
        assertEquals(input, result);
    }

    // ==================== Key Colorization Tests ====================

    @Test
    void shouldColorizeKeyBlue() {
        // Behavior: JSON keys are colorized with blue ANSI code
        String result = colorizer.colorize("{\"name\": \"value\"}");
        assertTrue(containsColorCode(result, ANSI_BLUE), "Key should be colored blue");
    }

    @Test
    void shouldColorizeSingleQuotedKeyBlue() {
        // Behavior: Single-quoted keys are also colorized blue
        String result = colorizer.colorize("{'name': 'value'}");
        assertTrue(containsColorCode(result, ANSI_BLUE), "Single-quoted key should be colored blue");
    }

    // ==================== String Value Colorization Tests ====================

    @Test
    void shouldColorizeStringValueGreen() {
        // Behavior: JSON string values are colorized with green ANSI code
        String result = colorizer.colorize("{\"key\": \"value\"}");
        assertTrue(containsColorCode(result, ANSI_GREEN), "String value should be colored green");
    }

    @Test
    void shouldColorizeSingleQuotedStringGreen() {
        // Behavior: Single-quoted string values are colorized green
        String result = colorizer.colorize("{'key': 'value'}");
        assertTrue(containsColorCode(result, ANSI_GREEN), "Single-quoted string should be colored green");
    }

    @Test
    void shouldColorizeArrayStringGreen() {
        // Behavior: Strings in arrays are colorized green
        String result = colorizer.colorize("[\"hello\", \"world\"]");
        assertTrue(containsColorCode(result, ANSI_GREEN), "Array strings should be colored green");
    }

    // ==================== Number Colorization Tests ====================

    @Test
    void shouldColorizeIntegerYellow() {
        // Behavior: Integer values are colorized with yellow ANSI code
        String result = colorizer.colorize("{\"count\": 42}");
        assertTrue(containsColorCode(result, ANSI_YELLOW), "Integer should be colored yellow");
    }

    @Test
    void shouldColorizeNegativeNumberYellow() {
        // Behavior: Negative numbers are colorized yellow
        String result = colorizer.colorize("{\"temp\": -10}");
        assertTrue(containsColorCode(result, ANSI_YELLOW), "Negative number should be colored yellow");
    }

    @Test
    void shouldColorizeDecimalYellow() {
        // Behavior: Decimal numbers are colorized yellow
        String result = colorizer.colorize("{\"pi\": 3.14159}");
        assertTrue(containsColorCode(result, ANSI_YELLOW), "Decimal should be colored yellow");
    }

    @Test
    void shouldColorizeScientificNotationYellow() {
        // Behavior: Scientific notation numbers are colorized yellow
        String result = colorizer.colorize("{\"big\": 1.5e10}");
        assertTrue(containsColorCode(result, ANSI_YELLOW), "Scientific notation should be colored yellow");
    }

    // ==================== Boolean Colorization Tests ====================

    @Test
    void shouldColorizeTrueMagenta() {
        // Behavior: Boolean true is colorized with magenta ANSI code
        String result = colorizer.colorize("{\"active\": true}");
        assertTrue(containsColorCode(result, ANSI_MAGENTA), "true should be colored magenta");
    }

    @Test
    void shouldColorizeFalseMagenta() {
        // Behavior: Boolean false is colorized with magenta ANSI code
        String result = colorizer.colorize("{\"deleted\": false}");
        assertTrue(containsColorCode(result, ANSI_MAGENTA), "false should be colored magenta");
    }

    // ==================== Null Colorization Tests ====================

    @Test
    void shouldColorizeNullRed() {
        // Behavior: Null values are colorized with red ANSI code
        String result = colorizer.colorize("{\"data\": null}");
        assertTrue(containsColorCode(result, ANSI_RED), "null should be colored red");
    }

    // ==================== Complex JSON Tests ====================

    @Test
    void shouldColorizeComplexJson() {
        // Behavior: Complex JSON with multiple value types is colorized correctly
        String input = "{\"name\": \"test\", \"count\": 5, \"active\": true, \"data\": null}";
        String result = colorizer.colorize(input);

        assertTrue(containsColorCode(result, ANSI_BLUE), "Keys should be blue");
        assertTrue(containsColorCode(result, ANSI_GREEN), "String values should be green");
        assertTrue(containsColorCode(result, ANSI_YELLOW), "Numbers should be yellow");
        assertTrue(containsColorCode(result, ANSI_MAGENTA), "Booleans should be magenta");
        assertTrue(containsColorCode(result, ANSI_RED), "Null should be red");

        assertEquals(input, stripAnsi(result), "Content should be preserved");
    }

    @Test
    void shouldColorizeDeeplyNestedJson() {
        // Behavior: Deeply nested JSON structures are colorized correctly
        String input = "{\"level1\": {\"level2\": {\"level3\": \"deep\"}}}";
        String result = colorizer.colorize(input);

        assertEquals(input, stripAnsi(result), "Content should be preserved");
        assertTrue(containsColorCode(result, ANSI_BLUE), "Nested keys should be blue");
        assertTrue(containsColorCode(result, ANSI_GREEN), "Nested string should be green");
    }

    @Test
    void shouldColorizeArrayWithMixedTypes() {
        // Behavior: Arrays with mixed types are colorized correctly
        String input = "[\"text\", 123, true, null]";
        String result = colorizer.colorize(input);

        assertEquals(input, stripAnsi(result), "Content should be preserved");
        assertTrue(containsColorCode(result, ANSI_GREEN), "String should be green");
        assertTrue(containsColorCode(result, ANSI_YELLOW), "Number should be yellow");
        assertTrue(containsColorCode(result, ANSI_MAGENTA), "Boolean should be magenta");
        assertTrue(containsColorCode(result, ANSI_RED), "Null should be red");
    }

    // ==================== Edge Cases ====================

    @Test
    void shouldHandleEmptyObject() {
        // Behavior: Empty object is handled correctly
        String input = "{}";
        String result = stripAnsi(colorizer.colorize(input));
        assertEquals(input, result);
    }

    @Test
    void shouldHandleEmptyArray() {
        // Behavior: Empty array is handled correctly
        String input = "[]";
        String result = stripAnsi(colorizer.colorize(input));
        assertEquals(input, result);
    }

    @Test
    void shouldHandleEscapedQuotesInString() {
        // Behavior: Escaped quotes within strings are preserved
        String input = "{\"msg\": \"say \\\"hello\\\"\"}";
        String result = stripAnsi(colorizer.colorize(input));
        assertEquals(input, result);
    }

    @Test
    void shouldHandleEscapedBackslash() {
        // Behavior: Escaped backslashes are preserved
        String input = "{\"path\": \"C:\\\\Users\"}";
        String result = stripAnsi(colorizer.colorize(input));
        assertEquals(input, result);
    }

    @Test
    void shouldHandleUnicodeInStrings() {
        // Behavior: Unicode characters in strings are preserved
        String input = "{\"emoji\": \"Hello 🌍\"}";
        String result = stripAnsi(colorizer.colorize(input));
        assertEquals(input, result);
    }

    @Test
    void shouldHandleTopLevelString() {
        // Behavior: Top-level string (not in object) is colorized green
        String result = colorizer.colorize("\"hello\"");
        assertTrue(containsColorCode(result, ANSI_GREEN), "Top-level string should be green");
        assertEquals("\"hello\"", stripAnsi(result));
    }

    @Test
    void shouldHandleTopLevelNumber() {
        // Behavior: Top-level number is colorized yellow
        String result = colorizer.colorize("42");
        assertTrue(containsColorCode(result, ANSI_YELLOW), "Top-level number should be yellow");
        assertEquals("42", stripAnsi(result));
    }

    @Test
    void shouldHandleTopLevelBoolean() {
        // Behavior: Top-level boolean is colorized magenta
        String result = colorizer.colorize("true");
        assertTrue(containsColorCode(result, ANSI_MAGENTA), "Top-level boolean should be magenta");
        assertEquals("true", stripAnsi(result));
    }

    @Test
    void shouldHandleTopLevelNull() {
        // Behavior: Top-level null is colorized red
        String result = colorizer.colorize("null");
        assertTrue(containsColorCode(result, ANSI_RED), "Top-level null should be red");
        assertEquals("null", stripAnsi(result));
    }

    @Test
    void shouldHandleTopLevelArray() {
        // Behavior: Top-level array is colorized correctly
        String input = "[1, 2, 3]";
        String result = colorizer.colorize(input);
        assertTrue(containsColorCode(result, ANSI_YELLOW), "Array numbers should be yellow");
        assertEquals(input, stripAnsi(result));
    }

    // ==================== Malformed Token Tests ====================

    @Test
    void shouldTreatNumberFollowedByLettersAsIdentifier() {
        // Behavior: 100abc is treated as single identifier, not number + text
        String input = "{\"x\": 100abc}";
        String result = colorizer.colorize(input);
        // Should NOT contain yellow for the number since it's malformed
        assertEquals(input, stripAnsi(result));
    }

    @Test
    void shouldTreatTrueFollowedByLettersAsIdentifier() {
        // Behavior: trueXYZ is treated as identifier, not boolean + text
        String input = "{\"x\": trueXYZ}";
        String result = colorizer.colorize(input);
        assertEquals(input, stripAnsi(result));
    }

    @Test
    void shouldTreatFalseFollowedByLettersAsIdentifier() {
        // Behavior: falseABC is treated as identifier, not boolean + text
        String input = "{\"x\": falseABC}";
        String result = colorizer.colorize(input);
        assertEquals(input, stripAnsi(result));
    }

    @Test
    void shouldTreatNullFollowedByLettersAsIdentifier() {
        // Behavior: nullXYZ is treated as identifier, not null + text
        String input = "{\"x\": nullXYZ}";
        String result = colorizer.colorize(input);
        assertEquals(input, stripAnsi(result));
    }

    @Test
    void shouldStillColorValidNumberBeforeDelimiter() {
        // Behavior: Valid number followed by delimiter is still colored
        String input = "{\"x\": 100}";
        String result = colorizer.colorize(input);
        assertTrue(containsColorCode(result, ANSI_YELLOW), "Valid number should be yellow");
        assertEquals(input, stripAnsi(result));
    }

    @Test
    void shouldStillColorValidBooleanBeforeDelimiter() {
        // Behavior: Valid boolean followed by delimiter is still colored
        String input = "{\"x\": true}";
        String result = colorizer.colorize(input);
        assertTrue(containsColorCode(result, ANSI_MAGENTA), "Valid boolean should be magenta");
        assertEquals(input, stripAnsi(result));
    }

    @Test
    void shouldStillColorValidNullBeforeDelimiter() {
        // Behavior: Valid null followed by delimiter is still colored
        String input = "{\"x\": null}";
        String result = colorizer.colorize(input);
        assertTrue(containsColorCode(result, ANSI_RED), "Valid null should be red");
        assertEquals(input, stripAnsi(result));
    }

    @Test
    void shouldHandleMalformedNumberInArray() {
        // Behavior: Malformed number in array is treated as identifier
        String input = "[100abc, 200]";
        String result = colorizer.colorize(input);
        assertTrue(containsColorCode(result, ANSI_YELLOW), "Valid number 200 should be yellow");
        assertEquals(input, stripAnsi(result));
    }

    @Test
    void shouldHandleMalformedBooleanInArray() {
        // Behavior: Malformed boolean in array is treated as identifier
        String input = "[trueXYZ, false]";
        String result = colorizer.colorize(input);
        assertTrue(containsColorCode(result, ANSI_MAGENTA), "Valid false should be magenta");
        assertEquals(input, stripAnsi(result));
    }
}
