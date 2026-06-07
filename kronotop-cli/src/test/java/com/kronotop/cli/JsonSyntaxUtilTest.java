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

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class JsonSyntaxUtilTest {

    // ==================== isValidTokenEnd Tests ====================

    @Test
    void shouldReturnTrueForEndOfString() {
        // Behavior: Position at or beyond string length is valid token end
        assertTrue(JsonSyntaxUtil.isValidTokenEnd("abc", 3));
        assertTrue(JsonSyntaxUtil.isValidTokenEnd("abc", 5));
    }

    @Test
    void shouldReturnTrueForWhitespace() {
        // Behavior: Whitespace is a valid token boundary
        assertTrue(JsonSyntaxUtil.isValidTokenEnd("abc ", 3));
        assertTrue(JsonSyntaxUtil.isValidTokenEnd("abc\t", 3));
        assertTrue(JsonSyntaxUtil.isValidTokenEnd("abc\n", 3));
    }

    @Test
    void shouldReturnTrueForJsonDelimiters() {
        // Behavior: JSON delimiters are valid token boundaries
        assertTrue(JsonSyntaxUtil.isValidTokenEnd("abc,", 3));
        assertTrue(JsonSyntaxUtil.isValidTokenEnd("abc}", 3));
        assertTrue(JsonSyntaxUtil.isValidTokenEnd("abc]", 3));
        assertTrue(JsonSyntaxUtil.isValidTokenEnd("abc:", 3));
    }

    @Test
    void shouldReturnFalseForLetters() {
        // Behavior: Letters are not valid token boundaries
        assertFalse(JsonSyntaxUtil.isValidTokenEnd("abcd", 3));
        assertFalse(JsonSyntaxUtil.isValidTokenEnd("123a", 3));
    }

    // ==================== scanNumberEnd Tests ====================

    @Test
    void shouldScanInteger() {
        // Behavior: Scans all digits of an integer
        assertEquals(3, JsonSyntaxUtil.scanNumberEnd("123", 0));
        assertEquals(5, JsonSyntaxUtil.scanNumberEnd("12345abc", 0));
    }

    @Test
    void shouldScanNegativeNumber() {
        // Behavior: Scans negative sign and digits
        assertEquals(4, JsonSyntaxUtil.scanNumberEnd("-123", 0));
    }

    @Test
    void shouldScanDecimalNumber() {
        // Behavior: Scans decimal point and following digits
        assertEquals(5, JsonSyntaxUtil.scanNumberEnd("3.141", 0));
    }

    @Test
    void shouldScanScientificNotation() {
        // Behavior: Scans scientific notation including e/E and +/-
        assertEquals(6, JsonSyntaxUtil.scanNumberEnd("1.5e10", 0));
        assertEquals(7, JsonSyntaxUtil.scanNumberEnd("1.5E+10", 0));
        assertEquals(7, JsonSyntaxUtil.scanNumberEnd("1.5e-10", 0));
    }

    @Test
    void shouldStopAtNonNumericCharacter() {
        // Behavior: Stops scanning at first non-numeric character
        assertEquals(3, JsonSyntaxUtil.scanNumberEnd("123abc", 0));
        assertEquals(3, JsonSyntaxUtil.scanNumberEnd("123}", 0));
    }

    // ==================== parseString Tests ====================

    @Test
    void shouldParseDoubleQuotedString() {
        // Behavior: Parses string enclosed in double quotes
        JsonSyntaxUtil.ParsedString result = JsonSyntaxUtil.parseString("\"hello\"", 0);
        assertEquals("\"hello\"", result.content());
        assertEquals(7, result.endPos());
    }

    @Test
    void shouldParseSingleQuotedString() {
        // Behavior: Parses string enclosed in single quotes
        JsonSyntaxUtil.ParsedString result = JsonSyntaxUtil.parseString("'hello'", 0);
        assertEquals("'hello'", result.content());
        assertEquals(7, result.endPos());
    }

    @Test
    void shouldParseStringWithEscapedQuote() {
        // Behavior: Handles escaped quotes within string
        JsonSyntaxUtil.ParsedString result = JsonSyntaxUtil.parseString("\"say \\\"hi\\\"\"", 0);
        assertEquals("\"say \\\"hi\\\"\"", result.content());
        assertEquals(12, result.endPos());
    }

    @Test
    void shouldParseStringWithEscapedBackslash() {
        // Behavior: Handles escaped backslashes
        JsonSyntaxUtil.ParsedString result = JsonSyntaxUtil.parseString("\"path\\\\file\"", 0);
        assertEquals("\"path\\\\file\"", result.content());
        assertEquals(12, result.endPos());
    }

    @Test
    void shouldParseStringFromMiddlePosition() {
        // Behavior: Can start parsing from non-zero position
        JsonSyntaxUtil.ParsedString result = JsonSyntaxUtil.parseString("key: \"value\"", 5);
        assertEquals("\"value\"", result.content());
        assertEquals(12, result.endPos());
    }

    // ==================== isFollowedByColon Tests ====================

    @Test
    void shouldReturnTrueWhenFollowedByColon() {
        // Behavior: Returns true when next non-whitespace is colon
        assertTrue(JsonSyntaxUtil.isFollowedByColon("key:", 3));
        assertTrue(JsonSyntaxUtil.isFollowedByColon("key :", 3));
        assertTrue(JsonSyntaxUtil.isFollowedByColon("key  :", 3));
    }

    @Test
    void shouldReturnFalseWhenNotFollowedByColon() {
        // Behavior: Returns false when next non-whitespace is not colon
        assertFalse(JsonSyntaxUtil.isFollowedByColon("key,", 3));
        assertFalse(JsonSyntaxUtil.isFollowedByColon("key}", 3));
        assertFalse(JsonSyntaxUtil.isFollowedByColon("key", 3));
    }

    // ==================== scanIdentifierEnd Tests ====================

    @Test
    void shouldScanAlphanumericIdentifier() {
        // Behavior: Scans letters, digits, underscores, dots, and dashes
        assertEquals(5, JsonSyntaxUtil.scanIdentifierEnd("hello", 0));
        assertEquals(8, JsonSyntaxUtil.scanIdentifierEnd("hello123", 0));
        assertEquals(9, JsonSyntaxUtil.scanIdentifierEnd("hello_123", 0));
        assertEquals(9, JsonSyntaxUtil.scanIdentifierEnd("hello.123", 0));
        assertEquals(9, JsonSyntaxUtil.scanIdentifierEnd("hello-123", 0));
    }

    @Test
    void shouldStopAtNonIdentifierCharacter() {
        // Behavior: Stops at characters not valid in identifiers
        assertEquals(5, JsonSyntaxUtil.scanIdentifierEnd("hello:", 0));
        assertEquals(5, JsonSyntaxUtil.scanIdentifierEnd("hello,", 0));
        assertEquals(5, JsonSyntaxUtil.scanIdentifierEnd("hello}", 0));
    }

    // ==================== scanOperatorEnd Tests ====================

    @Test
    void shouldScanOperator() {
        // Behavior: Scans $ followed by alphanumeric and underscores
        assertEquals(3, JsonSyntaxUtil.scanOperatorEnd("$gt", 0));
        assertEquals(4, JsonSyntaxUtil.scanOperatorEnd("$gte", 0));
        assertEquals(6, JsonSyntaxUtil.scanOperatorEnd("$regex", 0));
        assertEquals(8, JsonSyntaxUtil.scanOperatorEnd("$elem_id", 0));
    }

    @Test
    void shouldStopAtNonOperatorCharacter() {
        // Behavior: Stops at characters not valid in operators
        assertEquals(3, JsonSyntaxUtil.scanOperatorEnd("$gt:", 0));
        assertEquals(3, JsonSyntaxUtil.scanOperatorEnd("$gt}", 0));
        assertEquals(3, JsonSyntaxUtil.scanOperatorEnd("$gt ", 0));
    }

    // ==================== isJsonComplete Tests ====================

    @Test
    void shouldReturnTrueForEmptyBraces() {
        // Behavior: Empty object is complete
        assertTrue(JsonSyntaxUtil.isJsonComplete("{}"));
    }

    @Test
    void shouldReturnTrueForEmptyBrackets() {
        // Behavior: Empty array is complete
        assertTrue(JsonSyntaxUtil.isJsonComplete("[]"));
    }

    @Test
    void shouldReturnFalseForUnclosedBrace() {
        // Behavior: Unclosed brace needs more input
        assertFalse(JsonSyntaxUtil.isJsonComplete("{"));
        assertFalse(JsonSyntaxUtil.isJsonComplete("{\"key\": \"value\""));
    }

    @Test
    void shouldReturnFalseForUnclosedBracket() {
        // Behavior: Unclosed bracket needs more input
        assertFalse(JsonSyntaxUtil.isJsonComplete("["));
        assertFalse(JsonSyntaxUtil.isJsonComplete("[1, 2, 3"));
    }

    @Test
    void shouldReturnTrueForSimpleObject() {
        // Behavior: Simple key-value object is complete
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"key\": \"value\"}"));
    }

    @Test
    void shouldReturnFalseForUnclosedString() {
        // Behavior: Unclosed string needs more input
        assertFalse(JsonSyntaxUtil.isJsonComplete("{\"key\": \"val"));
        assertFalse(JsonSyntaxUtil.isJsonComplete("\"incomplete"));
    }

    @Test
    void shouldReturnTrueForNestedObjects() {
        // Behavior: Nested objects are complete when all braces match
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"a\": {\"b\": 1}}"));
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"a\": {\"b\": {\"c\": 2}}}"));
    }

    @Test
    void shouldReturnFalseForIncompleteNestedObjects() {
        // Behavior: Nested objects need all braces closed
        assertFalse(JsonSyntaxUtil.isJsonComplete("{\"a\": {\"b\": 1}"));
        assertFalse(JsonSyntaxUtil.isJsonComplete("{\"a\": {"));
    }

    @Test
    void shouldReturnTrueForArray() {
        // Behavior: Complete array is complete
        assertTrue(JsonSyntaxUtil.isJsonComplete("[1, 2, 3]"));
        assertTrue(JsonSyntaxUtil.isJsonComplete("[\"a\", \"b\"]"));
    }

    @Test
    void shouldReturnFalseForIncompleteArray() {
        // Behavior: Incomplete array needs more input
        assertFalse(JsonSyntaxUtil.isJsonComplete("[1, 2,"));
    }

    @Test
    void shouldReturnTrueForMixedStructures() {
        // Behavior: Mixed objects and arrays are complete when balanced
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"arr\": [1, 2, 3]}"));
        assertTrue(JsonSyntaxUtil.isJsonComplete("[{\"a\": 1}, {\"b\": 2}]"));
    }

    @Test
    void shouldHandleBracesInsideStrings() {
        // Behavior: Braces inside strings don't affect balance
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"msg\": \"use {braces}\"}"));
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"msg\": \"[brackets]\"}"));
    }

    @Test
    void shouldHandleEscapedQuotesInsideStrings() {
        // Behavior: Escaped quotes don't close the string
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"msg\": \"say \\\"hello\\\"\"}"));
    }

    @Test
    void shouldReturnTrueForPlainText() {
        // Behavior: Plain text without brackets/quotes is complete
        assertTrue(JsonSyntaxUtil.isJsonComplete("bucket.insert users"));
        assertTrue(JsonSyntaxUtil.isJsonComplete("simple command"));
    }

    @Test
    void shouldHandleSingleQuotedStrings() {
        // Behavior: Single-quoted strings are handled correctly
        assertTrue(JsonSyntaxUtil.isJsonComplete("{'key': 'value'}"));
        assertFalse(JsonSyntaxUtil.isJsonComplete("{'key': 'val"));
    }

    @Test
    void shouldHandleEscapedBackslashBeforeQuote() {
        // Behavior: Escaped backslash followed by quote closes the string correctly
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"path\": \"C:\\\\Users\\\\\"}"));
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"msg\": \"end with backslash\\\\\"}"));
    }

    @Test
    void shouldHandleMultiLineInput() {
        // Behavior: Multi-line JSON is evaluated as a whole
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\n  \"name\": \"Alice\",\n  \"age\": 30\n}"));
        assertFalse(JsonSyntaxUtil.isJsonComplete("{\n  \"name\": \"Alice\",\n  \"age\": 30"));
    }

    @Test
    void shouldHandleDeeplyNestedStructures() {
        // Behavior: Deeply nested structures are correctly balanced
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"a\": {\"b\": {\"c\": {\"d\": [1, 2, 3]}}}}"));
        assertFalse(JsonSyntaxUtil.isJsonComplete("{\"a\": {\"b\": {\"c\": {\"d\": [1, 2, 3]}}"));
    }

    @Test
    void shouldHandleArrayOfObjects() {
        // Behavior: Array of objects is complete when all brackets match
        assertTrue(JsonSyntaxUtil.isJsonComplete("[{\"a\": 1}, {\"b\": 2}, {\"c\": 3}]"));
        assertFalse(JsonSyntaxUtil.isJsonComplete("[{\"a\": 1}, {\"b\": 2}, {\"c\": 3}"));
    }

    @Test
    void shouldHandleEmptyStringValue() {
        // Behavior: Empty string values are handled correctly
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"empty\": \"\"}"));
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"empty\": ''}"));
    }

    @Test
    void shouldHandleStringWithNewlines() {
        // Behavior: Strings containing newline characters are handled
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"msg\": \"line1\\nline2\"}"));
    }

    @Test
    void shouldHandleOnlyOpeningBrackets() {
        // Behavior: Multiple unclosed brackets all need closing
        assertFalse(JsonSyntaxUtil.isJsonComplete("{{{"));
        assertFalse(JsonSyntaxUtil.isJsonComplete("[[["));
        assertFalse(JsonSyntaxUtil.isJsonComplete("{[{"));
    }

    @Test
    void shouldHandleOnlyClosingBrackets() {
        // Behavior: Extra closing brackets make count negative, which is incomplete
        assertFalse(JsonSyntaxUtil.isJsonComplete("}}}"));
        assertFalse(JsonSyntaxUtil.isJsonComplete("]]]"));
    }

    @Test
    void shouldHandleCommandWithJsonArgument() {
        // Behavior: CLI command followed by JSON is complete when JSON is complete
        assertTrue(JsonSyntaxUtil.isJsonComplete("bucket.insert users {\"name\": \"test\"}"));
        assertFalse(JsonSyntaxUtil.isJsonComplete("bucket.insert users {\"name\": \"test\""));
    }

    @Test
    void shouldHandleQueryOperators() {
        // Behavior: Query with operators is complete when balanced
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"age\": {\"$gt\": 18, \"$lt\": 65}}"));
        assertFalse(JsonSyntaxUtil.isJsonComplete("{\"age\": {\"$gt\": 18, \"$lt\": 65}"));
    }

    @Test
    void shouldHandleComplexNestedQuery() {
        // Behavior: Complex nested query structures are correctly evaluated
        String query = "{\"$or\": [{\"status\": \"active\"}, {\"age\": {\"$gte\": 21}}]}";
        assertTrue(JsonSyntaxUtil.isJsonComplete(query));

        String incompleteQuery = "{\"$or\": [{\"status\": \"active\"}, {\"age\": {\"$gte\": 21}}]";
        assertFalse(JsonSyntaxUtil.isJsonComplete(incompleteQuery));
    }

    @Test
    void shouldHandleEscapedCharactersInStrings() {
        // Behavior: Various escape sequences don't break parsing
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"tab\": \"a\\tb\"}"));
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"unicode\": \"\\u0041\"}"));
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"mixed\": \"\\t\\n\\r\\\\\\\"\"}"));
    }

    @Test
    void shouldHandleWhitespaceOnlyInput() {
        // Behavior: Whitespace-only input is complete
        assertTrue(JsonSyntaxUtil.isJsonComplete("   "));
        assertTrue(JsonSyntaxUtil.isJsonComplete("\t\n"));
    }

    @Test
    void shouldHandleMixedQuoteStyles() {
        // Behavior: Mixed single and double quotes in separate strings
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"key\": 'value'}"));
        assertFalse(JsonSyntaxUtil.isJsonComplete("{\"key\": 'value"));
    }

    @Test
    void shouldHandleQuotesInsideOppositeQuotes() {
        // Behavior: Single quotes inside double quotes and vice versa
        assertTrue(JsonSyntaxUtil.isJsonComplete("{\"msg\": \"it's fine\"}"));
        assertTrue(JsonSyntaxUtil.isJsonComplete("{'msg': 'say \"hello\"'}"));
    }
}
