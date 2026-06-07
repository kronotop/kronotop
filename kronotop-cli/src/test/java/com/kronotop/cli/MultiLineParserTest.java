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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MultiLineParserTest {

    private MultiLineParser parser;

    @BeforeEach
    void setUp() {
        parser = new MultiLineParser();
    }

    // ==================== parse() with ACCEPT_LINE context ====================

    @Test
    void shouldThrowEOFErrorForUnclosedBrace() {
        // Behavior: Unclosed brace signals need for more input
        EOFError error = assertThrows(EOFError.class, () ->
                parser.parse("{", 1, Parser.ParseContext.ACCEPT_LINE));
        assertEquals("...> ", error.getMissing());
    }

    @Test
    void shouldThrowEOFErrorForUnclosedBracket() {
        // Behavior: Unclosed bracket signals need for more input
        EOFError error = assertThrows(EOFError.class, () ->
                parser.parse("[1, 2,", 6, Parser.ParseContext.ACCEPT_LINE));
        assertEquals("...> ", error.getMissing());
    }

    @Test
    void shouldThrowEOFErrorForUnclosedString() {
        // Behavior: Unclosed string signals need for more input
        EOFError error = assertThrows(EOFError.class, () ->
                parser.parse("{\"key\": \"value", 14, Parser.ParseContext.ACCEPT_LINE));
        assertEquals("...> ", error.getMissing());
    }

    @Test
    void shouldThrowEOFErrorForNestedIncomplete() {
        // Behavior: Nested incomplete structures signal need for more input
        assertThrows(EOFError.class, () ->
                parser.parse("{\"a\": {\"b\": 1}", 14, Parser.ParseContext.ACCEPT_LINE));
    }

    @Test
    void shouldReturnParsedLineForCompleteObject() {
        // Behavior: Complete JSON object returns ParsedLine
        ParsedLine result = parser.parse("{\"key\": \"value\"}", 16, Parser.ParseContext.ACCEPT_LINE);
        assertNotNull(result);
        assertEquals("{\"key\": \"value\"}", result.line());
    }

    @Test
    void shouldReturnParsedLineForCompleteArray() {
        // Behavior: Complete JSON array returns ParsedLine
        ParsedLine result = parser.parse("[1, 2, 3]", 9, Parser.ParseContext.ACCEPT_LINE);
        assertNotNull(result);
        assertEquals("[1, 2, 3]", result.line());
    }

    @Test
    void shouldReturnParsedLineForSimpleCommand() {
        // Behavior: Simple command without JSON returns ParsedLine
        ParsedLine result = parser.parse("ping", 4, Parser.ParseContext.ACCEPT_LINE);
        assertNotNull(result);
        assertEquals("ping", result.line());
    }

    @Test
    void shouldReturnParsedLineForEmptyInput() {
        // Behavior: Empty input is considered complete
        ParsedLine result = parser.parse("", 0, Parser.ParseContext.ACCEPT_LINE);
        assertNotNull(result);
    }

    // ==================== parse() with other contexts ====================

    @Test
    void shouldNotThrowForIncompleteInCompleteContext() {
        // Behavior: COMPLETE context doesn't check JSON completeness
        ParsedLine result = parser.parse("{", 1, Parser.ParseContext.COMPLETE);
        assertNotNull(result);
    }

    @Test
    void shouldNotThrowForIncompleteInUnspecifiedContext() {
        // Behavior: UNSPECIFIED context doesn't check JSON completeness
        ParsedLine result = parser.parse("{", 1, Parser.ParseContext.UNSPECIFIED);
        assertNotNull(result);
    }

    // ==================== Delegate methods ====================

    @Test
    void shouldDelegateIsEscapeChar() {
        // Behavior: Backslash is escape character
        assertTrue(parser.isEscapeChar('\\'));
        assertFalse(parser.isEscapeChar('a'));
    }

    @Test
    void shouldDelegateValidCommandName() {
        // Behavior: Delegates command name validation to DefaultParser
        // DefaultParser only accepts simple alphanumeric names
        assertTrue(parser.validCommandName("ping"));
        assertTrue(parser.validCommandName("test123"));
    }

    @Test
    void shouldDelegateValidVariableName() {
        // Behavior: Delegates variable name validation to DefaultParser
        assertTrue(parser.validVariableName("myVar"));
    }

    @Test
    void shouldDelegateGetCommand() {
        // Behavior: Extracts command from line via DefaultParser
        // DefaultParser.getCommand returns the word before the first whitespace
        String command = parser.getCommand("ping");
        assertNotNull(command);
    }

    // ==================== Multi-line scenarios ====================

    @Test
    void shouldThrowEOFErrorForMultiLineIncomplete() {
        // Behavior: Multi-line incomplete JSON signals need for more input
        String multiLine = "{\n  \"name\": \"Alice\",\n  \"age\": 30";
        assertThrows(EOFError.class, () ->
                parser.parse(multiLine, multiLine.length(), Parser.ParseContext.ACCEPT_LINE));
    }

    @Test
    void shouldReturnParsedLineForMultiLineComplete() {
        // Behavior: Multi-line complete JSON returns ParsedLine
        String multiLine = "{\n  \"name\": \"Alice\",\n  \"age\": 30\n}";
        ParsedLine result = parser.parse(multiLine, multiLine.length(), Parser.ParseContext.ACCEPT_LINE);
        assertNotNull(result);
        assertEquals(multiLine, result.line());
    }

    @Test
    void shouldHandleCommandWithMultiLineJson() {
        // Behavior: Command followed by multi-line JSON works correctly
        String input = "bucket.insert users {\n  \"name\": \"test\"\n}";
        ParsedLine result = parser.parse(input, input.length(), Parser.ParseContext.ACCEPT_LINE);
        assertNotNull(result);
    }

    @Test
    void shouldThrowForCommandWithIncompleteJson() {
        // Behavior: Command followed by incomplete JSON signals need for more input
        String input = "bucket.insert users {\n  \"name\": \"test\"";
        assertThrows(EOFError.class, () ->
                parser.parse(input, input.length(), Parser.ParseContext.ACCEPT_LINE));
    }
}
