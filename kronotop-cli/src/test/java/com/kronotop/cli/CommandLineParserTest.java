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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class CommandLineParserTest {

    private final CommandLineParser parser = new CommandLineParser();
    private final CommandLineParser quotedParser = new CommandLineParser(true);

    @Test
    void shouldParseSimpleCommand() {
        List<String> args = parser.parse("SET key value");
        assertEquals(List.of("SET", "key", "value"), args);
    }

    @Test
    void shouldParseQuotedString() {
        List<String> args = parser.parse("SET key \"hello world\"");
        assertEquals(List.of("SET", "key", "hello world"), args);
    }

    @Test
    void shouldParseSingleQuotedString() {
        List<String> args = parser.parse("SET key 'hello world'");
        assertEquals(List.of("SET", "key", "hello world"), args);
    }

    @Test
    void shouldParseEscapedNewlineInQuotes() {
        List<String> args = parser.parse("SET key \"hello\\nworld\"");
        assertEquals(List.of("SET", "key", "hello\nworld"), args);
    }

    @Test
    void shouldParseEscapedTabInQuotes() {
        List<String> args = parser.parse("SET key \"hello\\tworld\"");
        assertEquals(List.of("SET", "key", "hello\tworld"), args);
    }

    @Test
    void shouldParseEscapedCarriageReturnInQuotes() {
        List<String> args = parser.parse("SET key \"hello\\rworld\"");
        assertEquals(List.of("SET", "key", "hello\rworld"), args);
    }

    @Test
    void shouldParseHexEscapeInQuotes() {
        List<String> args = parser.parse("SET key \"hello\\x20world\"");
        assertEquals(List.of("SET", "key", "hello world"), args);
    }

    @Test
    void shouldParseEscapedQuoteInQuotes() {
        List<String> args = parser.parse("SET key \"hello\\\"world\"");
        assertEquals(List.of("SET", "key", "hello\"world"), args);
    }

    @Test
    void shouldNotParseEscapeOutsideQuotesWithoutFlag() {
        List<String> args = parser.parse("SET key hello\\nworld");
        assertEquals(List.of("SET", "key", "hello\\nworld"), args);
    }

    @Test
    void shouldParseEscapedNewlineOutsideQuotesWithFlag() {
        List<String> args = quotedParser.parse("SET key hello\\nworld");
        assertEquals(List.of("SET", "key", "hello\nworld"), args);
    }

    @Test
    void shouldParseEscapedTabOutsideQuotesWithFlag() {
        List<String> args = quotedParser.parse("SET key hello\\tworld");
        assertEquals(List.of("SET", "key", "hello\tworld"), args);
    }

    @Test
    void shouldParseHexEscapeOutsideQuotesWithFlag() {
        List<String> args = quotedParser.parse("SET key hello\\x41world");
        assertEquals(List.of("SET", "key", "helloAworld"), args);
    }

    @Test
    void shouldParseNullByteWithFlag() {
        List<String> args = quotedParser.parse("SET key hello\\x00world");
        assertEquals(List.of("SET", "key", "hello\0world"), args);
    }

    @Test
    void shouldHandleEmptyInput() {
        List<String> args = parser.parse("");
        assertEquals(List.of(), args);
    }

    @Test
    void shouldHandleWhitespaceOnlyInput() {
        List<String> args = parser.parse("   ");
        assertEquals(List.of(), args);
    }

    @Test
    void shouldHandleMultipleSpacesBetweenArgs() {
        List<String> args = parser.parse("SET   key   value");
        assertEquals(List.of("SET", "key", "value"), args);
    }

    @Test
    void shouldHandleTrailingBackslashWithFlag() {
        List<String> args = quotedParser.parse("SET key value\\");
        assertEquals(List.of("SET", "key", "value"), args);
    }

    @Test
    void shouldHandleIncompleteHexEscapeWithFlag() {
        List<String> args = quotedParser.parse("SET key hello\\x4");
        assertEquals(List.of("SET", "key", "hello\\x4"), args);
    }

    @Test
    void shouldHandleInvalidHexEscapeWithFlag() {
        List<String> args = quotedParser.parse("SET key hello\\xGG");
        assertEquals(List.of("SET", "key", "hello\\xGG"), args);
    }

    @Test
    void shouldParseDelimiterNewline() {
        assertEquals("\n", CommandLineParser.parseDelimiter("\\n"));
    }

    @Test
    void shouldParseDelimiterTab() {
        assertEquals("\t", CommandLineParser.parseDelimiter("\\t"));
    }

    @Test
    void shouldParseDelimiterCarriageReturn() {
        assertEquals("\r", CommandLineParser.parseDelimiter("\\r"));
    }

    @Test
    void shouldParseDelimiterBackslash() {
        assertEquals("\\", CommandLineParser.parseDelimiter("\\\\"));
    }

    @Test
    void shouldParseDelimiterPlainText() {
        assertEquals(",", CommandLineParser.parseDelimiter(","));
    }

    @Test
    void shouldParseDelimiterMixed() {
        assertEquals("a\nb", CommandLineParser.parseDelimiter("a\\nb"));
    }

    @Test
    void shouldParseDelimiterEmpty() {
        assertEquals("", CommandLineParser.parseDelimiter(""));
    }

    @Test
    void shouldParseDelimiterTrailingBackslash() {
        assertEquals("test\\", CommandLineParser.parseDelimiter("test\\"));
    }
}
