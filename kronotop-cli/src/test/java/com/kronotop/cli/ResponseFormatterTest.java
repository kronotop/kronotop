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

import com.kronotop.cli.resp.RespValue;
import org.junit.jupiter.api.Test;

import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ResponseFormatterTest {

    private final ResponseFormatter formatter = new ResponseFormatter(false);
    private final ResponseFormatter jsonFormatter = new ResponseFormatter(true);

    @Test
    void shouldFormatSimpleString() {
        RespValue value = new RespValue.SimpleString("OK");
        assertEquals("OK", formatter.format(value));
    }

    @Test
    void shouldFormatBlobString() {
        RespValue value = new RespValue.BlobString("hello world");
        assertEquals("\"hello world\"", formatter.format(value));
    }

    @Test
    void shouldFormatMultiLineBlobStringRaw() {
        RespValue value = new RespValue.BlobString("line1\nline2\nline3");
        assertEquals("line1\nline2\nline3", formatter.format(value));
    }

    @Test
    void shouldFormatMultiLineBlobStringWithTrailingNewline() {
        RespValue value = new RespValue.BlobString("line1\nline2\n");
        assertEquals("line1\nline2", formatter.format(value));
    }

    @Test
    void shouldFormatSimpleError() {
        RespValue value = new RespValue.SimpleError("ERR", "unknown command");
        assertEquals("(error) ERR unknown command", formatter.format(value));
    }

    @Test
    void shouldFormatBlobError() {
        RespValue value = new RespValue.BlobError("SYNTAX", "invalid syntax");
        assertEquals("(error) SYNTAX invalid syntax", formatter.format(value));
    }

    @Test
    void shouldFormatNumber() {
        RespValue value = new RespValue.Number(42);
        assertEquals("(integer) 42", formatter.format(value));
    }

    @Test
    void shouldFormatNegativeNumber() {
        RespValue value = new RespValue.Number(-123);
        assertEquals("(integer) -123", formatter.format(value));
    }

    @Test
    void shouldFormatDouble() {
        RespValue value = new RespValue.Double(3.14);
        assertEquals("(double) 3.14", formatter.format(value));
    }

    @Test
    void shouldFormatBooleanTrue() {
        RespValue value = new RespValue.Boolean(true);
        assertEquals("(true)", formatter.format(value));
    }

    @Test
    void shouldFormatBooleanFalse() {
        RespValue value = new RespValue.Boolean(false);
        assertEquals("(false)", formatter.format(value));
    }

    @Test
    void shouldFormatNull() {
        RespValue value = new RespValue.Null();
        assertEquals("(nil)", formatter.format(value));
    }

    @Test
    void shouldFormatBigNumber() {
        RespValue value = new RespValue.BigNumber(new BigInteger("12345678901234567890"));
        assertEquals("(integer) 12345678901234567890", formatter.format(value));
    }

    @Test
    void shouldFormatVerbatimString() {
        RespValue value = new RespValue.VerbatimString("txt", "hello world");
        assertEquals("hello world", formatter.format(value));
    }

    @Test
    void shouldFormatEmptyArray() {
        RespValue value = new RespValue.Array(List.of());
        assertEquals("(empty array)", formatter.format(value));
    }

    @Test
    void shouldFormatArrayOfNumbers() {
        RespValue value = new RespValue.Array(List.of(
                new RespValue.Number(1),
                new RespValue.Number(2),
                new RespValue.Number(3)
        ));
        assertEquals("1) (integer) 1\n2) (integer) 2\n3) (integer) 3", formatter.format(value));
    }

    @Test
    void shouldFormatArrayWithPaddedIndices() {
        RespValue value = new RespValue.Array(List.of(
                new RespValue.Number(1),
                new RespValue.Number(2),
                new RespValue.Number(3),
                new RespValue.Number(4),
                new RespValue.Number(5),
                new RespValue.Number(6),
                new RespValue.Number(7),
                new RespValue.Number(8),
                new RespValue.Number(9),
                new RespValue.Number(10)
        ));
        String expected = " 1) (integer) 1\n" +
                " 2) (integer) 2\n" +
                " 3) (integer) 3\n" +
                " 4) (integer) 4\n" +
                " 5) (integer) 5\n" +
                " 6) (integer) 6\n" +
                " 7) (integer) 7\n" +
                " 8) (integer) 8\n" +
                " 9) (integer) 9\n" +
                "10) (integer) 10";
        assertEquals(expected, formatter.format(value));
    }

    @Test
    void shouldFormatNestedArray() {
        RespValue value = new RespValue.Array(List.of(
                new RespValue.Array(List.of(
                        new RespValue.Number(1),
                        new RespValue.Number(2)
                )),
                new RespValue.Array(List.of(
                        new RespValue.Number(3),
                        new RespValue.Number(4)
                ))
        ));
        String expected = "1) 1) (integer) 1\n" +
                "   2) (integer) 2\n" +
                "2) 1) (integer) 3\n" +
                "   2) (integer) 4";
        assertEquals(expected, formatter.format(value));
    }

    @Test
    void shouldFormatEmptyMap() {
        RespValue value = new RespValue.RespMap(Map.of());
        assertEquals("(empty map)", formatter.format(value));
    }

    @Test
    void shouldFormatMapWithSimpleValues() {
        Map<RespValue, RespValue> map = new LinkedHashMap<>();
        map.put(new RespValue.SimpleString("name"), new RespValue.BlobString("alice"));
        map.put(new RespValue.SimpleString("age"), new RespValue.Number(30));
        RespValue value = new RespValue.RespMap(map);
        String expected = "1# name => \"alice\"\n" +
                "2# age => (integer) 30";
        assertEquals(expected, formatter.format(value));
    }

    @Test
    void shouldFormatMapWithEmptyArrayInline() {
        Map<RespValue, RespValue> map = new LinkedHashMap<>();
        map.put(new RespValue.SimpleString("items"), new RespValue.Array(List.of()));
        RespValue value = new RespValue.RespMap(map);
        assertEquals("1# items => (empty array)", formatter.format(value));
    }

    @Test
    void shouldFormatMapWithSingleElementArrayInline() {
        Map<RespValue, RespValue> map = new LinkedHashMap<>();
        map.put(new RespValue.SimpleString("items"), new RespValue.Array(List.of(
                new RespValue.BlobString("item1")
        )));
        RespValue value = new RespValue.RespMap(map);
        assertEquals("1# items => 1) \"item1\"", formatter.format(value));
    }

    @Test
    void shouldFormatMapWithMultiElementArrayOnNewLine() {
        Map<RespValue, RespValue> map = new LinkedHashMap<>();
        map.put(new RespValue.SimpleString("items"), new RespValue.Array(List.of(
                new RespValue.BlobString("item1"),
                new RespValue.BlobString("item2")
        )));
        RespValue value = new RespValue.RespMap(map);
        String expected = "1# items =>\n" +
                "   1) \"item1\"\n" +
                "   2) \"item2\"";
        assertEquals(expected, formatter.format(value));
    }

    @Test
    void shouldFormatEmptySet() {
        RespValue value = new RespValue.RespSet(Set.of());
        assertEquals("(empty set)", formatter.format(value));
    }

    @Test
    void shouldFormatPush() {
        RespValue value = new RespValue.Push("message", List.of(
                new RespValue.BlobString("channel1"),
                new RespValue.BlobString("hello")
        ));
        String expected = "push: message\n" +
                "1) \"channel1\"\n" +
                "2) \"hello\"";
        assertEquals(expected, formatter.format(value));
    }

    @Test
    void shouldFormatAttribute() {
        Map<RespValue, RespValue> attrs = new LinkedHashMap<>();
        attrs.put(new RespValue.SimpleString("ttl"), new RespValue.Number(100));
        RespValue value = new RespValue.Attribute(attrs, new RespValue.SimpleString("OK"));
        assertEquals("OK", formatter.format(value));
    }

    @Test
    void shouldEscapeStringInJsonMode() {
        RespValue value = new RespValue.BlobString("hello\tworld");
        assertEquals("\"hello\\tworld\"", jsonFormatter.format(value));
    }

    @Test
    void shouldEscapeNewlineInJsonMode() {
        RespValue value = new RespValue.BlobString("line1");
        assertEquals("\"line1\"", jsonFormatter.format(value));
    }

    @Test
    void shouldEscapeQuotesInJsonMode() {
        RespValue value = new RespValue.BlobString("say \"hello\"");
        assertEquals("\"say \\\"hello\\\"\"", jsonFormatter.format(value));
    }

    @Test
    void shouldEscapeBackslashInJsonMode() {
        RespValue value = new RespValue.BlobString("path\\to\\file");
        assertEquals("\"path\\\\to\\\\file\"", jsonFormatter.format(value));
    }

    @Test
    void shouldNotEscapeInNonJsonMode() {
        RespValue value = new RespValue.BlobString("hello\tworld");
        assertEquals("\"hello\tworld\"", formatter.format(value));
    }

    @Test
    void shouldEscapeControlCharsInJsonMode() {
        RespValue value = new RespValue.BlobString("hello\u0001world");
        assertEquals("\"hello\\x01world\"", jsonFormatter.format(value));
    }
}
