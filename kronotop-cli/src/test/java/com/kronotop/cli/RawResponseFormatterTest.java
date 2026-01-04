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

import static org.junit.jupiter.api.Assertions.assertEquals;

class RawResponseFormatterTest {

    private final RawResponseFormatter formatter = new RawResponseFormatter();

    @Test
    void shouldFormatSimpleString() {
        RespValue value = new RespValue.SimpleString("OK");
        assertEquals("OK", formatter.format(value));
    }

    @Test
    void shouldFormatBlobString() {
        RespValue value = new RespValue.BlobString("hello world");
        assertEquals("hello world", formatter.format(value));
    }

    @Test
    void shouldFormatMultiLineString() {
        RespValue value = new RespValue.BlobString("line1\nline2");
        assertEquals("line1\nline2", formatter.format(value));
    }

    @Test
    void shouldFormatSimpleError() {
        RespValue value = new RespValue.SimpleError("ERR", "unknown command");
        assertEquals("ERR unknown command", formatter.format(value));
    }

    @Test
    void shouldFormatNumber() {
        RespValue value = new RespValue.Number(42);
        assertEquals("42", formatter.format(value));
    }

    @Test
    void shouldFormatDouble() {
        RespValue value = new RespValue.Double(3.14);
        assertEquals("3.14", formatter.format(value));
    }

    @Test
    void shouldFormatBooleanTrue() {
        RespValue value = new RespValue.Boolean(true);
        assertEquals("true", formatter.format(value));
    }

    @Test
    void shouldFormatBooleanFalse() {
        RespValue value = new RespValue.Boolean(false);
        assertEquals("false", formatter.format(value));
    }

    @Test
    void shouldFormatNull() {
        RespValue value = new RespValue.Null();
        assertEquals("", formatter.format(value));
    }

    @Test
    void shouldFormatBigNumber() {
        RespValue value = new RespValue.BigNumber(new BigInteger("12345678901234567890"));
        assertEquals("12345678901234567890", formatter.format(value));
    }

    @Test
    void shouldFormatEmptyArray() {
        RespValue value = new RespValue.Array(List.of());
        assertEquals("", formatter.format(value));
    }

    @Test
    void shouldFormatArrayOfStrings() {
        RespValue value = new RespValue.Array(List.of(
                new RespValue.BlobString("a"),
                new RespValue.BlobString("b"),
                new RespValue.BlobString("c")
        ));
        assertEquals("a\nb\nc", formatter.format(value));
    }

    @Test
    void shouldFormatArrayOfNumbers() {
        RespValue value = new RespValue.Array(List.of(
                new RespValue.Number(1),
                new RespValue.Number(2),
                new RespValue.Number(3)
        ));
        assertEquals("1\n2\n3", formatter.format(value));
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
        assertEquals("1\n2\n3\n4", formatter.format(value));
    }

    @Test
    void shouldFormatMapWithSimpleValues() {
        Map<RespValue, RespValue> map = new LinkedHashMap<>();
        map.put(new RespValue.SimpleString("name"), new RespValue.BlobString("alice"));
        map.put(new RespValue.SimpleString("age"), new RespValue.Number(30));
        RespValue value = new RespValue.RespMap(map);
        assertEquals("name alice\nage 30", formatter.format(value));
    }

    @Test
    void shouldFormatMapWithEmptyArray() {
        Map<RespValue, RespValue> map = new LinkedHashMap<>();
        map.put(new RespValue.SimpleString("standbys"), new RespValue.Array(List.of()));
        RespValue value = new RespValue.RespMap(map);
        assertEquals("standbys", formatter.format(value));
    }

    @Test
    void shouldFormatMapWithNestedMap() {
        Map<RespValue, RespValue> inner = new LinkedHashMap<>();
        inner.put(new RespValue.SimpleString("primary"), new RespValue.BlobString("node1"));
        inner.put(new RespValue.SimpleString("standbys"), new RespValue.Array(List.of()));
        inner.put(new RespValue.SimpleString("status"), new RespValue.BlobString("READWRITE"));

        Map<RespValue, RespValue> outer = new LinkedHashMap<>();
        outer.put(new RespValue.SimpleString("0"), new RespValue.RespMap(inner));

        RespValue value = new RespValue.RespMap(outer);
        assertEquals("0 primary node1\nstandbys\nstatus READWRITE", formatter.format(value));
    }

    @Test
    void shouldFormatBucketQueryResult() {
        // Simulates bucket.query output: count, then id/doc pairs
        RespValue value = new RespValue.Array(List.of(
                new RespValue.Number(3),
                new RespValue.BlobString("0000D9CHBVGKO0000000xxxx"),
                new RespValue.BlobString("{\"status\": \"ALIVE\"}"),
                new RespValue.BlobString("0000D9CHBVGKO0010000xxxx"),
                new RespValue.BlobString("{\"status\": \"ALIVE\"}"),
                new RespValue.BlobString("0000D9CHBVGKO0020000xxxx")
        ));
        String expected = "3\n" +
                "0000D9CHBVGKO0000000xxxx\n" +
                "{\"status\": \"ALIVE\"}\n" +
                "0000D9CHBVGKO0010000xxxx\n" +
                "{\"status\": \"ALIVE\"}\n" +
                "0000D9CHBVGKO0020000xxxx";
        assertEquals(expected, formatter.format(value));
    }

    @Test
    void shouldFormatPush() {
        RespValue value = new RespValue.Push("message", List.of(
                new RespValue.BlobString("channel1"),
                new RespValue.BlobString("hello")
        ));
        assertEquals("message\nchannel1\nhello", formatter.format(value));
    }

    @Test
    void shouldFormatAttribute() {
        Map<RespValue, RespValue> attrs = new LinkedHashMap<>();
        attrs.put(new RespValue.SimpleString("ttl"), new RespValue.Number(100));
        RespValue value = new RespValue.Attribute(attrs, new RespValue.SimpleString("OK"));
        assertEquals("OK", formatter.format(value));
    }

    @Test
    void shouldUseCustomDelimiter() {
        RawResponseFormatter spaceFormatter = new RawResponseFormatter(" ");
        RespValue value = new RespValue.Array(List.of(
                new RespValue.BlobString("a"),
                new RespValue.BlobString("b"),
                new RespValue.BlobString("c")
        ));
        assertEquals("a b c", spaceFormatter.format(value));
    }

    @Test
    void shouldUseTabDelimiter() {
        RawResponseFormatter tabFormatter = new RawResponseFormatter("\t");
        RespValue value = new RespValue.Array(List.of(
                new RespValue.Number(1),
                new RespValue.Number(2),
                new RespValue.Number(3)
        ));
        assertEquals("1\t2\t3", tabFormatter.format(value));
    }

    @Test
    void shouldUseCommaDelimiter() {
        RawResponseFormatter commaFormatter = new RawResponseFormatter(",");
        RespValue value = new RespValue.Array(List.of(
                new RespValue.BlobString("hello"),
                new RespValue.BlobString("world")
        ));
        assertEquals("hello,world", commaFormatter.format(value));
    }

    @Test
    void shouldUseEmptyDelimiter() {
        RawResponseFormatter noDelimFormatter = new RawResponseFormatter("");
        RespValue value = new RespValue.Array(List.of(
                new RespValue.BlobString("a"),
                new RespValue.BlobString("b"),
                new RespValue.BlobString("c")
        ));
        assertEquals("abc", noDelimFormatter.format(value));
    }
}
