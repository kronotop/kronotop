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
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonResponseFormatterTest {

    private final JsonResponseFormatter formatter = new JsonResponseFormatter();
    private final JsonResponseFormatter asciiSafeFormatter = new JsonResponseFormatter(true);

    @Test
    void shouldFormatSimpleString() {
        RespValue value = new RespValue.SimpleString("OK");
        assertEquals("\"OK\"", formatter.format(value));
    }

    @Test
    void shouldFormatBlobString() {
        RespValue value = new RespValue.BlobString("hello world");
        assertEquals("\"hello world\"", formatter.format(value));
    }

    @Test
    void shouldFormatStringWithSpecialChars() {
        RespValue value = new RespValue.BlobString("hello\nworld");
        assertEquals("\"hello\\nworld\"", formatter.format(value));
    }

    @Test
    void shouldFormatStringWithQuotes() {
        RespValue value = new RespValue.BlobString("say \"hello\"");
        assertEquals("\"say \\\"hello\\\"\"", formatter.format(value));
    }

    @Test
    void shouldFormatSimpleError() {
        RespValue value = new RespValue.SimpleError("ERR", "unknown command");
        assertEquals("{\"error\":\"ERR unknown command\"}", formatter.format(value));
    }

    @Test
    void shouldFormatBlobError() {
        RespValue value = new RespValue.BlobError("SYNTAX", "invalid syntax");
        assertEquals("{\"error\":\"SYNTAX invalid syntax\"}", formatter.format(value));
    }

    @Test
    void shouldFormatNumber() {
        RespValue value = new RespValue.Number(42);
        assertEquals("42", formatter.format(value));
    }

    @Test
    void shouldFormatNegativeNumber() {
        RespValue value = new RespValue.Number(-123);
        assertEquals("-123", formatter.format(value));
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
        assertEquals("null", formatter.format(value));
    }

    @Test
    void shouldFormatBigNumber() {
        RespValue value = new RespValue.BigNumber(new BigInteger("12345678901234567890"));
        assertEquals("12345678901234567890", formatter.format(value));
    }

    @Test
    void shouldFormatVerbatimString() {
        RespValue value = new RespValue.VerbatimString("txt", "hello world");
        assertEquals("\"hello world\"", formatter.format(value));
    }

    @Test
    void shouldFormatEmptyArray() {
        RespValue value = new RespValue.Array(List.of());
        assertEquals("[]", formatter.format(value));
    }

    @Test
    void shouldFormatArrayOfNumbers() {
        RespValue value = new RespValue.Array(List.of(
                new RespValue.Number(1),
                new RespValue.Number(2),
                new RespValue.Number(3)
        ));
        assertEquals("[1,2,3]", formatter.format(value));
    }

    @Test
    void shouldFormatArrayOfStrings() {
        RespValue value = new RespValue.Array(List.of(
                new RespValue.BlobString("a"),
                new RespValue.BlobString("b"),
                new RespValue.BlobString("c")
        ));
        assertEquals("[\"a\",\"b\",\"c\"]", formatter.format(value));
    }

    @Test
    void shouldFormatMixedArray() {
        RespValue value = new RespValue.Array(List.of(
                new RespValue.BlobString("hello"),
                new RespValue.Number(42),
                new RespValue.Boolean(true),
                new RespValue.Null()
        ));
        assertEquals("[\"hello\",42,true,null]", formatter.format(value));
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
        assertEquals("[[1,2],[3,4]]", formatter.format(value));
    }

    @Test
    void shouldFormatEmptyMap() {
        RespValue value = new RespValue.RespMap(Map.of());
        assertEquals("{}", formatter.format(value));
    }

    @Test
    void shouldFormatMapWithSimpleValues() {
        Map<RespValue, RespValue> map = new LinkedHashMap<>();
        map.put(new RespValue.SimpleString("name"), new RespValue.BlobString("alice"));
        map.put(new RespValue.SimpleString("age"), new RespValue.Number(30));
        RespValue value = new RespValue.RespMap(map);
        String result = formatter.format(value);
        // JSON object key order may vary, so check contains
        assertEquals(true, result.contains("\"name\":\"alice\""));
        assertEquals(true, result.contains("\"age\":30"));
    }

    @Test
    void shouldFormatMapWithNestedArray() {
        Map<RespValue, RespValue> map = new LinkedHashMap<>();
        map.put(new RespValue.SimpleString("items"), new RespValue.Array(List.of(
                new RespValue.BlobString("a"),
                new RespValue.BlobString("b")
        )));
        RespValue value = new RespValue.RespMap(map);
        assertEquals("{\"items\":[\"a\",\"b\"]}", formatter.format(value));
    }

    @Test
    void shouldFormatMapWithNestedMap() {
        Map<RespValue, RespValue> inner = new LinkedHashMap<>();
        inner.put(new RespValue.SimpleString("x"), new RespValue.Number(1));

        Map<RespValue, RespValue> outer = new LinkedHashMap<>();
        outer.put(new RespValue.SimpleString("nested"), new RespValue.RespMap(inner));

        RespValue value = new RespValue.RespMap(outer);
        assertEquals("{\"nested\":{\"x\":1}}", formatter.format(value));
    }

    @Test
    void shouldFormatEmptySet() {
        RespValue value = new RespValue.RespSet(Set.of());
        assertEquals("[]", formatter.format(value));
    }

    @Test
    void shouldFormatPush() {
        RespValue value = new RespValue.Push("message", List.of(
                new RespValue.BlobString("channel1"),
                new RespValue.BlobString("hello")
        ));
        String result = formatter.format(value);
        assertEquals(true, result.contains("\"kind\":\"message\""));
        assertEquals(true, result.contains("\"values\":[\"channel1\",\"hello\"]"));
    }

    @Test
    void shouldFormatAttribute() {
        Map<RespValue, RespValue> attrs = new LinkedHashMap<>();
        attrs.put(new RespValue.SimpleString("ttl"), new RespValue.Number(100));
        RespValue value = new RespValue.Attribute(attrs, new RespValue.SimpleString("OK"));
        assertEquals("\"OK\"", formatter.format(value));
    }

    @Test
    void shouldFormatComplexNestedStructure() {
        Map<RespValue, RespValue> shard = new LinkedHashMap<>();
        shard.put(new RespValue.SimpleString("primary"), new RespValue.BlobString("node1"));
        shard.put(new RespValue.SimpleString("standbys"), new RespValue.Array(List.of()));
        shard.put(new RespValue.SimpleString("status"), new RespValue.BlobString("READWRITE"));

        Map<RespValue, RespValue> redis = new LinkedHashMap<>();
        redis.put(new RespValue.SimpleString("0"), new RespValue.RespMap(shard));

        Map<RespValue, RespValue> root = new LinkedHashMap<>();
        root.put(new RespValue.SimpleString("redis"), new RespValue.RespMap(redis));

        RespValue value = new RespValue.RespMap(root);
        String result = formatter.format(value);
        // JSON object key order may vary
        assertTrue(result.contains("\"redis\":{"));
        assertTrue(result.contains("\"primary\":\"node1\""));
        assertTrue(result.contains("\"standbys\":[]"));
        assertTrue(result.contains("\"status\":\"READWRITE\""));
    }

    @Test
    void shouldEscapeNonAsciiCharacters() {
        RespValue value = new RespValue.BlobString("héllo wörld");
        String result = asciiSafeFormatter.format(value);
        assertEquals("\"h\\u00e9llo w\\u00f6rld\"", result);
    }

    @Test
    void shouldEscapeUnicodeEmoji() {
        RespValue value = new RespValue.BlobString("hello 🌍");
        String result = asciiSafeFormatter.format(value);
        assertTrue(result.contains("\\u"));
    }

    @Test
    void shouldNotEscapeAsciiCharacters() {
        RespValue value = new RespValue.BlobString("hello world");
        String result = asciiSafeFormatter.format(value);
        assertEquals("\"hello world\"", result);
    }

    @Test
    void shouldEscapeNonAsciiInMap() {
        Map<RespValue, RespValue> map = new LinkedHashMap<>();
        map.put(new RespValue.SimpleString("name"), new RespValue.BlobString("José"));
        RespValue value = new RespValue.RespMap(map);
        String result = asciiSafeFormatter.format(value);
        assertTrue(result.contains("\\u00e9"));
    }
}
