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

class CsvResponseFormatterTest {

    private final CsvResponseFormatter formatter = new CsvResponseFormatter();

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
    void shouldFormatStringWithComma() {
        RespValue value = new RespValue.BlobString("hello,world");
        assertEquals("\"hello,world\"", formatter.format(value));
    }

    @Test
    void shouldFormatStringWithQuotes() {
        RespValue value = new RespValue.BlobString("say \"hello\"");
        assertEquals("\"say \\\"hello\\\"\"", formatter.format(value));
    }

    @Test
    void shouldFormatStringWithNewline() {
        RespValue value = new RespValue.BlobString("hello\nworld");
        assertEquals("\"hello\nworld\"", formatter.format(value));
    }

    @Test
    void shouldFormatSimpleError() {
        RespValue value = new RespValue.SimpleError("ERR", "unknown command");
        assertEquals("\"ERR unknown command\"", formatter.format(value));
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
        assertEquals("\"true\"", formatter.format(value));
    }

    @Test
    void shouldFormatBooleanFalse() {
        RespValue value = new RespValue.Boolean(false);
        assertEquals("\"false\"", formatter.format(value));
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
        assertEquals("\"a\",\"b\",\"c\"", formatter.format(value));
    }

    @Test
    void shouldFormatArrayOfNumbers() {
        RespValue value = new RespValue.Array(List.of(
                new RespValue.Number(1),
                new RespValue.Number(2),
                new RespValue.Number(3)
        ));
        assertEquals("1,2,3", formatter.format(value));
    }

    @Test
    void shouldFormatMixedArray() {
        RespValue value = new RespValue.Array(List.of(
                new RespValue.BlobString("hello"),
                new RespValue.Number(42),
                new RespValue.Boolean(true)
        ));
        assertEquals("\"hello\",42,\"true\"", formatter.format(value));
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
        assertEquals("1,2,3,4", formatter.format(value));
    }

    @Test
    void shouldFormatEmptyArrayAsEmptyField() {
        Map<RespValue, RespValue> map = new LinkedHashMap<>();
        map.put(new RespValue.SimpleString("key"), new RespValue.BlobString("value"));
        map.put(new RespValue.SimpleString("empty"), new RespValue.Array(List.of()));
        map.put(new RespValue.SimpleString("next"), new RespValue.BlobString("after"));
        RespValue value = new RespValue.RespMap(map);
        assertEquals("\"key\",\"value\",\"empty\",,\"next\",\"after\"", formatter.format(value));
    }

    @Test
    void shouldFormatMapAsKeyValuePairs() {
        Map<RespValue, RespValue> map = new LinkedHashMap<>();
        map.put(new RespValue.SimpleString("name"), new RespValue.BlobString("alice"));
        map.put(new RespValue.SimpleString("age"), new RespValue.Number(30));
        RespValue value = new RespValue.RespMap(map);
        assertEquals("\"name\",\"alice\",\"age\",30", formatter.format(value));
    }

    @Test
    void shouldFormatPush() {
        RespValue value = new RespValue.Push("message", List.of(
                new RespValue.BlobString("channel1"),
                new RespValue.BlobString("hello")
        ));
        assertEquals("\"message\",\"channel1\",\"hello\"", formatter.format(value));
    }

    @Test
    void shouldFormatAttribute() {
        Map<RespValue, RespValue> attrs = new LinkedHashMap<>();
        attrs.put(new RespValue.SimpleString("ttl"), new RespValue.Number(100));
        RespValue value = new RespValue.Attribute(attrs, new RespValue.SimpleString("OK"));
        assertEquals("\"OK\"", formatter.format(value));
    }
}
