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

package com.kronotop.cli.resp;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RespWriterTest {

    private String write(RespValue value) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        RespWriter writer = new RespWriter(baos);
        writer.write(value);
        return baos.toString(StandardCharsets.UTF_8);
    }

    @Test
    void shouldWriteCommand() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        RespWriter writer = new RespWriter(baos);
        writer.writeCommand(List.of("SET", "key", "value"));

        assertEquals("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n", baos.toString(StandardCharsets.UTF_8));
    }

    @Test
    void shouldWriteBlobString() throws IOException {
        String result = write(new RespValue.BlobString("hello world"));
        assertEquals("$11\r\nhello world\r\n", result);
    }

    @Test
    void shouldWriteEmptyBlobString() throws IOException {
        String result = write(new RespValue.BlobString(""));
        assertEquals("$0\r\n\r\n", result);
    }

    @Test
    void shouldWriteSimpleString() throws IOException {
        String result = write(new RespValue.SimpleString("OK"));
        assertEquals("+OK\r\n", result);
    }

    @Test
    void shouldWriteSimpleError() throws IOException {
        String result = write(new RespValue.SimpleError("ERR", "unknown command"));
        assertEquals("-ERR unknown command\r\n", result);
    }

    @Test
    void shouldWriteNumber() throws IOException {
        String result = write(new RespValue.Number(1234));
        assertEquals(":1234\r\n", result);
    }

    @Test
    void shouldWriteNegativeNumber() throws IOException {
        String result = write(new RespValue.Number(-5678));
        assertEquals(":-5678\r\n", result);
    }

    @Test
    void shouldWriteNull() throws IOException {
        String result = write(new RespValue.Null());
        assertEquals("_\r\n", result);
    }

    @Test
    void shouldWriteDouble() throws IOException {
        String result = write(new RespValue.Double(3.14));
        assertEquals(",3.14\r\n", result);
    }

    @Test
    void shouldWritePositiveInfinity() throws IOException {
        String result = write(new RespValue.Double(Double.POSITIVE_INFINITY));
        assertEquals(",inf\r\n", result);
    }

    @Test
    void shouldWriteNegativeInfinity() throws IOException {
        String result = write(new RespValue.Double(Double.NEGATIVE_INFINITY));
        assertEquals(",-inf\r\n", result);
    }

    @Test
    void shouldWriteNaN() throws IOException {
        String result = write(new RespValue.Double(Double.NaN));
        assertEquals(",nan\r\n", result);
    }

    @Test
    void shouldWriteBooleanTrue() throws IOException {
        String result = write(new RespValue.Boolean(true));
        assertEquals("#t\r\n", result);
    }

    @Test
    void shouldWriteBooleanFalse() throws IOException {
        String result = write(new RespValue.Boolean(false));
        assertEquals("#f\r\n", result);
    }

    @Test
    void shouldWriteBlobError() throws IOException {
        String result = write(new RespValue.BlobError("SYNTAX", "invalid syntax"));
        assertEquals("!21\r\nSYNTAX invalid syntax\r\n", result);
    }

    @Test
    void shouldWriteVerbatimString() throws IOException {
        String result = write(new RespValue.VerbatimString("txt", "hello world"));
        assertEquals("=15\r\ntxt:hello world\r\n", result);
    }

    @Test
    void shouldWriteBigNumber() throws IOException {
        String result = write(new RespValue.BigNumber(new BigInteger("3492890328409238509324850943850943825024385")));
        assertEquals("(3492890328409238509324850943850943825024385\r\n", result);
    }

    @Test
    void shouldWriteEmptyArray() throws IOException {
        String result = write(new RespValue.Array(List.of()));
        assertEquals("*0\r\n", result);
    }

    @Test
    void shouldWriteArrayOfNumbers() throws IOException {
        String result = write(new RespValue.Array(List.of(
                new RespValue.Number(1),
                new RespValue.Number(2),
                new RespValue.Number(3)
        )));
        assertEquals("*3\r\n:1\r\n:2\r\n:3\r\n", result);
    }

    @Test
    void shouldWriteNestedArray() throws IOException {
        String result = write(new RespValue.Array(List.of(
                new RespValue.Array(List.of(new RespValue.Number(1), new RespValue.Number(2))),
                new RespValue.Array(List.of(new RespValue.Number(3), new RespValue.Number(4)))
        )));
        assertEquals("*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n", result);
    }

    @Test
    void shouldWriteMap() throws IOException {
        Map<RespValue, RespValue> map = new LinkedHashMap<>();
        map.put(new RespValue.SimpleString("first"), new RespValue.Number(1));
        map.put(new RespValue.SimpleString("second"), new RespValue.Number(2));

        String result = write(new RespValue.RespMap(map));
        assertEquals("%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n", result);
    }

    @Test
    void shouldWriteSet() throws IOException {
        // Use a list-based approach to maintain order for testing
        String result = write(new RespValue.RespSet(Set.of(
                new RespValue.SimpleString("a")
        )));
        assertEquals("~1\r\n+a\r\n", result);
    }

    @Test
    void shouldWriteAttribute() throws IOException {
        Map<RespValue, RespValue> attrs = new LinkedHashMap<>();
        attrs.put(new RespValue.SimpleString("key"), new RespValue.SimpleString("value"));

        String result = write(new RespValue.Attribute(attrs, new RespValue.SimpleString("OK")));
        assertEquals("|1\r\n+key\r\n+value\r\n+OK\r\n", result);
    }

    @Test
    void shouldWritePush() throws IOException {
        String result = write(new RespValue.Push("message", List.of(
                new RespValue.SimpleString("channel"),
                new RespValue.SimpleString("payload")
        )));
        assertEquals(">3\r\n+message\r\n+channel\r\n+payload\r\n", result);
    }

    @Test
    void shouldRoundTripBlobString() throws IOException {
        RespValue original = new RespValue.BlobString("hello world");
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new RespWriter(baos).write(original);

        RespReader reader = new RespReader(new java.io.ByteArrayInputStream(baos.toByteArray()));
        RespValue parsed = reader.read();

        assertEquals(original, parsed);
    }

    @Test
    void shouldRoundTripArray() throws IOException {
        RespValue original = new RespValue.Array(List.of(
                new RespValue.SimpleString("hello"),
                new RespValue.Number(42),
                new RespValue.Boolean(true)
        ));
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new RespWriter(baos).write(original);

        RespReader reader = new RespReader(new java.io.ByteArrayInputStream(baos.toByteArray()));
        RespValue parsed = reader.read();

        assertEquals(original, parsed);
    }
}
