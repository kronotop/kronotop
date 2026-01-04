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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class RespReaderTest {

    private RespReader readerFor(String data) {
        return new RespReader(new ByteArrayInputStream(data.getBytes(StandardCharsets.UTF_8)));
    }

    @Test
    void shouldReadBlobString() throws IOException {
        RespReader reader = readerFor("$11\r\nhello world\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.BlobString.class, value);
        assertEquals("hello world", ((RespValue.BlobString) value).value());
    }

    @Test
    void shouldReadEmptyBlobString() throws IOException {
        RespReader reader = readerFor("$0\r\n\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.BlobString.class, value);
        assertEquals("", ((RespValue.BlobString) value).value());
    }

    @Test
    void shouldReadSimpleString() throws IOException {
        RespReader reader = readerFor("+OK\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.SimpleString.class, value);
        assertEquals("OK", ((RespValue.SimpleString) value).value());
    }

    @Test
    void shouldReadSimpleError() throws IOException {
        RespReader reader = readerFor("-ERR unknown command\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.SimpleError.class, value);
        RespValue.SimpleError error = (RespValue.SimpleError) value;
        assertEquals("ERR", error.code());
        assertEquals("unknown command", error.message());
    }

    @Test
    void shouldReadSimpleErrorWithoutMessage() throws IOException {
        RespReader reader = readerFor("-ERR\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.SimpleError.class, value);
        RespValue.SimpleError error = (RespValue.SimpleError) value;
        assertEquals("ERR", error.code());
        assertEquals("", error.message());
    }

    @Test
    void shouldReadPositiveNumber() throws IOException {
        RespReader reader = readerFor(":1234\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Number.class, value);
        assertEquals(1234L, ((RespValue.Number) value).value());
    }

    @Test
    void shouldReadNegativeNumber() throws IOException {
        RespReader reader = readerFor(":-5678\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Number.class, value);
        assertEquals(-5678L, ((RespValue.Number) value).value());
    }

    @Test
    void shouldReadNull() throws IOException {
        RespReader reader = readerFor("_\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Null.class, value);
    }

    @Test
    void shouldReadDouble() throws IOException {
        RespReader reader = readerFor(",3.14159\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Double.class, value);
        assertEquals(3.14159, ((RespValue.Double) value).value(), 0.00001);
    }

    @Test
    void shouldReadPositiveInfinity() throws IOException {
        RespReader reader = readerFor(",inf\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Double.class, value);
        assertEquals(Double.POSITIVE_INFINITY, ((RespValue.Double) value).value());
    }

    @Test
    void shouldReadNegativeInfinity() throws IOException {
        RespReader reader = readerFor(",-inf\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Double.class, value);
        assertEquals(Double.NEGATIVE_INFINITY, ((RespValue.Double) value).value());
    }

    @Test
    void shouldReadNaN() throws IOException {
        RespReader reader = readerFor(",nan\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Double.class, value);
        assertTrue(Double.isNaN(((RespValue.Double) value).value()));
    }

    @Test
    void shouldReadBooleanTrue() throws IOException {
        RespReader reader = readerFor("#t\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Boolean.class, value);
        assertTrue(((RespValue.Boolean) value).value());
    }

    @Test
    void shouldReadBooleanFalse() throws IOException {
        RespReader reader = readerFor("#f\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Boolean.class, value);
        assertFalse(((RespValue.Boolean) value).value());
    }

    @Test
    void shouldReadBlobError() throws IOException {
        RespReader reader = readerFor("!21\r\nSYNTAX invalid syntax\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.BlobError.class, value);
        RespValue.BlobError error = (RespValue.BlobError) value;
        assertEquals("SYNTAX", error.code());
        assertEquals("invalid syntax", error.message());
    }

    @Test
    void shouldReadVerbatimString() throws IOException {
        RespReader reader = readerFor("=15\r\ntxt:hello world\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.VerbatimString.class, value);
        RespValue.VerbatimString vs = (RespValue.VerbatimString) value;
        assertEquals("txt", vs.format());
        assertEquals("hello world", vs.value());
    }

    @Test
    void shouldReadBigNumber() throws IOException {
        RespReader reader = readerFor("(3492890328409238509324850943850943825024385\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.BigNumber.class, value);
        assertEquals(new BigInteger("3492890328409238509324850943850943825024385"),
                ((RespValue.BigNumber) value).value());
    }

    @Test
    void shouldReadEmptyArray() throws IOException {
        RespReader reader = readerFor("*0\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Array.class, value);
        assertTrue(((RespValue.Array) value).values().isEmpty());
    }

    @Test
    void shouldReadArrayOfNumbers() throws IOException {
        RespReader reader = readerFor("*3\r\n:1\r\n:2\r\n:3\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Array.class, value);
        RespValue.Array array = (RespValue.Array) value;
        assertEquals(3, array.values().size());
        assertEquals(1L, ((RespValue.Number) array.values().get(0)).value());
        assertEquals(2L, ((RespValue.Number) array.values().get(1)).value());
        assertEquals(3L, ((RespValue.Number) array.values().get(2)).value());
    }

    @Test
    void shouldReadNestedArray() throws IOException {
        RespReader reader = readerFor("*2\r\n*2\r\n:1\r\n:2\r\n*2\r\n:3\r\n:4\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Array.class, value);
        RespValue.Array outer = (RespValue.Array) value;
        assertEquals(2, outer.values().size());

        RespValue.Array inner1 = (RespValue.Array) outer.values().get(0);
        assertEquals(2, inner1.values().size());
        assertEquals(1L, ((RespValue.Number) inner1.values().get(0)).value());
        assertEquals(2L, ((RespValue.Number) inner1.values().get(1)).value());

        RespValue.Array inner2 = (RespValue.Array) outer.values().get(1);
        assertEquals(2, inner2.values().size());
        assertEquals(3L, ((RespValue.Number) inner2.values().get(0)).value());
        assertEquals(4L, ((RespValue.Number) inner2.values().get(1)).value());
    }

    @Test
    void shouldReadMap() throws IOException {
        RespReader reader = readerFor("%2\r\n+first\r\n:1\r\n+second\r\n:2\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.RespMap.class, value);
        RespValue.RespMap map = (RespValue.RespMap) value;
        assertEquals(2, map.values().size());
    }

    @Test
    void shouldReadSet() throws IOException {
        RespReader reader = readerFor("~3\r\n+a\r\n+b\r\n+c\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.RespSet.class, value);
        RespValue.RespSet set = (RespValue.RespSet) value;
        assertEquals(3, set.values().size());
    }

    @Test
    void shouldReadAttribute() throws IOException {
        RespReader reader = readerFor("|1\r\n+key\r\n+value\r\n+OK\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Attribute.class, value);
        RespValue.Attribute attr = (RespValue.Attribute) value;
        assertEquals(1, attr.attributes().size());
        assertInstanceOf(RespValue.SimpleString.class, attr.value());
        assertEquals("OK", ((RespValue.SimpleString) attr.value()).value());
    }

    @Test
    void shouldReadPush() throws IOException {
        RespReader reader = readerFor(">3\r\n+message\r\n+channel\r\n+payload\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Push.class, value);
        RespValue.Push push = (RespValue.Push) value;
        assertEquals("message", push.kind());
        assertEquals(2, push.values().size());
    }

    @Test
    void shouldReadMixedArray() throws IOException {
        RespReader reader = readerFor("*4\r\n+hello\r\n:42\r\n$5\r\nworld\r\n#t\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Array.class, value);
        RespValue.Array array = (RespValue.Array) value;
        assertEquals(4, array.values().size());

        assertInstanceOf(RespValue.SimpleString.class, array.values().get(0));
        assertInstanceOf(RespValue.Number.class, array.values().get(1));
        assertInstanceOf(RespValue.BlobString.class, array.values().get(2));
        assertInstanceOf(RespValue.Boolean.class, array.values().get(3));
    }

    // RESP2 compatibility tests

    @Test
    void shouldReadResp2NullBulkString() throws IOException {
        RespReader reader = readerFor("$-1\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Null.class, value);
    }

    @Test
    void shouldReadResp2NullArray() throws IOException {
        RespReader reader = readerFor("*-1\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Null.class, value);
    }

    @Test
    void shouldReadArrayWithResp2NullElement() throws IOException {
        RespReader reader = readerFor("*3\r\n$3\r\nfoo\r\n$-1\r\n$3\r\nbar\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Array.class, value);
        RespValue.Array array = (RespValue.Array) value;
        assertEquals(3, array.values().size());

        assertInstanceOf(RespValue.BlobString.class, array.values().get(0));
        assertEquals("foo", ((RespValue.BlobString) array.values().get(0)).value());

        assertInstanceOf(RespValue.Null.class, array.values().get(1));

        assertInstanceOf(RespValue.BlobString.class, array.values().get(2));
        assertEquals("bar", ((RespValue.BlobString) array.values().get(2)).value());
    }

    // RESP3 streaming tests

    @Test
    void shouldReadStreamedBlobString() throws IOException {
        // $?\r\n;5\r\nhello\r\n;6\r\n world\r\n;0\r\n
        RespReader reader = readerFor("$?\r\n;5\r\nhello\r\n;6\r\n world\r\n;0\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.BlobString.class, value);
        assertEquals("hello world", ((RespValue.BlobString) value).value());
    }

    @Test
    void shouldReadStreamedArray() throws IOException {
        // *?\r\n:1\r\n:2\r\n:3\r\n.\r\n
        RespReader reader = readerFor("*?\r\n:1\r\n:2\r\n:3\r\n.\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Array.class, value);
        RespValue.Array array = (RespValue.Array) value;
        assertEquals(3, array.values().size());
        assertEquals(1L, ((RespValue.Number) array.values().get(0)).value());
        assertEquals(2L, ((RespValue.Number) array.values().get(1)).value());
        assertEquals(3L, ((RespValue.Number) array.values().get(2)).value());
    }

    @Test
    void shouldReadStreamedMap() throws IOException {
        // %?\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n.\r\n
        RespReader reader = readerFor("%?\r\n+key1\r\n:1\r\n+key2\r\n:2\r\n.\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.RespMap.class, value);
        RespValue.RespMap map = (RespValue.RespMap) value;
        assertEquals(2, map.values().size());
    }

    @Test
    void shouldReadStreamedSet() throws IOException {
        // ~?\r\n+a\r\n+b\r\n.\r\n
        RespReader reader = readerFor("~?\r\n+a\r\n+b\r\n.\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.RespSet.class, value);
        RespValue.RespSet set = (RespValue.RespSet) value;
        assertEquals(2, set.values().size());
    }

    @Test
    void shouldReadEmptyStreamedArray() throws IOException {
        // *?\r\n.\r\n
        RespReader reader = readerFor("*?\r\n.\r\n");
        RespValue value = reader.read();

        assertInstanceOf(RespValue.Array.class, value);
        assertTrue(((RespValue.Array) value).values().isEmpty());
    }
}
