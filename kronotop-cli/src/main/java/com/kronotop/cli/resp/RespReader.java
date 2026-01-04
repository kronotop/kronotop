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

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Reads and parses RESP3 protocol data from an input stream.
 */
public class RespReader {

    private final BufferedInputStream input;

    public RespReader(InputStream input) {
        this.input = new BufferedInputStream(input);
    }

    public RespValue read() throws IOException {
        int typeChar = input.read();
        if (typeChar == -1) {
            throw new IOException("Unexpected end of stream");
        }

        RespType type = RespType.fromPrefix((char) typeChar);
        return switch (type) {
            case BLOB_STRING -> readBlobString();
            case SIMPLE_STRING -> readSimpleString();
            case SIMPLE_ERROR -> readSimpleError();
            case NUMBER -> readNumber();
            case NULL -> readNull();
            case DOUBLE -> readDouble();
            case BOOLEAN -> readBoolean();
            case BLOB_ERROR -> readBlobError();
            case VERBATIM_STRING -> readVerbatimString();
            case BIG_NUMBER -> readBigNumber();
            case ARRAY -> readArray();
            case MAP -> readMap();
            case SET -> readSet();
            case ATTRIBUTE -> readAttribute();
            case PUSH -> readPush();
            case STREAMED_STRING_CHUNK -> readStreamedStringChunk();
            case STREAMED_END -> readStreamedEnd();
        };
    }

    private String readLine() throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        int b;
        while ((b = input.read()) != -1) {
            if (b == '\r') {
                int next = input.read();
                if (next == '\n') {
                    break;
                }
                baos.write(b);
                if (next != -1) {
                    baos.write(next);
                }
            } else {
                baos.write(b);
            }
        }
        return baos.toString(StandardCharsets.UTF_8);
    }

    private byte[] readBytes(int length) throws IOException {
        byte[] bytes = new byte[length];
        int bytesRead = 0;
        while (bytesRead < length) {
            int read = input.read(bytes, bytesRead, length - bytesRead);
            if (read == -1) {
                throw new IOException("Unexpected end of stream");
            }
            bytesRead += read;
        }
        // Read trailing \r\n
        input.read();
        input.read();
        return bytes;
    }

    private RespValue readBlobString() throws IOException {
        String line = readLine();
        // Streamed blob string: $?\r\n
        if (line.equals("?")) {
            return readStreamedBlobString();
        }
        int length = Integer.parseInt(line);
        // RESP2 null bulk string: $-1\r\n
        if (length == -1) {
            return new RespValue.Null();
        }
        byte[] bytes = readBytes(length);
        return new RespValue.BlobString(new String(bytes, StandardCharsets.UTF_8));
    }

    private RespValue.BlobString readStreamedBlobString() throws IOException {
        StringBuilder sb = new StringBuilder();
        while (true) {
            int typeChar = input.read();
            if (typeChar == -1) {
                throw new IOException("Unexpected end of stream in streamed string");
            }
            if (typeChar != ';') {
                throw new IOException("Expected ';' chunk marker in streamed string, got: " + (char) typeChar);
            }
            int chunkLength = Integer.parseInt(readLine());
            if (chunkLength == 0) {
                break;
            }
            byte[] bytes = readBytes(chunkLength);
            sb.append(new String(bytes, StandardCharsets.UTF_8));
        }
        return new RespValue.BlobString(sb.toString());
    }

    private RespValue.SimpleString readSimpleString() throws IOException {
        return new RespValue.SimpleString(readLine());
    }

    private RespValue.SimpleError readSimpleError() throws IOException {
        String line = readLine();
        int spaceIndex = line.indexOf(' ');
        if (spaceIndex == -1) {
            return new RespValue.SimpleError(line, "");
        }
        return new RespValue.SimpleError(line.substring(0, spaceIndex), line.substring(spaceIndex + 1));
    }

    private RespValue.Number readNumber() throws IOException {
        return new RespValue.Number(Long.parseLong(readLine()));
    }

    private RespValue.Null readNull() throws IOException {
        readLine(); // consume \r\n
        return new RespValue.Null();
    }

    private RespValue.Double readDouble() throws IOException {
        String line = readLine();
        double value = switch (line) {
            case "inf" -> java.lang.Double.POSITIVE_INFINITY;
            case "-inf" -> java.lang.Double.NEGATIVE_INFINITY;
            case "nan" -> java.lang.Double.NaN;
            default -> java.lang.Double.parseDouble(line);
        };
        return new RespValue.Double(value);
    }

    private RespValue.Boolean readBoolean() throws IOException {
        String line = readLine();
        return new RespValue.Boolean(line.equals("t"));
    }

    private RespValue.BlobError readBlobError() throws IOException {
        int length = Integer.parseInt(readLine());
        byte[] bytes = readBytes(length);
        String content = new String(bytes, StandardCharsets.UTF_8);
        int spaceIndex = content.indexOf(' ');
        if (spaceIndex == -1) {
            return new RespValue.BlobError(content, "");
        }
        return new RespValue.BlobError(content.substring(0, spaceIndex), content.substring(spaceIndex + 1));
    }

    private RespValue.VerbatimString readVerbatimString() throws IOException {
        int length = Integer.parseInt(readLine());
        byte[] bytes = readBytes(length);
        String content = new String(bytes, StandardCharsets.UTF_8);
        // Format is "fmt:content" where fmt is 3 characters
        String format = content.substring(0, 3);
        String value = content.substring(4); // skip "fmt:"
        return new RespValue.VerbatimString(format, value);
    }

    private RespValue.BigNumber readBigNumber() throws IOException {
        return new RespValue.BigNumber(new BigInteger(readLine()));
    }

    private RespValue readArray() throws IOException {
        String line = readLine();
        // Streamed array: *?\r\n
        if (line.equals("?")) {
            return readStreamedArray();
        }
        int count = Integer.parseInt(line);
        // RESP2 null array: *-1\r\n
        if (count == -1) {
            return new RespValue.Null();
        }
        List<RespValue> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(read());
        }
        return new RespValue.Array(values);
    }

    private RespValue.Array readStreamedArray() throws IOException {
        List<RespValue> values = new ArrayList<>();
        while (true) {
            input.mark(1);
            int typeChar = input.read();
            if (typeChar == '.') {
                readLine(); // consume \r\n after '.'
                break;
            }
            input.reset();
            values.add(read());
        }
        return new RespValue.Array(values);
    }

    private RespValue.RespMap readMap() throws IOException {
        String line = readLine();
        // Streamed map: %?\r\n
        if (line.equals("?")) {
            return readStreamedMap();
        }
        int pairCount = Integer.parseInt(line);
        Map<RespValue, RespValue> values = new LinkedHashMap<>(pairCount);
        for (int i = 0; i < pairCount; i++) {
            RespValue key = read();
            RespValue value = read();
            values.put(key, value);
        }
        return new RespValue.RespMap(values);
    }

    private RespValue.RespMap readStreamedMap() throws IOException {
        Map<RespValue, RespValue> values = new LinkedHashMap<>();
        while (true) {
            input.mark(1);
            int typeChar = input.read();
            if (typeChar == '.') {
                readLine(); // consume \r\n after '.'
                break;
            }
            input.reset();
            RespValue key = read();
            RespValue value = read();
            values.put(key, value);
        }
        return new RespValue.RespMap(values);
    }

    private RespValue.RespSet readSet() throws IOException {
        String line = readLine();
        // Streamed set: ~?\r\n
        if (line.equals("?")) {
            return readStreamedSet();
        }
        int count = Integer.parseInt(line);
        Set<RespValue> values = new HashSet<>(count);
        for (int i = 0; i < count; i++) {
            values.add(read());
        }
        return new RespValue.RespSet(values);
    }

    private RespValue.RespSet readStreamedSet() throws IOException {
        Set<RespValue> values = new HashSet<>();
        while (true) {
            input.mark(1);
            int typeChar = input.read();
            if (typeChar == '.') {
                readLine(); // consume \r\n after '.'
                break;
            }
            input.reset();
            values.add(read());
        }
        return new RespValue.RespSet(values);
    }

    private RespValue.Attribute readAttribute() throws IOException {
        int pairCount = Integer.parseInt(readLine());
        Map<RespValue, RespValue> attributes = new HashMap<>(pairCount);
        for (int i = 0; i < pairCount; i++) {
            RespValue key = read();
            RespValue value = read();
            attributes.put(key, value);
        }
        RespValue actualValue = read();
        return new RespValue.Attribute(attributes, actualValue);
    }

    private RespValue.Push readPush() throws IOException {
        int count = Integer.parseInt(readLine());
        List<RespValue> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(read());
        }
        // First element is the push type
        String kind = "";
        if (!values.isEmpty() && values.getFirst() instanceof RespValue.SimpleString(String value)) {
            kind = value;
            values = values.subList(1, values.size());
        }
        return new RespValue.Push(kind, values);
    }

    private RespValue readStreamedStringChunk() throws IOException {
        // This is called when ';' is encountered outside of streamed string context
        // Read and return as blob string chunk
        int length = Integer.parseInt(readLine());
        if (length == 0) {
            return new RespValue.Null();
        }
        byte[] bytes = readBytes(length);
        return new RespValue.BlobString(new String(bytes, StandardCharsets.UTF_8));
    }

    private RespValue readStreamedEnd() throws IOException {
        // End marker for streamed aggregates
        readLine(); // consume \r\n
        return new RespValue.Null();
    }
}
