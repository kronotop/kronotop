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

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Writes RESP3 protocol data to an output stream.
 */
public class RespWriter {

    private static final byte[] CRLF = "\r\n".getBytes(StandardCharsets.UTF_8);

    private final OutputStream output;

    public RespWriter(OutputStream output) {
        this.output = output;
    }

    /**
     * Writes a command as a RESP3 array of bulk strings.
     */
    public void writeCommand(List<String> args) throws IOException {
        // Write array header
        output.write('*');
        output.write(String.valueOf(args.size()).getBytes(StandardCharsets.UTF_8));
        output.write(CRLF);

        // Write each argument as a bulk string
        for (String arg : args) {
            writeBlobString(arg);
        }
        output.flush();
    }

    /**
     * Writes a blob string.
     */
    public void writeBlobString(String value) throws IOException {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        output.write('$');
        output.write(String.valueOf(bytes.length).getBytes(StandardCharsets.UTF_8));
        output.write(CRLF);
        output.write(bytes);
        output.write(CRLF);
    }

    /**
     * Writes a simple string.
     */
    public void writeSimpleString(String value) throws IOException {
        output.write('+');
        output.write(value.getBytes(StandardCharsets.UTF_8));
        output.write(CRLF);
        output.flush();
    }

    /**
     * Writes a number.
     */
    public void writeNumber(long value) throws IOException {
        output.write(':');
        output.write(String.valueOf(value).getBytes(StandardCharsets.UTF_8));
        output.write(CRLF);
        output.flush();
    }

    /**
     * Writes a RESP3 value.
     */
    public void write(RespValue value) throws IOException {
        switch (value) {
            case RespValue.BlobString bs -> writeBlobString(bs.value());
            case RespValue.SimpleString ss -> writeSimpleString(ss.value());
            case RespValue.SimpleError se -> {
                output.write('-');
                output.write((se.code() + " " + se.message()).getBytes(StandardCharsets.UTF_8));
                output.write(CRLF);
            }
            case RespValue.Number n -> writeNumber(n.value());
            case RespValue.Null ignored -> {
                output.write('_');
                output.write(CRLF);
            }
            case RespValue.Double d -> {
                output.write(',');
                if (java.lang.Double.isInfinite(d.value())) {
                    output.write((d.value() > 0 ? "inf" : "-inf").getBytes(StandardCharsets.UTF_8));
                } else if (java.lang.Double.isNaN(d.value())) {
                    output.write("nan".getBytes(StandardCharsets.UTF_8));
                } else {
                    output.write(String.valueOf(d.value()).getBytes(StandardCharsets.UTF_8));
                }
                output.write(CRLF);
            }
            case RespValue.Boolean b -> {
                output.write('#');
                output.write(b.value() ? 't' : 'f');
                output.write(CRLF);
            }
            case RespValue.BlobError be -> {
                String content = be.code() + " " + be.message();
                byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
                output.write('!');
                output.write(String.valueOf(bytes.length).getBytes(StandardCharsets.UTF_8));
                output.write(CRLF);
                output.write(bytes);
                output.write(CRLF);
            }
            case RespValue.VerbatimString vs -> {
                String content = vs.format() + ":" + vs.value();
                byte[] bytes = content.getBytes(StandardCharsets.UTF_8);
                output.write('=');
                output.write(String.valueOf(bytes.length).getBytes(StandardCharsets.UTF_8));
                output.write(CRLF);
                output.write(bytes);
                output.write(CRLF);
            }
            case RespValue.BigNumber bn -> {
                output.write('(');
                output.write(bn.value().toString().getBytes(StandardCharsets.UTF_8));
                output.write(CRLF);
            }
            case RespValue.Array arr -> {
                output.write('*');
                output.write(String.valueOf(arr.values().size()).getBytes(StandardCharsets.UTF_8));
                output.write(CRLF);
                for (RespValue v : arr.values()) {
                    write(v);
                }
            }
            case RespValue.RespMap m -> {
                output.write('%');
                output.write(String.valueOf(m.values().size()).getBytes(StandardCharsets.UTF_8));
                output.write(CRLF);
                for (var entry : m.values().entrySet()) {
                    write(entry.getKey());
                    write(entry.getValue());
                }
            }
            case RespValue.RespSet s -> {
                output.write('~');
                output.write(String.valueOf(s.values().size()).getBytes(StandardCharsets.UTF_8));
                output.write(CRLF);
                for (RespValue v : s.values()) {
                    write(v);
                }
            }
            case RespValue.Attribute attr -> {
                output.write('|');
                output.write(String.valueOf(attr.attributes().size()).getBytes(StandardCharsets.UTF_8));
                output.write(CRLF);
                for (var entry : attr.attributes().entrySet()) {
                    write(entry.getKey());
                    write(entry.getValue());
                }
                write(attr.value());
            }
            case RespValue.Push p -> {
                output.write('>');
                output.write(String.valueOf(p.values().size() + 1).getBytes(StandardCharsets.UTF_8));
                output.write(CRLF);
                writeSimpleString(p.kind());
                for (RespValue v : p.values()) {
                    write(v);
                }
            }
        }
        output.flush();
    }
}
