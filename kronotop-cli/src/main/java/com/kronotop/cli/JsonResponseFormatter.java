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

/**
 * Formats RESP values as compact JSON output, preserving map key order.
 */
public class JsonResponseFormatter {

    private final boolean asciiSafe;

    public JsonResponseFormatter() {
        this(false);
    }

    public JsonResponseFormatter(boolean asciiSafe) {
        this.asciiSafe = asciiSafe;
    }

    public String format(RespValue value) {
        StringBuilder sb = new StringBuilder();
        formatValue(value, sb);
        String result = sb.toString();
        return asciiSafe ? escapeNonAscii(result) : result;
    }

    private String escapeNonAscii(String s) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c > 127) {
                sb.append(String.format("\\u%04x", (int) c));
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }

    private void formatValue(RespValue value, StringBuilder sb) {
        switch (value) {
            case RespValue.SimpleString ss -> appendQuotedString(ss.value(), sb);
            case RespValue.BlobString bs -> appendQuotedString(bs.value(), sb);
            case RespValue.SimpleError se -> formatError(se.code(), se.message(), sb);
            case RespValue.BlobError be -> formatError(be.code(), be.message(), sb);
            case RespValue.Number n -> sb.append(n.value());
            case RespValue.Double d -> sb.append(d.value());
            case RespValue.Boolean b -> sb.append(b.value());
            case RespValue.Null ignored -> sb.append("null");
            case RespValue.BigNumber bn -> sb.append(bn.value());
            case RespValue.VerbatimString vs -> appendQuotedString(vs.value(), sb);
            case RespValue.Array arr -> formatArray(arr, sb);
            case RespValue.RespMap m -> formatMap(m, sb);
            case RespValue.RespSet s -> formatSet(s, sb);
            case RespValue.Attribute attr -> formatValue(attr.value(), sb);
            case RespValue.Push p -> formatPush(p, sb);
        }
    }

    private void appendQuotedString(String value, StringBuilder sb) {
        sb.append('"');
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            switch (c) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\b' -> sb.append("\\b");
                case '\f' -> sb.append("\\f");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) {
                        sb.append(String.format("\\u%04x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        sb.append('"');
    }

    private void formatError(String code, String message, StringBuilder sb) {
        sb.append("{\"error\":");
        appendQuotedString(code + " " + message, sb);
        sb.append('}');
    }

    private void formatArray(RespValue.Array array, StringBuilder sb) {
        sb.append('[');
        boolean first = true;
        for (RespValue v : array.values()) {
            if (!first) sb.append(',');
            first = false;
            formatValue(v, sb);
        }
        sb.append(']');
    }

    private void formatMap(RespValue.RespMap map, StringBuilder sb) {
        sb.append('{');
        boolean first = true;
        for (var entry : map.values().entrySet()) {
            if (!first) sb.append(',');
            first = false;
            appendQuotedString(getKeyString(entry.getKey()), sb);
            sb.append(':');
            formatValue(entry.getValue(), sb);
        }
        sb.append('}');
    }

    private void formatSet(RespValue.RespSet set, StringBuilder sb) {
        sb.append('[');
        boolean first = true;
        for (RespValue v : set.values()) {
            if (!first) sb.append(',');
            first = false;
            formatValue(v, sb);
        }
        sb.append(']');
    }

    private void formatPush(RespValue.Push push, StringBuilder sb) {
        sb.append("{\"kind\":");
        appendQuotedString(push.kind(), sb);
        sb.append(",\"values\":[");
        boolean first = true;
        for (RespValue v : push.values()) {
            if (!first) sb.append(',');
            first = false;
            formatValue(v, sb);
        }
        sb.append("]}");
    }

    private String getKeyString(RespValue key) {
        return switch (key) {
            case RespValue.SimpleString ss -> ss.value();
            case RespValue.BlobString bs -> bs.value();
            case RespValue.Number n -> String.valueOf(n.value());
            case RespValue.BigNumber bn -> bn.value().toString();
            default -> key.toString();
        };
    }
}
