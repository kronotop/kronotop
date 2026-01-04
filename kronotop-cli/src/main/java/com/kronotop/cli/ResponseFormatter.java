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

import java.util.ArrayList;
import java.util.List;

/**
 * Formats RESP values for CLI output in redis-cli compatible style.
 */
public class ResponseFormatter {

    private final boolean jsonReplyType;

    public ResponseFormatter(boolean jsonReplyType) {
        this.jsonReplyType = jsonReplyType;
    }

    public String format(RespValue value) {
        return format(value, "");
    }

    private String format(RespValue value, String indent) {
        return switch (value) {
            case RespValue.SimpleString ss -> ss.value();
            case RespValue.BlobString bs -> formatBlobString(bs.value());
            case RespValue.SimpleError se -> "(error) " + se.code() + " " + se.message();
            case RespValue.BlobError be -> "(error) " + be.code() + " " + be.message();
            case RespValue.Number n -> "(integer) " + n.value();
            case RespValue.Double d -> "(double) " + d.value();
            case RespValue.Boolean b -> b.value() ? "(true)" : "(false)";
            case RespValue.Null ignored -> "(nil)";
            case RespValue.BigNumber bn -> "(integer) " + bn.value();
            case RespValue.VerbatimString vs -> vs.value();
            case RespValue.Array arr -> formatArray(arr, indent);
            case RespValue.RespMap m -> formatMap(m, indent);
            case RespValue.RespSet s -> formatSet(s, indent);
            case RespValue.Attribute attr -> format(attr.value(), indent);
            case RespValue.Push p -> formatPush(p, indent);
        };
    }

    private String formatBlobString(String s) {
        if (s.contains("\n")) {
            if (s.endsWith("\n")) {
                return s.substring(0, s.length() - 1);
            }
            return s;
        }
        return "\"" + formatString(s) + "\"";
    }

    private String formatString(String s) {
        if (!jsonReplyType) {
            return s;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\' -> sb.append("\\\\");
                case '"' -> sb.append("\\\"");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 32 || c == 127) {
                        sb.append(String.format("\\x%02x", (int) c));
                    } else {
                        sb.append(c);
                    }
                }
            }
        }
        return sb.toString();
    }

    private String formatArray(RespValue.Array array, String indent) {
        if (array.values().isEmpty()) {
            return "(empty array)";
        }
        StringBuilder sb = new StringBuilder();
        int size = array.values().size();
        int maxIndexWidth = String.valueOf(size).length();

        for (int i = 0; i < size; i++) {
            int index = i + 1;
            String indexStr = String.valueOf(index);
            String padding = " ".repeat(maxIndexWidth - indexStr.length());

            if (i > 0) {
                sb.append("\n").append(indent);
            }

            RespValue v = array.values().get(i);
            String prefix = padding + indexStr + ") ";

            if (v instanceof RespValue.Array || v instanceof RespValue.RespMap || v instanceof RespValue.RespSet) {
                String nestedIndent = indent + " ".repeat(prefix.length());
                sb.append(prefix).append(format(v, nestedIndent));
            } else {
                sb.append(prefix).append(format(v, indent));
            }
        }
        return sb.toString();
    }

    private String formatMap(RespValue.RespMap map, String indent) {
        if (map.values().isEmpty()) {
            return "(empty map)";
        }
        StringBuilder sb = new StringBuilder();
        int size = map.values().size();
        int maxIndexWidth = String.valueOf(size).length();
        int i = 0;

        for (var entry : map.values().entrySet()) {
            int index = ++i;
            String indexStr = String.valueOf(index);
            String padding = " ".repeat(maxIndexWidth - indexStr.length());

            if (i > 1) {
                sb.append("\n").append(indent);
            }

            String prefix = padding + indexStr + "# ";
            String keyStr = format(entry.getKey(), indent);
            RespValue value = entry.getValue();

            if (isComplexValue(value)) {
                sb.append(prefix).append(keyStr).append(" =>");
                String nestedIndent = indent + "   ";
                sb.append("\n").append(nestedIndent).append(format(value, nestedIndent));
            } else {
                sb.append(prefix).append(keyStr).append(" => ").append(format(value, indent));
            }
        }
        return sb.toString();
    }

    private boolean isComplexValue(RespValue value) {
        if (value instanceof RespValue.RespMap m) {
            return !m.values().isEmpty();
        }
        if (value instanceof RespValue.Array arr) {
            if (arr.values().isEmpty()) {
                return false;
            }
            if (arr.values().size() == 1) {
                return isComplexValue(arr.values().getFirst());
            }
            return true;
        }
        if (value instanceof RespValue.RespSet s) {
            if (s.values().isEmpty()) {
                return false;
            }
            if (s.values().size() == 1) {
                return isComplexValue(s.values().iterator().next());
            }
            return true;
        }
        return false;
    }

    private String formatSet(RespValue.RespSet set, String indent) {
        if (set.values().isEmpty()) {
            return "(empty set)";
        }
        StringBuilder sb = new StringBuilder();
        List<RespValue> values = new ArrayList<>(set.values());
        int size = values.size();
        int maxIndexWidth = String.valueOf(size).length();

        for (int i = 0; i < size; i++) {
            int index = i + 1;
            String indexStr = String.valueOf(index);
            String padding = " ".repeat(maxIndexWidth - indexStr.length());

            if (i > 0) {
                sb.append("\n").append(indent);
            }

            RespValue v = values.get(i);
            String prefix = padding + indexStr + ") ";

            if (v instanceof RespValue.Array || v instanceof RespValue.RespMap || v instanceof RespValue.RespSet) {
                String nestedIndent = indent + " ".repeat(prefix.length());
                sb.append(prefix).append(format(v, nestedIndent));
            } else {
                sb.append(prefix).append(format(v, indent));
            }
        }
        return sb.toString();
    }

    private String formatPush(RespValue.Push push, String indent) {
        StringBuilder sb = new StringBuilder();
        sb.append("push: ").append(push.kind());
        List<RespValue> values = push.values();
        int size = values.size();
        int maxIndexWidth = String.valueOf(size).length();

        for (int i = 0; i < size; i++) {
            int index = i + 1;
            String indexStr = String.valueOf(index);
            String padding = " ".repeat(maxIndexWidth - indexStr.length());

            sb.append("\n").append(indent);

            RespValue v = values.get(i);
            String prefix = padding + indexStr + ") ";

            if (v instanceof RespValue.Array || v instanceof RespValue.RespMap || v instanceof RespValue.RespSet) {
                String nestedIndent = indent + " ".repeat(prefix.length());
                sb.append(prefix).append(format(v, nestedIndent));
            } else {
                sb.append(prefix).append(format(v, indent));
            }
        }
        return sb.toString();
    }
}
