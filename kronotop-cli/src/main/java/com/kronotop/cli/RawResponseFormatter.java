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
 * Formats RESP values as raw output matching redis-cli behavior.
 */
public class RawResponseFormatter {

    private final String delimiter;

    public RawResponseFormatter() {
        this("\n");
    }

    public RawResponseFormatter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String format(RespValue value) {
        List<String> lines = new ArrayList<>();
        formatValue(value, lines);
        return String.join(delimiter, lines);
    }

    private void formatValue(RespValue value, List<String> lines) {
        switch (value) {
            case RespValue.SimpleString ss -> lines.add(ss.value());
            case RespValue.BlobString bs -> lines.add(bs.value());
            case RespValue.SimpleError se -> lines.add(se.code() + " " + se.message());
            case RespValue.BlobError be -> lines.add(be.code() + " " + be.message());
            case RespValue.Number n -> lines.add(String.valueOf(n.value()));
            case RespValue.Double d -> lines.add(String.valueOf(d.value()));
            case RespValue.Boolean b -> lines.add(b.value() ? "true" : "false");
            case RespValue.Null ignored -> lines.add("");
            case RespValue.BigNumber bn -> lines.add(bn.value().toString());
            case RespValue.VerbatimString vs -> lines.add(vs.value());
            case RespValue.Array arr -> {
                for (RespValue v : arr.values()) {
                    formatValue(v, lines);
                }
            }
            case RespValue.RespMap m -> formatMap(m, lines);
            case RespValue.RespSet s -> {
                for (RespValue v : s.values()) {
                    formatValue(v, lines);
                }
            }
            case RespValue.Attribute attr -> formatValue(attr.value(), lines);
            case RespValue.Push p -> {
                lines.add(p.kind());
                for (RespValue v : p.values()) {
                    formatValue(v, lines);
                }
            }
        }
    }

    private void formatMap(RespValue.RespMap map, List<String> lines) {
        for (var entry : map.values().entrySet()) {
            String key = getSimpleValue(entry.getKey());
            RespValue val = entry.getValue();

            if (isSimpleValue(val)) {
                // Key and simple value on same line
                String simpleVal = getSimpleValue(val);
                if (simpleVal.isEmpty()) {
                    lines.add(key);
                } else {
                    lines.add(key + " " + simpleVal);
                }
            } else {
                // Key with complex value: key on line, then nested content
                List<String> nestedLines = new ArrayList<>();
                formatValue(val, nestedLines);
                if (nestedLines.isEmpty()) {
                    lines.add(key);
                } else {
                    // First nested line joins with key
                    lines.add(key + " " + nestedLines.get(0));
                    // Rest of nested lines added separately
                    for (int i = 1; i < nestedLines.size(); i++) {
                        lines.add(nestedLines.get(i));
                    }
                }
            }
        }
    }

    private boolean isSimpleValue(RespValue value) {
        return switch (value) {
            case RespValue.SimpleString ignored -> true;
            case RespValue.BlobString ignored -> true;
            case RespValue.Number ignored -> true;
            case RespValue.Double ignored -> true;
            case RespValue.Boolean ignored -> true;
            case RespValue.BigNumber ignored -> true;
            case RespValue.VerbatimString ignored -> true;
            case RespValue.Null ignored -> true;
            case RespValue.Array arr -> arr.values().isEmpty();
            case RespValue.RespSet s -> s.values().isEmpty();
            case RespValue.RespMap m -> m.values().isEmpty();
            default -> false;
        };
    }

    private String getSimpleValue(RespValue value) {
        return switch (value) {
            case RespValue.SimpleString ss -> ss.value();
            case RespValue.BlobString bs -> bs.value();
            case RespValue.Number n -> String.valueOf(n.value());
            case RespValue.Double d -> String.valueOf(d.value());
            case RespValue.Boolean b -> b.value() ? "true" : "false";
            case RespValue.BigNumber bn -> bn.value().toString();
            case RespValue.VerbatimString vs -> vs.value();
            case RespValue.Null ignored -> "";
            default -> "";
        };
    }
}
