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
 * Formats RESP values as CSV output matching redis-cli behavior.
 */
public class CsvResponseFormatter {

    public String format(RespValue value) {
        List<String> fields = new ArrayList<>();
        collectFields(value, fields);
        return String.join(",", fields);
    }

    private void collectFields(RespValue value, List<String> fields) {
        switch (value) {
            case RespValue.SimpleString ss -> fields.add(quoteString(ss.value()));
            case RespValue.BlobString bs -> fields.add(quoteString(bs.value()));
            case RespValue.SimpleError se -> fields.add(quoteString(se.code() + " " + se.message()));
            case RespValue.BlobError be -> fields.add(quoteString(be.code() + " " + be.message()));
            case RespValue.Number n -> fields.add(String.valueOf(n.value()));
            case RespValue.Double d -> fields.add(String.valueOf(d.value()));
            case RespValue.Boolean b -> fields.add(quoteString(b.value() ? "true" : "false"));
            case RespValue.Null ignored -> fields.add("");
            case RespValue.BigNumber bn -> fields.add(bn.value().toString());
            case RespValue.VerbatimString vs -> fields.add(quoteString(vs.value()));
            case RespValue.Array arr -> {
                if (arr.values().isEmpty()) {
                    fields.add("");
                } else {
                    for (RespValue v : arr.values()) {
                        collectFields(v, fields);
                    }
                }
            }
            case RespValue.RespMap m -> {
                for (var entry : m.values().entrySet()) {
                    collectFields(entry.getKey(), fields);
                    collectFields(entry.getValue(), fields);
                }
            }
            case RespValue.RespSet s -> {
                if (s.values().isEmpty()) {
                    fields.add("");
                } else {
                    for (RespValue v : s.values()) {
                        collectFields(v, fields);
                    }
                }
            }
            case RespValue.Attribute attr -> collectFields(attr.value(), fields);
            case RespValue.Push p -> {
                fields.add(quoteString(p.kind()));
                for (RespValue v : p.values()) {
                    collectFields(v, fields);
                }
            }
        }
    }

    private String quoteString(String value) {
        return "\"" + value.replace("\\", "\\\\").replace("\"", "\\\"") + "\"";
    }
}
