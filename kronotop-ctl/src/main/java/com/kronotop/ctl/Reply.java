/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kronotop.ctl;

import com.kronotop.resp.RespValue;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

// A map reply of a server command. Every accessor fails with the command name in the message.
record Reply(String command, Map<RespValue, RespValue> fields) {

    static Reply of(String command, RespValue value) throws IOException {
        if (value instanceof RespValue.RespMap(Map<RespValue, RespValue> fields)) {
            return new Reply(command, fields);
        }
        throw unexpected(command, value);
    }

    static IOException unexpected(String command, Object detail) {
        return new IOException("Unexpected " + command + " response: " + detail);
    }

    static String text(String command, RespValue value) throws IOException {
        if (value instanceof RespValue.BlobString(String text)) {
            return text;
        }
        throw unexpected(command, value);
    }

    static List<String> texts(String command, RespValue value) throws IOException {
        if (!(value instanceof RespValue.Array(List<RespValue> values))) {
            throw unexpected(command, value);
        }
        List<String> texts = new ArrayList<>();
        for (RespValue item : values) {
            texts.add(text(command, item));
        }
        return texts;
    }

    private RespValue get(String name) throws IOException {
        RespValue value = fields.get(new RespValue.BlobString(name));
        if (value == null) {
            throw unexpected(command, "missing '" + name + "'");
        }
        return value;
    }

    String text(String name) throws IOException {
        return text(command, get(name));
    }

    long number(String name) throws IOException {
        if (get(name) instanceof RespValue.Number(long value)) {
            return value;
        }
        throw unexpected(command, get(name));
    }

    Reply reply(String name) throws IOException {
        return of(command, get(name));
    }

    List<String> texts(String name) throws IOException {
        return texts(command, get(name));
    }
}
