/*
 * Copyright (c) 2023 Kronotop
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

package com.kronotop.sql.protocol;

import com.kronotop.server.KronotopMessage;
import com.kronotop.server.Request;
import io.netty.buffer.ByteBuf;

import java.util.LinkedList;
import java.util.List;

public class SqlMessage implements KronotopMessage<Void> {
    public static final String COMMAND = "SQL";
    private final Request request;
    private final List<String> queries = new LinkedList<>();

    public SqlMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        for (ByteBuf buf : request.getParams()) {
            byte[] rawQuery = new byte[buf.readableBytes()];
            buf.readBytes(rawQuery);
            queries.add(new String(rawQuery));
        }
    }

    public List<String> getQueries() {
        return queries;
    }

    @Override
    public Void getKey() {
        return null;
    }

    @Override
    public List<Void> getKeys() {
        return null;
    }
}
