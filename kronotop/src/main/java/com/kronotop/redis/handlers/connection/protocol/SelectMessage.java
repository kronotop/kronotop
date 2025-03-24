/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.redis.handlers.connection.protocol;

import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.List;

public class SelectMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "SELECT";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    public static final int MAXIMUM_PARAMETER_COUNT = 1;
    private final Request request;
    private String index;

    public SelectMessage(Request request) {
        this.request = request;
        parse();
    }

    private boolean isNumeric(final CharSequence cs) {
        // Source: https://commons.apache.org/proper/commons-lang/javadocs/api-release/src-html/org/apache/commons/lang3/StringUtils.html#line.3752
        if (cs == null || cs.isEmpty()) {
            return false;
        }
        final int sz = cs.length();
        for (int i = 0; i < sz; i++) {
            if (!Character.isDigit(cs.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    private void parse() {
        byte[] indexBytes = new byte[request.getParams().getFirst().readableBytes()];
        request.getParams().getFirst().readBytes(indexBytes);
        index = new String(indexBytes);
        if (!isNumeric(index)) {
            throw new IllegalArgumentException(String.format("index has to be a zero-based numeric: '%s'", index));
        }
    }

    public String getIndex() {
        return index;
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