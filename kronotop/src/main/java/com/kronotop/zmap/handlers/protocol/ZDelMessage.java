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

package com.kronotop.zmap.handlers.protocol;

import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.List;

public class ZDelMessage implements ProtocolMessage<byte[]> {
    public static final String COMMAND = "ZDEL";
    public static final int MINIMUM_ARGUMENT_COUNT = 1;
    public static final int MAXIMUM_ARGUMENT_COUNT = 3;
    private final Request request;
    private String namespace;
    private byte[] key;

    public ZDelMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        key = ProtocolMessageUtil.readAsByteArray(request.getArguments().get(0));
        namespace = ProtocolMessageUtil.readTrailingNamespace(request.getArguments(), 1);
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public List<byte[]> getKeys() {
        return null;
    }

    /**
     * Returns the namespace given on the command, or null if not specified.
     */
    public String getNamespace() {
        return namespace;
    }
}
