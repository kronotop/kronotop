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
import com.kronotop.internal.StringUtil;
import com.kronotop.server.IllegalCommandArgumentException;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;
import io.netty.buffer.ByteBuf;

import java.util.List;

public class ZSetMessage implements ProtocolMessage<byte[]> {
    public static final String COMMAND = "ZSET";
    private static final String NAMESPACE = "NAMESPACE";
    public static final int MINIMUM_PARAMETER_COUNT = 2;
    public static final int MAXIMUM_PARAMETER_COUNT = 4;
    private final Request request;
    private byte[] key;
    private byte[] value;
    private String namespace;

    public ZSetMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        key = ProtocolMessageUtil.readAsByteArray(request.getParams().get(0));
        value = ProtocolMessageUtil.readAsByteArray(request.getParams().get(1));
        for (int i = 2; i < request.getParams().size(); i++) {
            String raw = ProtocolMessageUtil.readAsString(request.getParams().get(i));
            if (!StringUtil.toUpperCaseAscii(raw).equals(NAMESPACE)) {
                throw new IllegalCommandArgumentException(String.format("Unknown '%s' argument", raw));
            }
            ByteBuf value = ProtocolMessageUtil.requireValue(request.getParams(), i, NAMESPACE, "a namespace");
            namespace = ProtocolMessageUtil.readAsString(value);
            i++;
        }
    }

    @Override
    public byte[] getKey() {
        return key;
    }

    @Override
    public List<byte[]> getKeys() {
        return null;
    }

    public byte[] getValue() {
        return value;
    }

    /**
     * Returns the namespace given on the command, or null if not specified.
     */
    public String getNamespace() {
        return namespace;
    }
}