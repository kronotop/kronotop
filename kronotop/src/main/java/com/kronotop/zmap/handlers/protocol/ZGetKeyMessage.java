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

public class ZGetKeyMessage implements ProtocolMessage<byte[]> {
    public static final String COMMAND = "ZGETKEY";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    public static final int MAXIMUM_PARAMETER_COUNT = 3;
    public static final RangeKeySelector DEFAULT_KEY_SELECTOR = RangeKeySelector.FIRST_GREATER_OR_EQUAL;

    private final Request request;
    private byte[] key;
    private RangeKeySelector keySelector = DEFAULT_KEY_SELECTOR;

    public ZGetKeyMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        key = ProtocolMessageUtil.readAsByteArray(request.getParams().getFirst());

        for (int i = 1; i < request.getParams().size(); i++) {
            String raw = ProtocolMessageUtil.readAsString(request.getParams().get(i));
            ZGetKeyArgumentKey argument = valueOfArgument(raw);

            if (argument.equals(ZGetKeyArgumentKey.KEY_SELECTOR)) {
                ByteBuf value = ProtocolMessageUtil.requireValue(
                        request.getParams(), i, argument.getValue(), ProtocolMessageUtil.VALID_KEY_SELECTOR);
                keySelector = RangeKeySelector.getValue(ProtocolMessageUtil.readAsString(value));
                i++;
            }
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

    public RangeKeySelector getKeySelector() {
        return keySelector;
    }

    private ZGetKeyArgumentKey valueOfArgument(String raw) {
        String upper = StringUtil.toUpperCaseAscii(raw);
        for (ZGetKeyArgumentKey key : ZGetKeyArgumentKey.values()) {
            if (key.getValue().equals(upper)) {
                return key;
            }
        }
        throw new IllegalCommandArgumentException(String.format("Unknown '%s' argument", raw));
    }

    enum ZGetKeyArgumentKey {
        KEY_SELECTOR("KEY-SELECTOR");

        private final String value;

        ZGetKeyArgumentKey(String value) {
            this.value = value;
        }

        String getValue() {
            return value;
        }
    }
}