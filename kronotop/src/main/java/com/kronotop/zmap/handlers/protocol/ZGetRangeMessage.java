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

public class ZGetRangeMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "ZGETRANGE";
    public static final int MINIMUM_PARAMETER_COUNT = 2;
    public static final int MAXIMUM_PARAMETER_COUNT = 9;

    public static final int DEFAULT_LIMIT = 100;
    public static final boolean DEFAULT_REVERSE = false;
    public static final RangeKeySelector DEFAULT_BEGIN_KEY_SELECTOR = RangeKeySelector.FIRST_GREATER_OR_EQUAL;
    public static final RangeKeySelector DEFAULT_END_KEY_SELECTOR = RangeKeySelector.FIRST_GREATER_THAN;

    private static final String POSITIVE_INTEGER = "a positive integer";
    private static final String VALID_KEY_SELECTOR = "a valid key selector";

    private final Request request;
    private byte[] begin;
    private byte[] end;
    private int limit = DEFAULT_LIMIT;
    private boolean reverse = DEFAULT_REVERSE;
    private RangeKeySelector beginKeySelector = DEFAULT_BEGIN_KEY_SELECTOR;
    private RangeKeySelector endKeySelector = DEFAULT_END_KEY_SELECTOR;

    public ZGetRangeMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        begin = ProtocolMessageUtil.readAsByteArray(request.getParams().get(0));
        end = ProtocolMessageUtil.readAsByteArray(request.getParams().get(1));

        long seen = 0;
        for (int i = 2; i < request.getParams().size(); i++) {
            String raw = ProtocolMessageUtil.readAsString(request.getParams().get(i));
            ZGetRangeArgumentKey argument = valueOfArgument(raw);
            seen = ProtocolMessageUtil.markArgumentSeen(seen, argument, argument.getValue());
            switch (argument) {
                case LIMIT -> {
                    ByteBuf value = requireValue(i, ZGetRangeArgumentKey.LIMIT, POSITIVE_INTEGER);
                    limit = ProtocolMessageUtil.readAsInteger(value);
                    if (limit <= 0) {
                        throw illegalValue(ZGetRangeArgumentKey.LIMIT, POSITIVE_INTEGER);
                    }
                    i++;
                }
                case REVERSE -> reverse = true;
                case BEGIN_KEY_SELECTOR -> {
                    ByteBuf value = requireValue(i, ZGetRangeArgumentKey.BEGIN_KEY_SELECTOR, VALID_KEY_SELECTOR);
                    beginKeySelector = valueOfRangeKeySelector(ProtocolMessageUtil.readAsString(value));
                    i++;
                }
                case END_KEY_SELECTOR -> {
                    ByteBuf value = requireValue(i, ZGetRangeArgumentKey.END_KEY_SELECTOR, VALID_KEY_SELECTOR);
                    endKeySelector = valueOfRangeKeySelector(ProtocolMessageUtil.readAsString(value));
                    i++;
                }
            }
        }
    }

    public byte[] getBegin() {
        return begin;
    }

    public byte[] getEnd() {
        return end;
    }

    public int getLimit() {
        return limit;
    }

    public boolean getReverse() {
        return reverse;
    }

    public RangeKeySelector getBeginKeySelector() {
        return beginKeySelector;
    }

    public RangeKeySelector getEndKeySelector() {
        return endKeySelector;
    }

    @Override
    public Void getKey() {
        return null;
    }

    @Override
    public List<Void> getKeys() {
        return null;
    }

    /**
     * Returns the value that follows a keyword at index {@code i}.
     *
     * @throws IllegalCommandArgumentException if the keyword is the last argument
     */
    private ByteBuf requireValue(int i, ZGetRangeArgumentKey key, String expected) {
        if (request.getParams().size() <= i + 1) {
            throw illegalValue(key, expected);
        }
        return request.getParams().get(i + 1);
    }

    private IllegalCommandArgumentException illegalValue(ZGetRangeArgumentKey key, String expected) {
        return new IllegalCommandArgumentException(
                String.format("%s argument must be followed by %s", key.getValue(), expected)
        );
    }

    private RangeKeySelector valueOfRangeKeySelector(String value) {
        try {
            return RangeKeySelector.valueOf(StringUtil.toUpperCaseAscii(value));
        } catch (IllegalArgumentException ignored) {
            throw new IllegalCommandArgumentException(String.format("Unknown key selector: '%s'", value));
        }
    }

    private ZGetRangeArgumentKey valueOfArgument(String raw) {
        String upper = StringUtil.toUpperCaseAscii(raw);
        for (ZGetRangeArgumentKey key : ZGetRangeArgumentKey.values()) {
            if (key.getValue().equals(upper)) {
                return key;
            }
        }
        throw new IllegalCommandArgumentException(String.format("Unknown '%s' argument", raw));
    }

    enum ZGetRangeArgumentKey {
        LIMIT("LIMIT"),
        REVERSE("REVERSE"),
        BEGIN_KEY_SELECTOR("BEGIN-KEY-SELECTOR"),
        END_KEY_SELECTOR("END-KEY-SELECTOR");

        private final String value;

        ZGetRangeArgumentKey(String value) {
            this.value = value;
        }

        String getValue() {
            return value;
        }
    }
}
