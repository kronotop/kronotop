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

package com.kronotop.foundationdb.zmap.protocol;

import com.kronotop.server.KronotopMessage;
import com.kronotop.server.Request;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.Locale;

public class ZGetKeyMessage implements KronotopMessage<byte[]> {
    public static final String COMMAND = "ZGETKEY";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    public static final int MAXIMUM_PARAMETER_COUNT = 3;
    public static final String KEY_SELECTOR_KEYWORD = "KEY_SELECTOR";
    public static final RangeKeySelector DEFAULT_KEY_SELECTOR = RangeKeySelector.FIRST_GREATER_OR_EQUAL;

    private final Request request;
    private byte[] key;
    private RangeKeySelector keySelector = DEFAULT_KEY_SELECTOR;

    public ZGetKeyMessage(Request request) {
        this.request = request;
        parse();
    }

    private String readStringFromByteBuf(ByteBuf buf) {
        byte[] rawItem = new byte[buf.readableBytes()];
        buf.readBytes(rawItem);
        return new String(rawItem);
    }

    private void parse() {
        key = new byte[request.getParams().getFirst().readableBytes()];
        request.getParams().getFirst().readBytes(key);

        if (request.getParams().size() > 1) {
            for (int i = 1; i < request.getParams().size(); i++) {
                String keyword = readStringFromByteBuf(request.getParams().get(i));
                if (keyword.equalsIgnoreCase(KEY_SELECTOR_KEYWORD)) {
                    String enumVal = readStringFromByteBuf(request.getParams().get(i + 1));
                    keySelector = RangeKeySelector.valueOf(enumVal.toUpperCase(Locale.ROOT));
                    i++;
                }
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
}