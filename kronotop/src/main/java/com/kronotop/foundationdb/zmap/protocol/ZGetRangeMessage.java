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

import com.kronotop.KronotopException;
import com.kronotop.server.RESPError;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.Locale;

public class ZGetRangeMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "ZGETRANGE";
    public static final int MINIMUM_PARAMETER_COUNT = 2;
    public static final int MAXIMUM_PARAMETER_COUNT = 8;
    public static final int DEFAULT_LIMIT = 25;
    public static final boolean DEFAULT_REVERSE = false;
    public static final String LIMIT_KEYWORD = "LIMIT";
    public static final String REVERSE_KEYWORD = "REVERSE";
    public static final String BEGIN_KEY_SELECTOR_KEYWORD = "BEGIN_KEY_SELECTOR";
    public static final String END_KEY_SELECTOR_KEYWORD = "END_KEY_SELECTOR";
    public static final RangeKeySelector DEFAULT_BEGIN_KEY_SELECTOR = RangeKeySelector.FIRST_GREATER_OR_EQUAL;
    public static final RangeKeySelector DEFAULT_END_KEY_SELECTOR = RangeKeySelector.FIRST_GREATER_THAN;

    public static final byte[] ASTERISK = new byte[]{0x2A};
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

    private String readStringFromByteBuf(ByteBuf buf) {
        byte[] rawItem = new byte[buf.readableBytes()];
        buf.readBytes(rawItem);
        return new String(rawItem);
    }

    private void parse() {
        begin = new byte[request.getParams().get(0).readableBytes()];
        request.getParams().get(0).readBytes(begin);

        end = new byte[request.getParams().get(1).readableBytes()];
        request.getParams().get(1).readBytes(end);

        if (request.getParams().size() >= 2) {
            for (int i = 2; i < request.getParams().size(); i++) {
                String keyword = readStringFromByteBuf(request.getParams().get(i));
                if (keyword.equalsIgnoreCase(LIMIT_KEYWORD)) {
                    String limitStr = readStringFromByteBuf(request.getParams().get(i + 1));
                    try {
                        limit = Integer.parseInt(limitStr);
                    } catch (NumberFormatException e) {
                        throw new KronotopException(RESPError.NUMBER_FORMAT_EXCEPTION_MESSAGE_INTEGER);
                    }
                    i++;
                } else if (keyword.equalsIgnoreCase(REVERSE_KEYWORD)) {
                    reverse = true;
                } else if (keyword.equalsIgnoreCase(BEGIN_KEY_SELECTOR_KEYWORD)) {
                    String enumVal = readStringFromByteBuf(request.getParams().get(i + 1));
                    beginKeySelector = RangeKeySelector.valueOf(enumVal.toUpperCase(Locale.ROOT));
                    i++;
                } else if (keyword.equalsIgnoreCase(END_KEY_SELECTOR_KEYWORD)) {
                    String enumVal = readStringFromByteBuf(request.getParams().get(i + 1));
                    endKeySelector = RangeKeySelector.valueOf(enumVal.toUpperCase(Locale.ROOT));
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
}