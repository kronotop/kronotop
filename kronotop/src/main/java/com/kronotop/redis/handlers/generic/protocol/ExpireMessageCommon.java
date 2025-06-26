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

package com.kronotop.redis.handlers.generic.protocol;

import com.kronotop.KronotopException;
import com.kronotop.internal.ByteBufUtils;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.List;

public class ExpireMessageCommon implements ProtocolMessage<String> {
    public static final int MINIMUM_PARAMETER_COUNT = 2;
    public static final int MAXIMUM_PARAMETER_COUNT = 3;
    private final Request request;
    private String key;
    private long numberValue;
    private Option option;

    public ExpireMessageCommon(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        key = ByteBufUtils.readAsString(request.getParams().getFirst());
        numberValue = ByteBufUtils.readAsLong(request.getParams().get(1));
        // TODO:
        if (numberValue <= 0) {
            throw new KronotopException("invalid expire time in 'expire' command");
        }
        if (request.getParams().size() == 3) {
            String stringOption = ByteBufUtils.readAsString(request.getParams().get(2)).toUpperCase();
            try {
                option = Option.valueOf(stringOption);
            } catch (IllegalArgumentException e) {
                throw new KronotopException("Unsupported option " + stringOption);
            }
        }
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public List<String> getKeys() {
        return null;
    }

    public long getNumberValue() {
        return numberValue;
    }

    public Option getOption() {
        return option;
    }

    /**
     * Enum Option defines the conditions under which the expiry time of a key can be set.
     * This is used in commands where conditional updates to the expiry time are required.
     * <p>
     * - NX: Sets the expiry only if the key does not already have an expiry time.
     * - XX: Sets the expiry only if the key already has an existing expiry time.
     * - GT: Sets the expiry only if the new expiry time is greater than the current expiry time.
     * - LT: Sets the expiry only if the new expiry time is less than the current expiry time.
     */
    public enum Option {
        // NX -- Set expiry only when the key has no expiry
        NX,

        // XX -- Set expiry only when the key has an existing expiry
        XX,

        // GT -- Set expiry only when the new expiry is greater than current one
        GT,

        // LT -- Set expiry only when the new expiry is less than current one
        LT
    }
}
