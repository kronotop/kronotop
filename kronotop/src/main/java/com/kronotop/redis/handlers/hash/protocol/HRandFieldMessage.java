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

package com.kronotop.redis.handlers.hash.protocol;

import com.kronotop.KronotopException;
import com.kronotop.server.RESPError;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;

import java.util.List;

public class HRandFieldMessage implements ProtocolMessage<String> {
    public static final String COMMAND = "HRANDFIELD";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    public static final int MAXIMUM_PARAMETER_COUNT = 3;
    private final Request request;
    private String key;
    private Integer count;
    private boolean withValues;

    public HRandFieldMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        byte[] rawKey = new byte[request.getParams().get(0).readableBytes()];
        request.getParams().get(0).readBytes(rawKey);
        key = new String(rawKey);

        if (request.getParams().size() >= 2) {
            byte[] rawCount = new byte[request.getParams().get(1).readableBytes()];
            request.getParams().get(1).readBytes(rawCount);
            try {
                count = Integer.parseInt(new String(rawCount));
            } catch (NumberFormatException e) {
                throw new KronotopException(RESPError.NUMBER_FORMAT_EXCEPTION_MESSAGE_INTEGER);
            }

            if (request.getParams().size() == 3) {
                byte[] rawOperand = new byte[request.getParams().get(2).readableBytes()];
                request.getParams().get(2).readBytes(rawOperand);
                String operand = new String(rawOperand);
                if (operand.equalsIgnoreCase("WITHVALUES")) {
                    withValues = true;
                }
            }
        }
    }

    public Integer getCount() {
        return count;
    }

    public boolean getWithValues() {
        return withValues;
    }

    @Override
    public String getKey() {
        return key;
    }

    @Override
    public List<String> getKeys() {
        return null;
    }


}
