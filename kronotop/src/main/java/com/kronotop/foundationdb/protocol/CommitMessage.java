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

package com.kronotop.foundationdb.protocol;

import com.kronotop.server.KronotopMessage;
import com.kronotop.server.Request;
import com.kronotop.server.UnknownOperandException;

import java.util.List;

public class CommitMessage implements KronotopMessage<Void> {
    public static final String COMMAND = "COMMIT";
    public static final int MAXIMUM_PARAMETER_COUNT = 1;
    public static final String GET_COMMITTED_VERSION_OPERAND = "GET_COMMITTED_VERSION";
    public static final String GET_VERSIONSTAMP_OPERAND = "GET_VERSIONSTAMP";
    private final Request request;
    private String operand;

    public CommitMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        if (request.getParams().size() > 0) {
            byte[] rawOperand = new byte[request.getParams().get(0).readableBytes()];
            request.getParams().get(0).readBytes(rawOperand);
            operand = new String(rawOperand);
            if (GET_COMMITTED_VERSION_OPERAND.equalsIgnoreCase(operand)) {
                operand = GET_COMMITTED_VERSION_OPERAND;
            } else if (GET_VERSIONSTAMP_OPERAND.equalsIgnoreCase(operand)) {
                operand = GET_VERSIONSTAMP_OPERAND;
            } else {
                throw new UnknownOperandException(String.format("unknown operand: %s", operand));
            }
        }
    }

    public String getOperand() {
        return operand;
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