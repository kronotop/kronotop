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
import com.kronotop.server.UnknownSubcommandException;

import java.util.List;

public class CommitMessage implements KronotopMessage<Void> {
    public static final String COMMAND = "COMMIT";
    public static final int MAXIMUM_PARAMETER_COUNT = 1;
    private final Request request;
    private Parameter parameter;

    public CommitMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        if (!request.getParams().isEmpty()) {
            byte[] rawParameter = new byte[request.getParams().get(0).readableBytes()];
            request.getParams().get(0).readBytes(rawParameter);

            String value = new String(rawParameter);
            try {
                this.parameter = Parameter.fromValue(value);
            } catch (IllegalArgumentException e) {
                throw new UnknownSubcommandException(value);
            }
        }
    }

    public Parameter getParameter() {
        return parameter;
    }

    @Override
    public Void getKey() {
        return null;
    }

    @Override
    public List<Void> getKeys() {
        return null;
    }

    public enum Parameter {
        COMMITTED_VERSION("committed-version"),
        VERSIONSTAMP("versionstamp");

        private final String value;

        Parameter(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }

        public static Parameter fromValue(String value) {
            for (Parameter parameter : values()) {
                if (parameter.getValue().equalsIgnoreCase(value)) {
                    return parameter;
                }
            }
            throw new IllegalArgumentException(value);
        }
    }
}