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

package com.kronotop.foundationdb.protocol;

import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;
import com.kronotop.server.UnknownSubcommandException;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CommitMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "COMMIT";
    public static final int MAXIMUM_PARAMETER_COUNT = 2;
    private static final String RETURNING_ARGUMENT = "RETURNING";
    private final Request request;
    private final Set<Parameter> returning = new HashSet<>();

    public CommitMessage(Request request) {
        this.request = request;
        parse();
    }

    private void parse() {
        if (!request.getParams().isEmpty()) {
            byte[] rawReturning = new byte[request.getParams().getFirst().readableBytes()];
            request.getParams().getFirst().readBytes(rawReturning);

            String returning = new String(rawReturning);
            if (!returning.equalsIgnoreCase(RETURNING_ARGUMENT)) {
                throw new UnknownSubcommandException(returning);
            }

            if (request.getParams().size() - 1 <= 0) {
                throw new IllegalArgumentException("No arguments given for " + returning);
            }

            for (int i = 1; i < request.getParams().size(); i++) {
                byte[] rawParameter = new byte[request.getParams().get(i).readableBytes()];
                request.getParams().get(i).readBytes(rawParameter);
                String parameter = new String(rawParameter);
                try {
                    this.returning.add(Parameter.fromValue(parameter));
                } catch (IllegalArgumentException e) {
                    throw new IllegalArgumentException("Illegal argument: " + parameter);
                }
            }
        }
    }

    public Set<Parameter> getReturning() {
        return returning;
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
        VERSIONSTAMP("versionstamp"),
        FUTURES("futures");

        private final String value;

        Parameter(String value) {
            this.value = value;
        }

        public static Parameter fromValue(String value) {
            for (Parameter parameter : values()) {
                if (parameter.getValue().equalsIgnoreCase(value)) {
                    return parameter;
                }
            }
            throw new IllegalArgumentException(value);
        }

        public String getValue() {
            return value;
        }
    }
}