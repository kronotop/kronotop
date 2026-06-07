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

package com.kronotop.core.handlers.transaction.protocol;

import com.kronotop.server.IllegalCommandArgumentException;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;
import com.kronotop.server.UnknownSubcommandException;
import io.netty.buffer.ByteBuf;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class CommitMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "COMMIT";
    public static final int MAXIMUM_PARAMETER_COUNT = 2;
    private static final String RETURNING_ARGUMENT = "RETURNING";
    private final Request request;
    private Parameter returning;

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

            int paramCount = request.getParams().size() - 1;
            if (paramCount == 0) {
                throw new IllegalArgumentException("No arguments given for " + returning);
            }
            if (paramCount > 1) {
                throw new IllegalArgumentException("RETURNING accepts only one parameter");
            }

            this.returning = Parameter.fromByteBuf(request.getParams().get(1));
        }
    }

    public Parameter getReturning() {
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
        VERSIONSTAMP("versionstamp");

        private final String value;
        private final byte[] bytes;

        Parameter(String value) {
            this.value = value;
            this.bytes = value.getBytes(StandardCharsets.US_ASCII);
        }

        private static byte toLower(byte b) {
            return (b >= 'A' && b <= 'Z') ? (byte) (b + 32) : b;
        }

        private static boolean matches(ByteBuf buf, byte[] expected) {
            int readerIndex = buf.readerIndex();
            for (int i = 0; i < expected.length; i++) {
                if (toLower(buf.getByte(readerIndex + i)) != expected[i]) {
                    return false;
                }
            }
            return true;
        }

        public static Parameter fromByteBuf(ByteBuf buf) {
            return switch (buf.readableBytes()) {
                case 12 -> {
                    if (matches(buf, VERSIONSTAMP.bytes)) yield VERSIONSTAMP;
                    throw illegalArgument(buf);
                }
                case 17 -> {
                    if (matches(buf, COMMITTED_VERSION.bytes)) yield COMMITTED_VERSION;
                    throw illegalArgument(buf);
                }
                default -> throw illegalArgument(buf);
            };
        }

        private static IllegalCommandArgumentException illegalArgument(ByteBuf buf) {
            return new IllegalCommandArgumentException("Illegal argument: " + buf.toString(StandardCharsets.US_ASCII));
        }

        public String getValue() {
            return value;
        }
    }
}
