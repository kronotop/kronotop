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

package com.kronotop.core.handlers.session.protocol;

import com.kronotop.KronotopException;
import com.kronotop.cluster.handlers.InvalidNumberOfParametersException;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.InputType;
import com.kronotop.server.ObjectIdFormat;
import com.kronotop.server.ReplyType;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

public class SessionAttributeParameters {
    private final SessionAttributeSubcommand subcommand;
    private SessionAttribute attribute;
    private ReplyType replyType;
    private InputType inputType;
    private int bucketBatchSize;
    private ObjectIdFormat objectIdFormat;

    public SessionAttributeParameters(ArrayList<ByteBuf> params) {
        String rawSubcommand = ProtocolMessageUtil.readAsString(params.getFirst());
        try {
            subcommand = SessionAttributeSubcommand.valueOf(rawSubcommand.toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new KronotopException("Invalid subcommand status: " + rawSubcommand);
        }

        if (subcommand.equals(SessionAttributeSubcommand.LIST)) {
            return;
        }

        if (subcommand.equals(SessionAttributeSubcommand.SET)) {
            if (params.size() != 3) {
                throw new InvalidNumberOfParametersException();
            }
        }

        String rawSessionAttribute = ProtocolMessageUtil.readAsString(params.get(1));
        attribute = SessionAttribute.findByValue(rawSessionAttribute);

        switch (attribute) {
            case INPUT_TYPE -> {
                String rawInputType = ProtocolMessageUtil.readAsString(params.get(2));
                try {
                    inputType = InputType.valueOf(rawInputType.toUpperCase());
                } catch (IllegalArgumentException e) {
                    throw new KronotopException("Invalid input type: " + rawInputType);
                }
            }
            case REPLY_TYPE -> {
                String rawReplyType = ProtocolMessageUtil.readAsString(params.get(2));
                try {
                    replyType = ReplyType.valueOf(rawReplyType.toUpperCase());
                } catch (IllegalArgumentException e) {
                    throw new KronotopException("Invalid reply type: " + rawReplyType);
                }
            }
            case LIMIT -> {
                bucketBatchSize = ProtocolMessageUtil.readAsInteger(params.get(2));
            }
            case OBJECT_ID_FORMAT -> {
                String rawVersionstampFormat = ProtocolMessageUtil.readAsString(params.get(2));
                try {
                    objectIdFormat = ObjectIdFormat.valueOf(rawVersionstampFormat.toUpperCase());
                } catch (IllegalArgumentException e) {
                    throw new KronotopException("Invalid versionstamp format: " + rawVersionstampFormat);
                }
            }
            default -> throw new KronotopException("Unknown session attribute: " + rawSessionAttribute);
        }
    }

    public SessionAttributeSubcommand getSubcommand() {
        return subcommand;
    }

    public SessionAttribute getAttribute() {
        return attribute;
    }

    public ReplyType replyType() {
        return replyType;
    }

    public InputType inputType() {
        return inputType;
    }

    public int bucketBatchSize() {
        return bucketBatchSize;
    }

    public ObjectIdFormat objectIdFormat() {
        return objectIdFormat;
    }

    public enum SessionAttributeSubcommand {
        SET,
        LIST
    }

    public enum SessionAttribute {
        REPLY_TYPE("reply_type"),
        INPUT_TYPE("input_type"),
        LIMIT("limit"),
        OBJECT_ID_FORMAT("object_id_format");

        final String value;

        SessionAttribute(String value) {
            this.value = value;
        }

        public static SessionAttribute findByValue(String v) {
            if (v.toLowerCase().equals(REPLY_TYPE.getValue())) {
                return REPLY_TYPE;
            } else if (v.toLowerCase().equals(INPUT_TYPE.getValue())) {
                return INPUT_TYPE;
            } else if (v.toLowerCase().equals(LIMIT.getValue())) {
                return LIMIT;
            } else if (v.toLowerCase().endsWith(OBJECT_ID_FORMAT.getValue())) {
                return OBJECT_ID_FORMAT;
            } else {
                throw new IllegalArgumentException(
                        String.format("Invalid session attribute: '%s'", v)
                );
            }
        }

        public String getValue() {
            return value;
        }
    }
}
