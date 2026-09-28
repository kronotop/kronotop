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

import com.kronotop.cluster.handlers.InvalidNumberOfArgumentsException;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.internal.StringUtil;
import com.kronotop.server.IllegalCommandArgumentException;
import com.kronotop.server.InputType;
import com.kronotop.server.ObjectIdFormat;
import com.kronotop.server.ReplyType;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;

public class SessionAttributeArguments {
    private final SessionAttributeSubcommand subcommand;
    private SessionAttribute attribute;
    private ReplyType replyType;
    private InputType inputType;
    private int bucketBatchSize;
    private ObjectIdFormat objectIdFormat;

    public SessionAttributeArguments(ArrayList<ByteBuf> args) {
        subcommand = ProtocolMessageUtil.readEnum(SessionAttributeSubcommand.class, args.getFirst(), "subcommand");

        if (subcommand.equals(SessionAttributeSubcommand.LIST)) {
            if (args.size() != 1) {
                throw new InvalidNumberOfArgumentsException();
            }
            return;
        }

        if (args.size() != 3) {
            throw new InvalidNumberOfArgumentsException();
        }

        String rawSessionAttribute = ProtocolMessageUtil.readAsString(args.get(1));
        attribute = SessionAttribute.findByValue(rawSessionAttribute);

        switch (attribute) {
            case INPUT_TYPE -> inputType = ProtocolMessageUtil.readEnum(InputType.class, args.get(2), "input type");
            case REPLY_TYPE -> replyType = ProtocolMessageUtil.readEnum(ReplyType.class, args.get(2), "reply type");
            case BATCH -> bucketBatchSize = ProtocolMessageUtil.readAsInteger(args.get(2));
            case OBJECT_ID_FORMAT ->
                    objectIdFormat = ProtocolMessageUtil.readEnum(ObjectIdFormat.class, args.get(2), "object id format");
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
        BATCH("batch"),
        OBJECT_ID_FORMAT("object_id_format");

        final String value;

        SessionAttribute(String value) {
            this.value = value;
        }

        public static SessionAttribute findByValue(String v) {
            String lower = StringUtil.toLowerCaseAscii(v);
            for (SessionAttribute attribute : values()) {
                if (attribute.value.equals(lower)) {
                    return attribute;
                }
            }
            throw new IllegalCommandArgumentException(String.format("Unknown session attribute: '%s'", v));
        }

        public String getValue() {
            return value;
        }
    }
}
