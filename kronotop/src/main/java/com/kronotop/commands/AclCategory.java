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
package com.kronotop.commands;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * ACL categories in the order Redis clients expect them in COMMAND INFO.
 * The Redis categories come first, Kronotop categories follow.
 */
public enum AclCategory {
    KEYSPACE,
    READ,
    WRITE,
    SET,
    SORTEDSET,
    LIST,
    HASH,
    STRING,
    ARRAY,
    BITMAP,
    HYPERLOGLOG,
    GEO,
    STREAM,
    PUBSUB,
    ADMIN,
    FAST,
    SLOW,
    BLOCKING,
    DANGEROUS,
    CONNECTION,
    TRANSACTION,
    SCRIPTING,
    BUCKET,
    ZMAP,
    VOLUME,
    SEGMENT,
    CHANGELOG,
    TASK,
    SESSION,
    NAMESPACE;

    @JsonCreator
    public static AclCategory fromValue(String value) {
        for (AclCategory category : values()) {
            if (category.name().equalsIgnoreCase(value)) {
                return category;
            }
        }
        throw new IllegalArgumentException("unknown ACL category '" + value + "'");
    }

    /**
     * Same as {@link #fromValue} but returns null instead of throwing.
     */
    public static AclCategory find(String value) {
        for (AclCategory category : values()) {
            if (category.name().equalsIgnoreCase(value)) {
                return category;
            }
        }
        return null;
    }

    @JsonValue
    public String value() {
        return name();
    }

    /**
     * Name written to the reply, with the "@" prefix.
     */
    public String replyName() {
        return "@" + name().toLowerCase();
    }
}
