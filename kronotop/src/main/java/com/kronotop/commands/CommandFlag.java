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
 * Command flags in the order Redis clients expect them in COMMAND INFO.
 * Hidden flags are loaded but never written to a reply.
 */
public enum CommandFlag {
    WRITE,
    READONLY,
    DENYOOM,
    MODULE,
    ADMIN,
    PUBSUB,
    NOSCRIPT,
    BLOCKING,
    LOADING,
    STALE,
    SKIP_MONITOR,
    SKIP_SLOWLOG,
    ASKING,
    FAST,
    NO_AUTH,
    MAY_REPLICATE(true),
    SENTINEL(true),
    ONLY_SENTINEL(true),
    NO_MANDATORY_KEYS,
    PROTECTED(true),
    NO_ASYNC_LOADING,
    NO_MULTI,
    MOVABLE_KEYS("movablekeys"),
    ALLOW_BUSY,
    TOUCHES_ARBITRARY_KEYS(true),
    SCRIPT_RUNNER;

    private final boolean hidden;
    private final String replyName;

    CommandFlag() {
        this(false, null);
    }

    CommandFlag(boolean hidden) {
        this(hidden, null);
    }

    CommandFlag(String replyName) {
        this(false, replyName);
    }

    CommandFlag(boolean hidden, String replyName) {
        this.hidden = hidden;
        this.replyName = replyName == null ? name().toLowerCase() : replyName;
    }

    @JsonCreator
    public static CommandFlag fromValue(String value) {
        for (CommandFlag flag : values()) {
            if (flag.name().equalsIgnoreCase(value)) {
                return flag;
            }
        }
        throw new IllegalArgumentException("unknown command flag '" + value + "'");
    }

    @JsonValue
    public String value() {
        return name();
    }

    /**
     * True for flags Redis does not show in COMMAND INFO.
     */
    public boolean hidden() {
        return hidden;
    }

    /**
     * Name written to the reply.
     */
    public String replyName() {
        return replyName;
    }
}
