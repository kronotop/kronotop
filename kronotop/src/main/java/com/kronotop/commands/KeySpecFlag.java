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
 * Key spec flags in the order Redis clients expect them.
 * The first four keep their uppercase form in replies.
 */
public enum KeySpecFlag {
    RO("RO"),
    RW("RW"),
    OW("OW"),
    RM("RM"),
    ACCESS,
    UPDATE,
    INSERT,
    DELETE,
    NOT_KEY,
    INCOMPLETE,
    VARIABLE_FLAGS;

    private final String replyName;

    KeySpecFlag() {
        this.replyName = name().toLowerCase();
    }

    KeySpecFlag(String replyName) {
        this.replyName = replyName;
    }

    @JsonCreator
    public static KeySpecFlag fromValue(String value) {
        for (KeySpecFlag flag : values()) {
            if (flag.name().equalsIgnoreCase(value)) {
                return flag;
            }
        }
        throw new IllegalArgumentException("unknown key spec flag '" + value + "'");
    }

    @JsonValue
    public String value() {
        return name();
    }

    /**
     * Name written to the reply.
     */
    public String replyName() {
        return replyName;
    }
}
