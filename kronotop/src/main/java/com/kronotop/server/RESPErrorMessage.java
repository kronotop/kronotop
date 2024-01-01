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

package com.kronotop.server;

import com.kronotop.common.resp.RESPError;

/**
 * Represents an error message in the RESP protocol.
 */
public class RESPErrorMessage {
    private final RESPError prefix;
    private final String message;


    public RESPErrorMessage(RESPError prefix, String message) {
        this.prefix = prefix;
        this.message = message;
    }

    public RESPError getPrefix() {
        return prefix;
    }

    public String getMessage() {
        return message;
    }
}
