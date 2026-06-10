/*
 * Copyright (c) 2023-2026 Burak Sezer
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.kronotop.commands;

import io.lettuce.core.protocol.CommandArgs;

/**
 * Represents the arguments for the TICK command.
 * <p>
 * The class provides methods for selecting the read version mode, either FRESH or CACHED,
 * which can be applied to command arguments during the execution of a corresponding operation.
 */
public class TickArgs {
    private String parameter;

    public TickArgs fresh() {
        this.parameter = "FRESH";
        return this;
    }

    public TickArgs cached() {
        this.parameter = "CACHED";
        return this;
    }

    public <K, V> void build(CommandArgs<K, V> args) {
        if (parameter == null) {
            throw new IllegalArgumentException("parameter has to be FRESH or CACHED");
        }
        args.add(parameter);
    }

    public static class Builder {
        private Builder() {
        }

        public static TickArgs fresh() {
            return new TickArgs().fresh();
        }

        public static TickArgs cached() {
            return new TickArgs().cached();
        }
    }
}
