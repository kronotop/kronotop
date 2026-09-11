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

import java.util.Map;

/**
 * Resolves a command name, including the "container|subcommand" form, to its metadata.
 */
public final class CommandLookup {
    private CommandLookup() {
    }

    /**
     * Returns the match for the name or null. The name is matched without regard to case.
     */
    public static Match find(Map<String, CommandMetadata> commands, String name) {
        String[] parts = name.split("\\|", -1);
        if (parts.length > 2) {
            return null;
        }
        String container = parts[0].toUpperCase();
        CommandMetadata metadata = commands.get(container);
        if (metadata == null) {
            return null;
        }
        if (parts.length == 1) {
            return new Match(container.toLowerCase(), metadata);
        }
        String sub = parts[1].toUpperCase();
        CommandMetadata subMetadata = metadata.subcommands().get(sub);
        if (subMetadata == null) {
            return null;
        }
        return new Match(container.toLowerCase() + "|" + sub.toLowerCase(), subMetadata);
    }

    /**
     * A resolved command. The full name is lowercase, subcommands use "container|subcommand".
     */
    public record Match(String fullName, CommandMetadata metadata) {
    }
}
