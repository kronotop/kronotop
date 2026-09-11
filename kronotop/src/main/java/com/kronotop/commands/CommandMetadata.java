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

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Metadata of a command or a subcommand, loaded from a JSON definition file.
 * A subcommand names its parent in the container field. The loader fills subcommands.
 * The reply schema is a JSON Schema object kept as a nested map in definition order.
 */
@JsonIgnoreProperties({"function", "get_keys_function"})
public record CommandMetadata(
        String summary,
        String complexity,
        CommandGroup group,
        String since,
        int arity,
        String container,
        List<DocFlag> docFlags,
        String deprecatedSince,
        String replacedBy,
        List<List<String>> history,
        List<CommandFlag> commandFlags,
        List<AclCategory> aclCategories,
        List<String> commandTips,
        List<KeySpec> keySpecs,
        Map<String, Object> replySchema,
        List<Argument> arguments,
        Map<String, CommandMetadata> subcommands
) {
    public CommandMetadata {
        docFlags = docFlags == null ? List.of() : List.copyOf(docFlags);
        history = history == null ? List.of() : List.copyOf(history);
        commandFlags = commandFlags == null ? List.of() : List.copyOf(commandFlags);
        aclCategories = aclCategories == null ? List.of() : List.copyOf(aclCategories);
        commandTips = commandTips == null ? List.of() : List.copyOf(commandTips);
        keySpecs = keySpecs == null ? List.of() : List.copyOf(keySpecs);
        replySchema = replySchema == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(replySchema));
        arguments = arguments == null ? List.of() : List.copyOf(arguments);
        subcommands = subcommands == null ? Map.of() : Collections.unmodifiableMap(new LinkedHashMap<>(subcommands));
    }

    /**
     * Returns a copy with the given subcommands attached.
     */
    public CommandMetadata withSubcommands(Map<String, CommandMetadata> subcommands) {
        return new CommandMetadata(summary, complexity, group, since, arity, container, docFlags, deprecatedSince,
                replacedBy, history, commandFlags, aclCategories, commandTips, keySpecs, replySchema, arguments, subcommands);
    }
}
