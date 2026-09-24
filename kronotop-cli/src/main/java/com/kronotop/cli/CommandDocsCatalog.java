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

package com.kronotop.cli;

import com.kronotop.resp.RespValue;
import org.jline.console.ArgDesc;
import org.jline.console.CmdDesc;
import org.jline.console.CmdLine;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Turns a COMMAND DOCS reply into argument hints for the interactive prompt.
 * Keys are lowercase command names. Subcommands use the "parent|sub" form.
 */
public class CommandDocsCatalog {

    private static final String SUBCOMMAND_PLACEHOLDER = "subcommand";
    private static final String EMPTY_TOKEN = "";
    private static final String NBSP = "\u00A0";

    /**
     * Help entry of one command. Words are the lowercase name parts, such as ["client", "setname"].
     * The name is uppercase, a subcommand name has the form "CLIENT SETNAME". Missing fields are empty strings.
     */
    public record CommandDoc(List<String> words, String name, List<String> usage, String summary, String since,
                             String group) {
        /**
         * Returns the usage as a single line, or an empty string when the command takes no arguments.
         */
        public String usageLine() {
            return String.join(" ", usage).replace(NBSP.charAt(0), ' ');
        }
    }

    private final Map<String, CommandDoc> docs = new HashMap<>();
    private final List<CommandDoc> entries = new ArrayList<>();
    private final Set<String> containers = new HashSet<>();
    private final List<String> commandNames = new ArrayList<>();
    private final Map<String, List<String>> subcommandNames = new HashMap<>();

    public CommandDocsCatalog(RespValue reply) {
        Map<String, RespValue> commands = asMap(reply);
        commands.forEach((name, value) -> {
            commandNames.add(name);
            addCommand(name, value);
        });
    }

    /**
     * Returns all top-level command names in lowercase.
     */
    public List<String> commandNames() {
        return commandNames;
    }

    /**
     * Returns the subcommand names of a container command in lowercase, or an empty list.
     */
    public List<String> subcommandNames(String command) {
        return subcommandNames.getOrDefault(command.toLowerCase(Locale.ROOT), List.of());
    }

    /**
     * Returns the argument hint for the command line, or null when the command is unknown.
     */
    public CmdDesc lookup(CmdLine line) {
        if (line.getDescriptionType() != CmdLine.DescriptionType.COMMAND) {
            return null;
        }
        return lookup(line.getArgs());
    }

    /**
     * The hint list always ends with an empty token. JLine shows the token at the
     * current word position, so the empty token clears the hint after the last argument.
     * For a subcommand the list also starts with an empty token: it stands in for the
     * subcommand word itself, so the following tokens line up with the typed words.
     */
    CmdDesc lookup(List<String> words) {
        if (words == null || words.isEmpty()) {
            return null;
        }
        String command = words.get(0).toLowerCase(Locale.ROOT);
        if (containers.contains(command)) {
            if (words.size() >= 2) {
                String sub = words.get(1).toLowerCase(Locale.ROOT);
                CommandDoc subDoc = docs.get(command + "|" + sub);
                if (subDoc != null) {
                    List<String> names = new ArrayList<>(subDoc.usage().size() + 2);
                    names.add(EMPTY_TOKEN);
                    names.addAll(subDoc.usage());
                    return hint(names);
                }
            }
            return hint(List.of(SUBCOMMAND_PLACEHOLDER));
        }
        CommandDoc doc = docs.get(command);
        if (doc == null) {
            return null;
        }
        return hint(doc.usage());
    }

    private static CmdDesc hint(List<String> names) {
        List<String> out = new ArrayList<>(names.size() + 1);
        out.addAll(names);
        out.add(EMPTY_TOKEN);
        return new CmdDesc(ArgDesc.doArgNames(out));
    }

    /**
     * Returns every entry whose name starts with the given words, in catalog order.
     * "client" matches CLIENT and all CLIENT subcommands. The match ignores case.
     */
    public List<CommandDoc> find(List<String> words) {
        List<CommandDoc> out = new ArrayList<>();
        if (words == null || words.isEmpty()) {
            return out;
        }
        for (CommandDoc doc : entries) {
            if (startsWith(doc.words(), words)) {
                out.add(doc);
            }
        }
        return out;
    }

    /**
     * Returns every entry in the group, in catalog order. The match ignores case.
     */
    public List<CommandDoc> byGroup(String group) {
        List<CommandDoc> out = new ArrayList<>();
        for (CommandDoc doc : entries) {
            if (doc.group().equalsIgnoreCase(group)) {
                out.add(doc);
            }
        }
        return out;
    }

    /**
     * Returns the distinct group names in sorted order.
     */
    public List<String> groupNames() {
        Set<String> groups = new TreeSet<>();
        for (CommandDoc doc : entries) {
            if (!doc.group().isEmpty()) {
                groups.add(doc.group());
            }
        }
        return new ArrayList<>(groups);
    }

    private static boolean startsWith(List<String> name, List<String> prefix) {
        if (prefix.size() > name.size()) {
            return false;
        }
        for (int i = 0; i < prefix.size(); i++) {
            if (!name.get(i).equalsIgnoreCase(prefix.get(i))) {
                return false;
            }
        }
        return true;
    }

    List<String> usage(String command) {
        CommandDoc doc = docs.get(command.toLowerCase(Locale.ROOT));
        return doc == null ? null : doc.usage();
    }

    boolean isContainer(String command) {
        return containers.contains(command.toLowerCase(Locale.ROOT));
    }

    private void addCommand(String name, RespValue value) {
        Map<String, RespValue> fields = asMap(value);
        CommandDoc doc = new CommandDoc(
                List.of(name.split("\\|")),
                name.toUpperCase(Locale.ROOT).replace('|', ' '),
                renderArguments(fields.get("arguments")),
                asString(fields.get("summary")),
                asString(fields.get("since")),
                asString(fields.get("group")));
        docs.put(name, doc);
        entries.add(doc);
        RespValue subcommands = fields.get("subcommands");
        if (subcommands != null) {
            containers.add(name);
            List<String> subs = new ArrayList<>();
            asMap(subcommands).forEach((fullName, subValue) -> {
                subs.add(fullName.substring(fullName.indexOf('|') + 1));
                addCommand(fullName, subValue);
            });
            subcommandNames.put(name, subs);
        }
    }

    /**
     * Renders each argument as one hint token per word the user types.
     * A block such as "SORTBY field ASC|DESC" becomes three tokens so the hint
     * advances with every word.
     */
    private static List<String> renderArguments(RespValue arguments) {
        List<String> out = new ArrayList<>();
        if (arguments instanceof RespValue.Array(List<RespValue> values)) {
            for (RespValue argument : values) {
                out.addAll(renderArgument(asMap(argument)));
            }
        }
        return out;
    }

    private static List<String> renderArgument(Map<String, RespValue> argument) {
        String type = asString(argument.get("type"));
        String token = asString(argument.get("token"));
        List<String> tokens = new ArrayList<>();
        switch (type) {
            case "pure-token" -> addIfPresent(tokens, token);
            case "oneof" -> {
                List<String> choices = new ArrayList<>();
                if (argument.get("arguments") instanceof RespValue.Array(List<RespValue> values)) {
                    for (RespValue child : values) {
                        List<String> childTokens = renderArgument(asMap(child));
                        if (!childTokens.isEmpty()) {
                            choices.add(String.join(NBSP, childTokens));
                        }
                    }
                }
                addIfPresent(tokens, token);
                addIfPresent(tokens, String.join("|", choices));
            }
            case "block" -> {
                addIfPresent(tokens, token);
                tokens.addAll(renderArguments(argument.get("arguments")));
            }
            default -> {
                String display = asString(argument.get("display_text"));
                addIfPresent(tokens, token);
                addIfPresent(tokens, display.isEmpty() ? asString(argument.get("name")) : display);
            }
        }
        if (tokens.isEmpty()) {
            return tokens;
        }
        Set<String> flags = asStringSet(argument.get("flags"));
        int last = tokens.size() - 1;
        if (flags.contains("multiple")) {
            tokens.set(last, tokens.get(last) + "...");
        }
        if (flags.contains("optional")) {
            tokens.set(0, "[" + tokens.get(0));
            tokens.set(last, tokens.get(last) + "]");
        }
        return tokens;
    }

    private static void addIfPresent(List<String> tokens, String value) {
        if (!value.isEmpty()) {
            tokens.add(value.replace(' ', NBSP.charAt(0)));
        }
    }

    private static Map<String, RespValue> asMap(RespValue value) {
        Map<String, RespValue> out = new LinkedHashMap<>();
        if (value instanceof RespValue.RespMap(Map<RespValue, RespValue> values)) {
            values.forEach((k, v) -> out.put(asString(k), v));
        } else if (value instanceof RespValue.Array(List<RespValue> values)) {
            for (int i = 0; i + 1 < values.size(); i += 2) {
                out.put(asString(values.get(i)), values.get(i + 1));
            }
        }
        return out;
    }

    private static Set<String> asStringSet(RespValue value) {
        Set<String> out = new HashSet<>();
        if (value instanceof RespValue.RespSet(Set<RespValue> values)) {
            values.forEach(v -> out.add(asString(v)));
        } else if (value instanceof RespValue.Array(List<RespValue> values)) {
            values.forEach(v -> out.add(asString(v)));
        }
        return out;
    }

    private static String asString(RespValue value) {
        return switch (value) {
            case RespValue.BlobString(String s) -> s;
            case RespValue.SimpleString(String s) -> s;
            case RespValue.VerbatimString(String ignored, String s) -> s;
            case null, default -> "";
        };
    }
}
