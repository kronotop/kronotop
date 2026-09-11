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
package com.kronotop.core.handlers.server;

import com.kronotop.Context;
import com.kronotop.commands.AclCategory;
import com.kronotop.commands.CommandFlag;
import com.kronotop.commands.CommandLookup;
import com.kronotop.commands.CommandMetadata;
import com.kronotop.core.handlers.server.protocol.CommandMessage;
import com.kronotop.internal.GlobMatcher;
import com.kronotop.server.Handler;
import com.kronotop.server.IllegalCommandArgumentException;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.resp3.*;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.function.BiPredicate;

@Command(CommandMessage.COMMAND)
public class CommandHandler implements Handler {
    private static final List<String> HELP = List.of(
            "COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:",
            "(no subcommand)",
            "    Return details about all commands in this server.",
            "COUNT",
            "    Return the total number of commands in this server.",
            "LIST",
            "    Return a list of all commands in this server.",
            "INFO [<command-name> ...]",
            "    Return details about multiple commands.",
            "    If no command names are given, documentation details for all",
            "    commands are returned.",
            "DOCS [<command-name> ...]",
            "    Return documentation details about multiple commands.",
            "    If no command names are given, documentation details for all",
            "    commands are returned.",
            "GETKEYS <full-command>",
            "    Return the keys from a full command.",
            "GETKEYSANDFLAGS <full-command>",
            "    Return the keys and the access flags from a full command.",
            "HELP",
            "    Print this help."
    );
    private final Context context;

    public CommandHandler(Context context) {
        this.context = context;
    }

    @Override
    public boolean requiresClusterInitialization() {
        return false;
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.COMMAND).set(new CommandMessage(request));
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        CommandMessage message = request.attr(MessageTypes.COMMAND).get();
        if (!message.hasArgument()) {
            response.writeArray(infoForAll());
            return;
        }
        switch (message.getArgument()) {
            case DOCS -> response.writeMap(CommandDocsReply.build(selectCommands(message.getCommands())));
            case INFO -> response.writeArray(message.getCommands().isEmpty() ? infoForAll() : infoFor(message.getCommands()));
            case COUNT -> response.writeInteger(context.getCommandMetadata().size());
            case LIST -> response.writeArray(list(message.getCommands()));
            case GETKEYS -> response.writeArray(getKeys(message.getCommands(), false));
            case GETKEYSANDFLAGS -> response.writeArray(getKeys(message.getCommands(), true));
            case HELP -> response.writeArray(help());
        }
    }

    private List<RedisMessage> infoForAll() {
        Map<String, CommandMetadata> all = context.getCommandMetadata();
        List<RedisMessage> entries = new ArrayList<>(all.size());
        all.forEach((name, metadata) -> entries.add(CommandInfoReply.build(name, metadata)));
        return entries;
    }

    private List<RedisMessage> infoFor(List<String> names) {
        Map<String, CommandMetadata> all = context.getCommandMetadata();
        List<RedisMessage> entries = new ArrayList<>(names.size());
        for (String name : names) {
            CommandLookup.Match match = CommandLookup.find(all, name);
            entries.add(match == null ? NullRedisMessage.INSTANCE : CommandInfoReply.build(match.fullName(), match.metadata()));
        }
        return entries;
    }

    private Map<String, CommandMetadata> selectCommands(List<String> names) {
        Map<String, CommandMetadata> all = context.getCommandMetadata();
        if (names.isEmpty()) {
            return all;
        }
        Map<String, CommandMetadata> selected = new LinkedHashMap<>();
        for (String name : names) {
            CommandLookup.Match match = CommandLookup.find(all, name);
            if (match != null) {
                selected.put(match.fullName(), match.metadata());
            }
        }
        return selected;
    }

    private List<RedisMessage> list(List<String> args) {
        if (args.isEmpty()) {
            return listNames((name, metadata) -> true);
        }
        if (args.size() != 3 || !args.get(0).equalsIgnoreCase("FILTERBY")) {
            throw new IllegalCommandArgumentException("syntax error");
        }
        String type = args.get(1);
        String arg = args.get(2);
        if (type.equalsIgnoreCase("MODULE")) {
            return List.of();
        }
        if (type.equalsIgnoreCase("ACLCAT")) {
            AclCategory category = AclCategory.find(arg);
            if (category == null) {
                return List.of();
            }
            return listNames((name, metadata) -> CommandInfoReply.aclCategories(metadata).contains(category));
        }
        if (type.equalsIgnoreCase("PATTERN")) {
            return listNames((name, metadata) -> GlobMatcher.matches(arg, name, true));
        }
        throw new IllegalCommandArgumentException("syntax error");
    }

    private List<RedisMessage> listNames(BiPredicate<String, CommandMetadata> filter) {
        List<RedisMessage> names = new ArrayList<>();
        context.getCommandMetadata().forEach((name, metadata) -> {
            String fullName = name.toLowerCase();
            if (filter.test(fullName, metadata)) {
                names.add(bulk(fullName));
            }
            metadata.subcommands().forEach((sub, subMetadata) -> {
                String subName = fullName + "|" + sub.toLowerCase();
                if (filter.test(subName, subMetadata)) {
                    names.add(bulk(subName));
                }
            });
        });
        return names;
    }

    private List<RedisMessage> getKeys(List<String> argv, boolean withFlags) {
        CommandMetadata metadata = resolve(argv);
        if (metadata == null) {
            throw new IllegalCommandArgumentException("Invalid command specified");
        }
        if (metadata.keySpecs().isEmpty()) {
            throw new IllegalCommandArgumentException("The command has no key arguments");
        }
        int argc = argv.size();
        if ((metadata.arity() > 0 && metadata.arity() != argc) || argc < -metadata.arity()) {
            throw new IllegalCommandArgumentException("Invalid number of arguments specified for command");
        }
        List<CommandKeyExtractor.Key> keys = CommandKeyExtractor.extract(metadata, argv);
        if (keys == null) {
            if (metadata.commandFlags().contains(CommandFlag.NO_MANDATORY_KEYS)) {
                return List.of();
            }
            throw new IllegalCommandArgumentException("Invalid arguments specified for command");
        }
        List<RedisMessage> result = new ArrayList<>(keys.size());
        for (CommandKeyExtractor.Key key : keys) {
            RedisMessage name = bulk(argv.get(key.pos()));
            if (withFlags) {
                result.add(new ArrayRedisMessage(List.of(name, CommandInfoReply.keySpecFlags(key.flags()))));
            } else {
                result.add(name);
            }
        }
        return result;
    }

    private CommandMetadata resolve(List<String> argv) {
        CommandMetadata metadata = context.getCommandMetadata().get(argv.getFirst().toUpperCase());
        if (metadata == null || argv.size() == 1 || metadata.subcommands().isEmpty()) {
            return metadata;
        }
        return metadata.subcommands().get(argv.get(1).toUpperCase());
    }

    private List<RedisMessage> help() {
        List<RedisMessage> lines = new ArrayList<>(HELP.size());
        for (String line : HELP) {
            lines.add(new SimpleStringRedisMessage(line));
        }
        return lines;
    }

    private static RedisMessage bulk(String value) {
        return new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8)));
    }
}
