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
import com.kronotop.commands.CommandMetadata;
import com.kronotop.commands.KeySpec;
import com.kronotop.core.handlers.server.protocol.CommandMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.RedisMessage;

import java.util.ArrayList;
import java.util.List;

import static com.kronotop.server.RESPUtil.bulkString;

@Command(CommandMessage.COMMAND)
public class CommandHandler implements Handler {
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

    private ArrayRedisMessage processCommandInfo(String command) {
        List<RedisMessage> document = new ArrayList<>();
        CommandMetadata commandMetadata = context.getCommandMetadata().get(command.toUpperCase());
        document.add(bulkString(command.toLowerCase()));
        document.add(new IntegerRedisMessage(commandMetadata.getArity()));

        // FLAGS
        List<RedisMessage> commandFlags = new ArrayList<>();
        for (String flag : commandMetadata.getCommandFlags()) {
            if (flag.equals("SENTINEL")) {
                continue;
            }
            commandFlags.add(bulkString(flag.toLowerCase()));
        }
        document.add(new ArrayRedisMessage(commandFlags));
        ///////

        // FIRST KEY
        int firstKey = 0;
        int lastKey = 0;
        int keyStep = 0;
        for (KeySpec keySpec : commandMetadata.getKeySpecs()) {
            if (keySpec.getBeginSearch() != null) {
                firstKey = keySpec.getBeginSearch().getIndex().getPos();
                lastKey = keySpec.getFindKeys().getRange().getLastkey();
                if (lastKey >= 0) {
                    lastKey += firstKey;
                }
                keyStep = keySpec.getFindKeys().getRange().getStep();
            }
        }
        document.add(new IntegerRedisMessage(firstKey));
        document.add(new IntegerRedisMessage(lastKey));
        document.add(new IntegerRedisMessage(keyStep));
        /////////

        // ACL CATEGORIES
        List<RedisMessage> aclCategories = new ArrayList<>();
        for (KeySpec keySpec : commandMetadata.getKeySpecs()) {
            for (String flag : keySpec.getFlags()) {
                if (flag.equalsIgnoreCase("RW")) {
                    aclCategories.add(bulkString("@write"));
                }
            }
        }
        boolean isFast = false;
        for (String raw : commandMetadata.getAclCategories()) {
            String aclCategory = String.format("@%s", raw.toLowerCase());
            aclCategories.add(bulkString(aclCategory));
            if (raw.equalsIgnoreCase("FAST")) {
                isFast = true;
            }
        }
        for (String flag : commandMetadata.getCommandFlags()) {
            if (flag.equals("SENTINEL")) {
                continue;
            }
            if (flag.equalsIgnoreCase("FAST")) {
                isFast = true;
            }
            String aclCategory = String.format("@%s", flag.toLowerCase());
            aclCategories.add(bulkString(aclCategory));
        }

        if (!isFast) {
            aclCategories.add(bulkString("@slow"));
        }

        document.add(new ArrayRedisMessage(aclCategories));
        ////////

        // TIPS section
        List<RedisMessage> tips = new ArrayList<>();
        for (String tip : commandMetadata.getCommandTips()) {
            tips.add(bulkString(tip.toLowerCase()));
        }
        document.add(new ArrayRedisMessage(tips));

        // KEY SPECIFICATIONS
        List<RedisMessage> keySpecifications = new ArrayList<>();
        for (KeySpec keySpec : commandMetadata.getKeySpecs()) {
            List<RedisMessage> keySpecification = new ArrayList<>();
            if (keySpec.getNotes() != null) {
                keySpecification.add(bulkString("notes"));
                keySpecification.add(bulkString(keySpec.getNotes()));
            }

            List<RedisMessage> flags = new ArrayList<>();
            keySpecification.add(bulkString("flags"));
            for (String flag : keySpec.getFlags()) {
                if (flag.equals("RW")) {
                    flags.add(bulkString(flag));
                } else {
                    flags.add(bulkString(flag.toLowerCase()));
                }
            }
            keySpecification.add(new ArrayRedisMessage(flags));

            keySpecification.add(bulkString("begin_search"));
            List<RedisMessage> beginSearch = new ArrayList<>();
            beginSearch.add(bulkString("type"));
            beginSearch.add(bulkString("index"));
            beginSearch.add(bulkString("spec"));

            List<RedisMessage> beginSearchSpec = new ArrayList<>();
            beginSearchSpec.add(bulkString("index"));
            beginSearchSpec.add(new IntegerRedisMessage(keySpec.getBeginSearch().getIndex().getPos()));
            beginSearch.add(new ArrayRedisMessage(beginSearchSpec));

            keySpecification.add(new ArrayRedisMessage(beginSearch));

            keySpecification.add(bulkString("find_keys"));
            List<RedisMessage> findKeys = new ArrayList<>();
            findKeys.add(bulkString("type"));
            findKeys.add(bulkString("range"));
            findKeys.add(bulkString("spec"));

            List<RedisMessage> findKeysSpec = new ArrayList<>();
            findKeysSpec.add(bulkString("lastkey"));
            findKeysSpec.add(new IntegerRedisMessage(keySpec.getFindKeys().getRange().getLastkey()));
            findKeysSpec.add(bulkString("keystep"));
            findKeysSpec.add(new IntegerRedisMessage(keySpec.getFindKeys().getRange().getStep()));
            findKeysSpec.add(bulkString("limit"));
            findKeysSpec.add(new IntegerRedisMessage(keySpec.getFindKeys().getRange().getLimit()));
            findKeys.add(new ArrayRedisMessage(findKeysSpec));

            keySpecification.add(new ArrayRedisMessage(findKeys));

            keySpecifications.add(new ArrayRedisMessage(keySpecification));
        }

        document.add(new ArrayRedisMessage(keySpecifications));

        // Subcommands section
        // TODO: Add subcommands section
        List<RedisMessage> subcommands = new ArrayList<>();
        document.add(new ArrayRedisMessage(subcommands));

        return new ArrayRedisMessage(document);
    }

    @Override
    public void execute(Request request, Response response) throws Exception {
        List<RedisMessage> root = new ArrayList<>();

        CommandMessage commandMessage = request.attr(MessageTypes.COMMAND).get();
        if (!commandMessage.hasSubcommand()) {
            for (String command : context.getCommandMetadata().keySet()) {
                root.add(processCommandInfo(command));
            }
            response.writeArray(root);
        } else {
            switch (commandMessage.getSubcommand()) {
                case CommandMessage.SUBCOMMAND_INFO:
                    if (commandMessage.getCommands().isEmpty()) {
                        for (String command : context.getCommandMetadata().keySet()) {
                            root.add(processCommandInfo(command));
                        }
                    } else {
                        for (String command : commandMessage.getCommands()) {
                            root.add(processCommandInfo(command));
                        }
                    }
                    response.writeArray(root);
                    break;
                case CommandMessage.SUBCOMMAND_COUNT:
                    response.writeInteger(context.getCommandMetadata().size());
                    break;
                case CommandMessage.SUBCOMMAND_DOCS:
                    response.writeArray(root);
                    break;
                default:
                    response.writeError(String.format("unknown subcommand '%s'. Try COMMAND HELP.", commandMessage.getSubcommand()));
                    break;
            }
        }
    }
}
