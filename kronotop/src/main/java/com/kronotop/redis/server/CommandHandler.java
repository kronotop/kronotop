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

package com.kronotop.redis.server;

import com.kronotop.core.commands.CommandMetadata;
import com.kronotop.core.commands.KeySpec;
import com.kronotop.redis.RedisService;
import com.kronotop.redis.server.protocol.CommandMessage;
import com.kronotop.server.Handler;
import com.kronotop.server.MessageTypes;
import com.kronotop.server.Request;
import com.kronotop.server.Response;
import com.kronotop.server.annotation.Command;
import com.kronotop.server.resp3.*;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

@Command(CommandMessage.COMMAND)
public class CommandHandler implements Handler {
    private final RedisService service;

    public CommandHandler(RedisService service) {
        this.service = service;
    }

    private FullBulkStringRedisMessage bulkStringReply(Response response, String str) {
        ByteBuf buf = response.getChannelContext().channel().alloc().buffer();
        buf.writeBytes(str.getBytes());
        return new FullBulkStringRedisMessage(buf);
    }

    @Override
    public void beforeExecute(Request request) {
        request.attr(MessageTypes.COMMAND).set(new CommandMessage(request));
    }

    private ArrayRedisMessage processCommandInfo(String command, Response response) {
        List<RedisMessage> document = new ArrayList<>();
        CommandMetadata commandMetadata = service.getContext().getCommandMetadata().get(command.toUpperCase());
        document.add(bulkStringReply(response, command.toLowerCase()));
        document.add(new IntegerRedisMessage(commandMetadata.getArity()));

        // FLAGS
        List<RedisMessage> commandFlags = new ArrayList<>();
        for (String flag : commandMetadata.getCommandFlags()) {
            if (flag.equals("SENTINEL")) {
                continue;
            }
            commandFlags.add(new SimpleStringRedisMessage(flag.toLowerCase()));
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
                    aclCategories.add(new SimpleStringRedisMessage("@write"));
                }
            }
        }
        boolean isFast = false;
        for (String raw : commandMetadata.getAclCategories()) {
            String aclCategory = String.format("@%s", raw.toLowerCase());
            aclCategories.add(new SimpleStringRedisMessage(aclCategory));
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
            aclCategories.add(new SimpleStringRedisMessage(aclCategory));
        }

        if (!isFast) {
            aclCategories.add(new SimpleStringRedisMessage("@slow"));
        }

        document.add(new ArrayRedisMessage(aclCategories));
        ////////

        // TIPS section
        List<RedisMessage> tips = new ArrayList<>();
        for (String tip : commandMetadata.getCommandTips()) {
            tips.add(bulkStringReply(response, tip.toLowerCase()));
        }
        document.add(new ArrayRedisMessage(tips));

        // KEY SPECIFICATIONS
        List<RedisMessage> keySpecifications = new ArrayList<>();
        for (KeySpec keySpec : commandMetadata.getKeySpecs()) {
            List<RedisMessage> keySpecification = new ArrayList<>();
            if (keySpec.getNotes() != null) {
                keySpecification.add(bulkStringReply(response, "notes"));
                keySpecification.add(bulkStringReply(response, keySpec.getNotes()));
            }

            List<RedisMessage> flags = new ArrayList<>();
            keySpecification.add(bulkStringReply(response, "flags"));
            for (String flag : keySpec.getFlags()) {
                if (flag.equals("RW")) {
                    flags.add(new SimpleStringRedisMessage(flag));
                } else {
                    flags.add(new SimpleStringRedisMessage(flag.toLowerCase()));
                }
            }
            keySpecification.add(new ArrayRedisMessage(flags));

            keySpecification.add(bulkStringReply(response, "begin_search"));
            List<RedisMessage> beginSearch = new ArrayList<>();
            beginSearch.add(bulkStringReply(response, "type"));
            beginSearch.add(bulkStringReply(response, "index"));
            beginSearch.add(bulkStringReply(response, "spec"));

            List<RedisMessage> beginSearchSpec = new ArrayList<>();
            beginSearchSpec.add(bulkStringReply(response, "index"));
            beginSearchSpec.add(new IntegerRedisMessage(keySpec.getBeginSearch().getIndex().getPos()));
            beginSearch.add(new ArrayRedisMessage(beginSearchSpec));

            keySpecification.add(new ArrayRedisMessage(beginSearch));

            keySpecification.add(bulkStringReply(response, "find_keys"));
            List<RedisMessage> findKeys = new ArrayList<>();
            findKeys.add(bulkStringReply(response, "type"));
            findKeys.add(bulkStringReply(response, "range"));
            findKeys.add(bulkStringReply(response, "spec"));

            List<RedisMessage> findKeysSpec = new ArrayList<>();
            findKeysSpec.add(bulkStringReply(response, "lastkey"));
            findKeysSpec.add(new IntegerRedisMessage(keySpec.getFindKeys().getRange().getLastkey()));
            findKeysSpec.add(bulkStringReply(response, "keystep"));
            findKeysSpec.add(new IntegerRedisMessage(keySpec.getFindKeys().getRange().getStep()));
            findKeysSpec.add(bulkStringReply(response, "limit"));
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
            for (String command : service.getContext().getCommandMetadata().keySet()) {
                root.add(processCommandInfo(command, response));
            }
            response.writeArray(root);
        } else {
            switch (commandMessage.getSubcommand()) {
                case CommandMessage.SUBCOMMAND_INFO:
                    if (commandMessage.getCommands().isEmpty()) {
                        for (String command : service.getContext().getCommandMetadata().keySet()) {
                            root.add(processCommandInfo(command, response));
                        }
                    } else {
                        for (String command : commandMessage.getCommands()) {
                            root.add(processCommandInfo(command, response));
                        }
                    }
                    response.writeArray(root);
                    break;
                case CommandMessage.SUBCOMMAND_COUNT:
                    response.writeInteger(service.getContext().getCommandMetadata().size());
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
