/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.namespace.handlers.protocol;

import com.kronotop.KronotopException;
import com.kronotop.namespace.handlers.Namespace;
import com.kronotop.internal.ProtocolMessageUtil;
import com.kronotop.server.ProtocolMessage;
import com.kronotop.server.Request;
import com.kronotop.server.UnknownSubcommandException;
import com.kronotop.server.WrongNumberOfArgumentsException;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class NamespaceMessage implements ProtocolMessage<Void> {
    public static final String COMMAND = "NAMESPACE";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    private final Request request;
    private NamespaceSubcommand subcommand;
    private CreateMessage createMessage;
    private ListMessage listMessage;
    private MoveMessage moveMessage;
    private RemoveMessage removeMessage;
    private PurgeMessage purgeMessage;
    private ExistsMessage existsMessage;
    private UseMessage useMessage;

    public NamespaceMessage(Request request) {
        this.request = request;
        parse();
    }

    private void validateSubpath(List<String> subpath) {
        for (String item : subpath) {
            if (item.equals(Namespace.INTERNAL_LEAF)) {
                throw new KronotopException("Namespace '" + String.join(".", subpath) + "' is reserved for internal use");
            }
        }
    }

    private void parseCreateCommand() {
        createMessage = new CreateMessage();
        ListIterator<ByteBuf> iterator = request.getParams().listIterator(1);
        while (iterator.hasNext()) {
            ByteBuf rawItem = iterator.next();
            String item = ProtocolMessageUtil.readAsString(rawItem);

            if (!createMessage.getSubpath().isEmpty()) {
                throw new WrongNumberOfArgumentsException(
                        String.format("wrong number of arguments for '%s %s' command", request.getCommand(), subcommand)
                );
            }

            for (String sb : item.split("\\.")) {
                createMessage.addToSubpath(sb);
            }

            validateSubpath(createMessage.subpath);
        }

        if (createMessage.getSubpath().isEmpty()) {
            throw new WrongNumberOfArgumentsException(
                    String.format("wrong number of arguments for '%s %s' command", request.getCommand(), subcommand)
            );
        }
    }

    private void parseListCommand() {
        listMessage = new ListMessage();
        ListIterator<ByteBuf> iterator = request.getParams().listIterator(1);
        while (iterator.hasNext()) {
            ByteBuf rawItem = iterator.next();
            String item = ProtocolMessageUtil.readAsString(rawItem);
            for (String sb : item.split("\\.")) {
                listMessage.addToSubpath(sb);
            }
        }
        validateSubpath(listMessage.subpath);
    }

    private void parseMoveCommand() {
        if (request.getParams().size() <= 2) {
            throw new WrongNumberOfArgumentsException(String.format("wrong number of arguments for '%s' command", request.getCommand()));
        }

        moveMessage = new MoveMessage();
        ByteBuf oldPath = request.getParams().get(1);
        String oldPathStr = ProtocolMessageUtil.readAsString(oldPath);
        for (String sb : oldPathStr.split("\\.")) {
            moveMessage.addToOldPath(sb);
        }

        ByteBuf newPath = request.getParams().get(2);
        String newPathStr = ProtocolMessageUtil.readAsString(newPath);
        for (String sb : newPathStr.split("\\.")) {
            moveMessage.addToNewPath(sb);
        }
        validateSubpath(moveMessage.oldPath);
        validateSubpath(moveMessage.newPath);
    }

    private void parseRemoveCommand() {
        if (request.getParams().size() != 2) {
            throw new WrongNumberOfArgumentsException(String.format("wrong number of arguments for '%s %s' command", request.getCommand(), subcommand));
        }

        removeMessage = new RemoveMessage();
        ByteBuf path = request.getParams().get(1);
        String pathStr = ProtocolMessageUtil.readAsString(path);
        for (String sb : pathStr.split("\\.")) {
            removeMessage.addToPath(sb);
        }
        validateSubpath(removeMessage.subpath);
    }

    private void parsePurgeCommand() {
        if (request.getParams().size() != 2) {
            throw new WrongNumberOfArgumentsException(String.format("wrong number of arguments for '%s %s' command", request.getCommand(), subcommand));
        }

        purgeMessage = new PurgeMessage();
        ByteBuf path = request.getParams().get(1);
        String pathStr = ProtocolMessageUtil.readAsString(path);
        for (String sb : pathStr.split("\\.")) {
            purgeMessage.addToPath(sb);
        }
        validateSubpath(purgeMessage.subpath);
    }

    private void parseExistsCommand() {
        if (request.getParams().size() <= 1) {
            throw new WrongNumberOfArgumentsException(String.format("wrong number of arguments for '%s %s' command", request.getCommand(), subcommand));
        }

        existsMessage = new ExistsMessage();
        ByteBuf path = request.getParams().get(1);
        String pathStr = ProtocolMessageUtil.readAsString(path);
        for (String sb : pathStr.split("\\.")) {
            existsMessage.addToPath(sb);
        }
        validateSubpath(existsMessage.subpath);
    }

    private void parseUseCommand() {
        if (request.getParams().size() <= 1) {
            throw new WrongNumberOfArgumentsException(String.format("wrong number of arguments for '%s %s' command", request.getCommand(), subcommand));
        }

        useMessage = new UseMessage();
        ByteBuf path = request.getParams().get(1);
        String pathStr = ProtocolMessageUtil.readAsString(path);
        for (String sb : pathStr.split("\\.")) {
            useMessage.addToPath(sb);
        }
        validateSubpath(useMessage.subpath);
    }

    private void parse() {
        byte[] rawSubcommand = new byte[request.getParams().getFirst().readableBytes()];
        request.getParams().getFirst().readBytes(rawSubcommand);

        String preSubcommand = new String(rawSubcommand).trim().toUpperCase();
        if (preSubcommand.isBlank()) {
            throw new IllegalArgumentException("invalid subcommand given");
        }

        try {
            subcommand = NamespaceSubcommand.valueOf(preSubcommand);
        } catch (IllegalArgumentException e) {
            throw new UnknownSubcommandException(preSubcommand);
        }
        switch (subcommand) {
            case CREATE:
                parseCreateCommand();
                break;
            case LIST:
                parseListCommand();
                break;
            case MOVE:
                parseMoveCommand();
                break;
            case REMOVE:
                parseRemoveCommand();
                break;
            case PURGE:
                parsePurgeCommand();
                break;
            case EXISTS:
                parseExistsCommand();
                break;
            case CURRENT:
                break;
            case USE:
                parseUseCommand();
                break;
            default:
                throw new UnknownSubcommandException(subcommand.toString());
        }
    }

    public UseMessage getUseMessage() {
        return useMessage;
    }

    public ExistsMessage getExistsMessage() {
        return existsMessage;
    }

    public RemoveMessage getRemoveMessage() {
        return removeMessage;
    }

    public PurgeMessage getPurgeMessage() {
        return purgeMessage;
    }

    public MoveMessage getMoveMessage() {
        return moveMessage;
    }

    public ListMessage getListMessage() {
        return listMessage;
    }

    public CreateMessage getCreateMessage() {
        return createMessage;
    }

    public NamespaceSubcommand getSubcommand() {
        return subcommand;
    }

    @Override
    public Void getKey() {
        return null;
    }

    @Override
    public List<Void> getKeys() {
        return null;
    }

    public static class UseMessage {
        private final List<String> subpath = new ArrayList<>();

        private void addToPath(String item) {
            subpath.add(item);
        }

        public List<String> getSubpath() {
            return subpath;
        }
    }

    public static class ExistsMessage {
        private final List<String> subpath = new ArrayList<>();

        private void addToPath(String item) {
            subpath.add(item);
        }

        public List<String> getSubpath() {
            return subpath;
        }
    }

    public static class RemoveMessage {
        private final List<String> subpath = new ArrayList<>();

        private void addToPath(String item) {
            subpath.add(item);
        }

        public List<String> getSubpath() {
            return subpath;
        }
    }

    public static class PurgeMessage {
        private final List<String> subpath = new ArrayList<>();

        private void addToPath(String item) {
            subpath.add(item);
        }

        public List<String> getSubpath() {
            return subpath;
        }
    }

    public static class MoveMessage {
        private final List<String> oldPath = new ArrayList<>();
        private final List<String> newPath = new ArrayList<>();


        private void addToNewPath(String item) {
            newPath.add(item);
        }

        private void addToOldPath(String item) {
            oldPath.add(item);
        }


        public List<String> getOldPath() {
            return oldPath;
        }

        public List<String> getNewPath() {
            return newPath;
        }
    }

    public static class ListMessage {
        private final List<String> subpath = new ArrayList<>();

        private void addToSubpath(String item) {
            subpath.add(item);
        }

        public List<String> getSubpath() {
            return subpath;
        }
    }

    public static class CreateMessage {
        private final List<String> subpath = new ArrayList<>();

        private void addToSubpath(String item) {
            subpath.add(item);
        }

        public List<String> getSubpath() {
            return subpath;
        }
    }
}