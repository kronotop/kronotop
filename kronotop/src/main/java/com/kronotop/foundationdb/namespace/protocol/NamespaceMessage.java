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
 *
 */

package com.kronotop.foundationdb.namespace.protocol;

import com.kronotop.server.KronotopMessage;
import com.kronotop.server.Request;
import com.kronotop.server.UnknownSubcommandException;
import com.kronotop.server.WrongNumberOfArgumentsException;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;
import java.util.ListIterator;

public class NamespaceMessage implements KronotopMessage<Void> {
    public static final String COMMAND = "NAMESPACE";
    public static final int MINIMUM_PARAMETER_COUNT = 1;
    private final Request request;
    private NamespaceSubcommand subcommand;
    private CreateMessage createMessage;
    private ListMessage listMessage;
    private MoveMessage moveMessage;
    private RemoveMessage removeMessage;
    private ExistsMessage existsMessage;
    private UseMessage useMessage;

    public NamespaceMessage(Request request) {
        this.request = request;
        parse();
    }

    private String readFromByteBuf(ByteBuf buf) {
        byte[] rawItem = new byte[buf.readableBytes()];
        buf.readBytes(rawItem);
        return new String(rawItem);
    }

    private void parseCreateCommand() {
        createMessage = new CreateMessage();
        ListIterator<ByteBuf> iterator = request.getParams().listIterator(1);
        while (iterator.hasNext()) {
            ByteBuf rawItem = iterator.next();
            String item = readFromByteBuf(rawItem);

            if (item.equalsIgnoreCase(CreateMessage.LAYER_PARAMETER)) {
                ByteBuf rawLayer = iterator.next();
                String layer = readFromByteBuf(rawLayer);
                createMessage.setLayer(layer);
                continue;
            }

            if (item.equalsIgnoreCase(CreateMessage.PREFIX_PARAMETER)) {
                ByteBuf rawPrefix = iterator.next();
                String prefix = readFromByteBuf(rawPrefix);
                createMessage.setPrefix(prefix);
                continue;
            }

            if (!createMessage.getSubpath().isEmpty()) {
                throw new WrongNumberOfArgumentsException(
                        String.format("wrong number of arguments for '%s %s' command", request.getCommand(), subcommand)
                );
            }

            for (String sb : item.split("\\.")) {
                createMessage.addToSubpath(sb);
            }
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
            String item = readFromByteBuf(rawItem);
            for (String sb : item.split("\\.")) {
                listMessage.addToSubpath(sb);
            }
        }
    }

    private void parseMoveCommand() {
        if (request.getParams().size() <= 2) {
            throw new WrongNumberOfArgumentsException(String.format("wrong number of arguments for '%s' command", request.getCommand()));
        }

        moveMessage = new MoveMessage();
        ByteBuf oldPath = request.getParams().get(1);
        String oldPathStr = readFromByteBuf(oldPath);
        for (String sb : oldPathStr.split("\\.")) {
            moveMessage.addToOldPath(sb);
        }

        ByteBuf newPath = request.getParams().get(2);
        String newPathStr = readFromByteBuf(newPath);
        for (String sb : newPathStr.split("\\.")) {
            moveMessage.addToNewPath(sb);
        }
    }

    private void parseRemoveCommand() {
        if (request.getParams().size() != 2) {
            throw new WrongNumberOfArgumentsException(String.format("wrong number of arguments for '%s %s' command", request.getCommand(), subcommand));
        }

        removeMessage = new RemoveMessage();
        ByteBuf path = request.getParams().get(1);
        String pathStr = readFromByteBuf(path);
        for (String sb : pathStr.split("\\.")) {
            removeMessage.addToPath(sb);
        }
    }

    private void parseExistsCommand() {
        if (request.getParams().size() <= 1) {
            throw new WrongNumberOfArgumentsException(String.format("wrong number of arguments for '%s %s' command", request.getCommand(), subcommand));
        }

        existsMessage = new ExistsMessage();
        ByteBuf path = request.getParams().get(1);
        String pathStr = readFromByteBuf(path);
        for (String sb : pathStr.split("\\.")) {
            existsMessage.addToPath(sb);
        }
    }

    private void parseUseCommand() {
        if (request.getParams().size() <= 1) {
            throw new WrongNumberOfArgumentsException(String.format("wrong number of arguments for '%s %s' command", request.getCommand(), subcommand));
        }

        useMessage = new UseMessage();
        ByteBuf path = request.getParams().get(1);
        String pathStr = readFromByteBuf(path);
        for (String sb : pathStr.split("\\.")) {
            useMessage.addToPath(sb);
        }
    }

    private void parse() {
        byte[] rawSubcommand = new byte[request.getParams().get(0).readableBytes()];
        request.getParams().get(0).readBytes(rawSubcommand);

        String preSubcommand = new String(rawSubcommand).trim().toUpperCase();
        if (preSubcommand.isEmpty() || preSubcommand.isBlank()) {
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
        private final List<String> path = new ArrayList<>();

        private void addToPath(String item) {
            path.add(item);
        }

        public List<String> getPath() {
            return path;
        }
    }

    public static class ExistsMessage {
        private final List<String> path = new ArrayList<>();

        private void addToPath(String item) {
            path.add(item);
        }

        public List<String> getPath() {
            return path;
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
        public static final String LAYER_PARAMETER = "LAYER";
        public static final String PREFIX_PARAMETER = "PREFIX";
        private final List<String> subpath = new ArrayList<>();
        private String layer;
        private String prefix;

        public String getLayer() {
            return layer;
        }

        private void setLayer(String layer) {
            this.layer = layer;
        }

        public boolean hasLayer() {
            if (layer == null) {
                return false;
            }
            return !layer.isEmpty();
        }

        public String getPrefix() {
            return prefix;
        }

        private void setPrefix(String prefix) {
            this.prefix = prefix;
        }

        public boolean hasPrefix() {
            if (prefix == null) {
                return false;
            }
            return !prefix.isEmpty();
        }

        private void addToSubpath(String item) {
            subpath.add(item);
        }

        public List<String> getSubpath() {
            return subpath;
        }
    }
}
