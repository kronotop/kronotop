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

import com.kronotop.commands.Argument;
import com.kronotop.commands.CommandMetadata;
import com.kronotop.commands.DocFlag;
import com.kronotop.server.resp3.*;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Builds the COMMAND DOCS reply from command metadata. Field order and omission rules follow
 * the format Redis clients expect.
 */
public final class CommandDocsReply {
    private static final String ONEOF = "oneof";
    private static final String BLOCK = "block";

    private CommandDocsReply() {
    }

    /**
     * Builds the top-level map. Keys are lowercase command names.
     */
    public static Map<RedisMessage, RedisMessage> build(Map<String, CommandMetadata> commands) {
        Map<RedisMessage, RedisMessage> root = new LinkedHashMap<>();
        commands.forEach((name, metadata) -> root.put(bulk(name.toLowerCase()), command(name, metadata)));
        return root;
    }

    private static RedisMessage command(String name, CommandMetadata metadata) {
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        putString(map, "summary", metadata.summary());
        putString(map, "since", metadata.since());
        putString(map, "group", metadata.group() == null ? null : metadata.group().value());
        putString(map, "complexity", metadata.complexity());
        if (!metadata.docFlags().isEmpty()) {
            map.put(bulk("doc_flags"), docFlags(metadata.docFlags()));
        }
        putString(map, "deprecated_since", metadata.deprecatedSince());
        putString(map, "replaced_by", metadata.replacedBy());
        if (!metadata.history().isEmpty()) {
            map.put(bulk("history"), history(metadata.history()));
        }
        if (!metadata.replySchema().isEmpty()) {
            map.put(bulk("reply_schema"), json(metadata.replySchema()));
        }
        if (!metadata.arguments().isEmpty()) {
            map.put(bulk("arguments"), arguments(metadata.arguments()));
        }
        if (!metadata.subcommands().isEmpty()) {
            Map<RedisMessage, RedisMessage> subcommands = new LinkedHashMap<>();
            String prefix = name.toLowerCase() + "|";
            metadata.subcommands().forEach((sub, subMetadata) ->
                    subcommands.put(bulk(prefix + sub.toLowerCase()), command(sub, subMetadata)));
            map.put(bulk("subcommands"), new MapRedisMessage(subcommands));
        }
        return new MapRedisMessage(map);
    }

    private static RedisMessage arguments(List<Argument> arguments) {
        List<RedisMessage> list = new ArrayList<>(arguments.size());
        for (Argument argument : arguments) {
            list.add(argument(argument));
        }
        return new ArrayRedisMessage(list);
    }

    private static RedisMessage argument(Argument argument) {
        boolean container = ONEOF.equals(argument.type()) || BLOCK.equals(argument.type());
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        putString(map, "name", argument.name());
        putString(map, "type", argument.type());
        if (!container) {
            putString(map, "display_text", argument.display() == null ? argument.name() : argument.display());
        }
        if (argument.keySpecIndex() != null) {
            map.put(bulk("key_spec_index"), new IntegerRedisMessage(argument.keySpecIndex()));
        }
        putString(map, "token", argument.token());
        putString(map, "summary", argument.summary());
        putString(map, "since", argument.since());
        putString(map, "deprecated_since", argument.deprecatedSince());
        List<String> flags = new ArrayList<>(3);
        if (argument.optional()) {
            flags.add("optional");
        }
        if (argument.multiple()) {
            flags.add("multiple");
        }
        if (argument.multipleToken()) {
            flags.add("multiple_token");
        }
        if (!flags.isEmpty()) {
            map.put(bulk("flags"), flags(flags));
        }
        if (container) {
            map.put(bulk("arguments"), arguments(argument.arguments()));
        }
        return new MapRedisMessage(map);
    }

    /**
     * Maps a JSON object to RESP: objects become maps, arrays become arrays, scalars keep their type.
     */
    private static RedisMessage json(Map<String, Object> object) {
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        object.forEach((key, value) -> map.put(bulk(key), jsonValue(value)));
        return new MapRedisMessage(map);
    }

    @SuppressWarnings("unchecked")
    private static RedisMessage jsonValue(Object value) {
        return switch (value) {
            case null -> NullRedisMessage.INSTANCE;
            case Boolean b -> b ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE;
            case Integer i -> new IntegerRedisMessage(i);
            case Long l -> new IntegerRedisMessage(l);
            case Number n -> new DoubleRedisMessage(n.doubleValue());
            case String s -> bulk(s);
            case Map<?, ?> m -> json((Map<String, Object>) m);
            case List<?> list -> {
                List<RedisMessage> items = new ArrayList<>(list.size());
                for (Object item : list) {
                    items.add(jsonValue(item));
                }
                yield new ArrayRedisMessage(items);
            }
            default -> throw new IllegalStateException("unsupported reply_schema value: " + value.getClass().getName());
        };
    }

    private static RedisMessage history(List<List<String>> history) {
        Set<RedisMessage> entries = new LinkedHashSet<>();
        for (List<String> entry : history) {
            List<RedisMessage> pair = new ArrayList<>(entry.size());
            for (String value : entry) {
                pair.add(bulk(value));
            }
            entries.add(new ArrayRedisMessage(pair));
        }
        return new SetRedisMessage(entries);
    }

    private static RedisMessage docFlags(List<DocFlag> flags) {
        EnumSet<DocFlag> ordered = EnumSet.noneOf(DocFlag.class);
        ordered.addAll(flags);
        Set<RedisMessage> set = new LinkedHashSet<>();
        for (DocFlag flag : ordered) {
            set.add(new SimpleStringRedisMessage(flag.replyName()));
        }
        return new SetRedisMessage(set);
    }

    private static RedisMessage flags(List<String> flags) {
        Set<RedisMessage> set = new LinkedHashSet<>();
        for (String flag : flags) {
            set.add(new SimpleStringRedisMessage(flag.toLowerCase()));
        }
        return new SetRedisMessage(set);
    }

    private static void putString(Map<RedisMessage, RedisMessage> map, String key, String value) {
        if (value != null) {
            map.put(bulk(key), bulk(value));
        }
    }

    private static RedisMessage bulk(String value) {
        return new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8)));
    }
}
