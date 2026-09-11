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

import com.kronotop.commands.*;
import com.kronotop.server.resp3.*;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * Builds the COMMAND and COMMAND INFO reply from command metadata. The shape follows
 * the ten-element array Redis clients expect.
 */
public final class CommandInfoReply {
    private CommandInfoReply() {
    }

    /**
     * Builds the info entry for one command. Subcommands are nested with the {@code container|sub} name.
     */
    public static RedisMessage build(String name, CommandMetadata metadata) {
        KeyRange range = KeyRange.of(metadata.keySpecs());
        List<RedisMessage> entry = new ArrayList<>(10);
        entry.add(bulk(name.toLowerCase()));
        entry.add(new IntegerRedisMessage(metadata.arity()));
        entry.add(flags(metadata, range.movable));
        entry.add(new IntegerRedisMessage(range.first));
        entry.add(new IntegerRedisMessage(range.last));
        entry.add(new IntegerRedisMessage(range.step));
        entry.add(categories(metadata));
        entry.add(tips(metadata.commandTips()));
        entry.add(keySpecs(metadata.keySpecs()));
        entry.add(subcommands(name, metadata.subcommands()));
        return new ArrayRedisMessage(entry);
    }

    /**
     * ACL categories of a command: the explicit ones plus the ones derived from its flags, in reply order.
     */
    public static EnumSet<AclCategory> aclCategories(CommandMetadata metadata) {
        EnumSet<AclCategory> categories = EnumSet.noneOf(AclCategory.class);
        categories.addAll(metadata.aclCategories());
        EnumSet<CommandFlag> flags = EnumSet.noneOf(CommandFlag.class);
        flags.addAll(metadata.commandFlags());
        if (flags.contains(CommandFlag.READONLY)) {
            categories.add(AclCategory.READ);
        }
        if (flags.contains(CommandFlag.WRITE)) {
            categories.add(AclCategory.WRITE);
        }
        categories.add(flags.contains(CommandFlag.FAST) ? AclCategory.FAST : AclCategory.SLOW);
        if (flags.contains(CommandFlag.ADMIN)) {
            categories.add(AclCategory.ADMIN);
            categories.add(AclCategory.DANGEROUS);
        }
        if (flags.contains(CommandFlag.BLOCKING)) {
            categories.add(AclCategory.BLOCKING);
        }
        return categories;
    }

    /**
     * Key spec flags as a set of simple strings in reply order.
     */
    public static RedisMessage keySpecFlags(Collection<KeySpecFlag> flags) {
        EnumSet<KeySpecFlag> ordered = EnumSet.noneOf(KeySpecFlag.class);
        ordered.addAll(flags);
        Set<RedisMessage> set = new LinkedHashSet<>();
        for (KeySpecFlag flag : ordered) {
            set.add(new SimpleStringRedisMessage(flag.replyName()));
        }
        return new SetRedisMessage(set);
    }

    private static RedisMessage flags(CommandMetadata metadata, boolean movable) {
        EnumSet<CommandFlag> ordered = EnumSet.noneOf(CommandFlag.class);
        ordered.addAll(metadata.commandFlags());
        if (movable) {
            ordered.add(CommandFlag.MOVABLE_KEYS);
        }
        Set<RedisMessage> flags = new LinkedHashSet<>();
        for (CommandFlag flag : ordered) {
            if (!flag.hidden()) {
                flags.add(new SimpleStringRedisMessage(flag.replyName()));
            }
        }
        return new SetRedisMessage(flags);
    }

    private static RedisMessage categories(CommandMetadata metadata) {
        Set<RedisMessage> set = new LinkedHashSet<>();
        for (AclCategory category : aclCategories(metadata)) {
            set.add(new SimpleStringRedisMessage(category.replyName()));
        }
        return new SetRedisMessage(set);
    }

    private static RedisMessage tips(List<String> tips) {
        Set<RedisMessage> set = new LinkedHashSet<>();
        for (String tip : tips) {
            set.add(bulk(tip));
        }
        return new SetRedisMessage(set);
    }

    private static RedisMessage keySpecs(List<KeySpec> keySpecs) {
        Set<RedisMessage> set = new LinkedHashSet<>();
        for (KeySpec keySpec : keySpecs) {
            Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
            if (keySpec.notes() != null) {
                map.put(bulk("notes"), bulk(keySpec.notes()));
            }
            map.put(bulk("flags"), keySpecFlags(keySpec.flags()));
            map.put(bulk("begin_search"), beginSearch(keySpec.beginSearch()));
            map.put(bulk("find_keys"), findKeys(keySpec.findKeys()));
            set.add(new MapRedisMessage(map));
        }
        return new SetRedisMessage(set);
    }

    private static RedisMessage beginSearch(BeginSearch beginSearch) {
        Map<RedisMessage, RedisMessage> spec = new LinkedHashMap<>();
        switch (beginSearch.type()) {
            case INDEX -> spec.put(bulk("index"), new IntegerRedisMessage(beginSearch.index().pos()));
            case KEYWORD -> {
                spec.put(bulk("keyword"), bulk(beginSearch.keyword().keyword()));
                spec.put(bulk("startfrom"), new IntegerRedisMessage(beginSearch.keyword().startfrom()));
            }
            case UNKNOWN -> {
            }
        }
        return typed(beginSearch.type().replyName(), spec);
    }

    private static RedisMessage findKeys(FindKeys findKeys) {
        Map<RedisMessage, RedisMessage> spec = new LinkedHashMap<>();
        switch (findKeys.type()) {
            case RANGE -> {
                spec.put(bulk("lastkey"), new IntegerRedisMessage(findKeys.range().lastkey()));
                spec.put(bulk("keystep"), new IntegerRedisMessage(findKeys.range().step()));
                spec.put(bulk("limit"), new IntegerRedisMessage(findKeys.range().limit()));
            }
            case KEYNUM -> {
                spec.put(bulk("keynumidx"), new IntegerRedisMessage(findKeys.keynum().keynumidx()));
                spec.put(bulk("firstkey"), new IntegerRedisMessage(findKeys.keynum().firstkey()));
                spec.put(bulk("keystep"), new IntegerRedisMessage(findKeys.keynum().step()));
            }
            case UNKNOWN -> {
            }
        }
        return typed(findKeys.type().replyName(), spec);
    }

    private static RedisMessage typed(String type, Map<RedisMessage, RedisMessage> spec) {
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        map.put(bulk("type"), bulk(type));
        map.put(bulk("spec"), new MapRedisMessage(spec));
        return new MapRedisMessage(map);
    }

    private static RedisMessage subcommands(String name, Map<String, CommandMetadata> subcommands) {
        if (subcommands.isEmpty()) {
            return new SetRedisMessage(Set.of());
        }
        List<RedisMessage> list = new ArrayList<>(subcommands.size());
        String prefix = name.toLowerCase() + "|";
        subcommands.forEach((sub, metadata) -> list.add(build(prefix + sub.toLowerCase(), metadata)));
        return new ArrayRedisMessage(list);
    }

    private static RedisMessage bulk(String value) {
        return new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Legacy first key, last key and step derived from the key specs, same rule as Redis.
     */
    private record KeyRange(int first, int last, int step, boolean movable) {
        private static final KeyRange NONE = new KeyRange(0, 0, 0, false);
        private static final KeyRange MOVABLE = new KeyRange(0, 0, 0, true);

        static KeyRange of(List<KeySpec> keySpecs) {
            if (keySpecs.isEmpty()) {
                return NONE;
            }
            if (keySpecs.size() == 1 && keySpecs.getFirst().isIndexRange()) {
                KeySpec spec = keySpecs.getFirst();
                int pos = spec.beginSearch().index().pos();
                int lastkey = spec.findKeys().range().lastkey();
                boolean incomplete = spec.flags().contains(KeySpecFlag.INCOMPLETE);
                return new KeyRange(pos, lastkey >= 0 ? pos + lastkey : lastkey, spec.findKeys().range().step(), incomplete);
            }
            int first = Integer.MAX_VALUE;
            int last = 0;
            int previousLast = 0;
            boolean movable = false;
            for (KeySpec spec : keySpecs) {
                if (!spec.isIndexRange()) {
                    movable = true;
                    continue;
                }
                int pos = spec.beginSearch().index().pos();
                if (spec.findKeys().range().step() != 1 || (previousLast != 0 && previousLast != pos - 1)) {
                    movable = true;
                    continue;
                }
                if (spec.flags().contains(KeySpecFlag.INCOMPLETE)) {
                    movable = true;
                }
                first = Math.min(first, pos);
                int lastkey = spec.findKeys().range().lastkey();
                int absoluteLast = lastkey >= 0 ? pos + lastkey : lastkey;
                last = Integer.compareUnsigned(last, absoluteLast) >= 0 ? last : absoluteLast;
                previousLast = last;
            }
            if (first == Integer.MAX_VALUE) {
                return MOVABLE;
            }
            return new KeyRange(first, last, 1, movable);
        }
    }
}
