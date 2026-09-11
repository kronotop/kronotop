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
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;


import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CommandInfoReplyTest {

    private static CommandMetadata metadata(List<CommandFlag> flags, List<AclCategory> categories, List<String> tips,
                                            List<KeySpec> keySpecs, Map<String, CommandMetadata> subcommands) {
        return new CommandMetadata("Summary", "O(1)", CommandGroup.STRING, "2026.06-1", -3, null,
                null, null, null, null, flags, categories, tips, keySpecs, null, null, subcommands);
    }

    private static KeySpec keySpec(String notes, List<KeySpecFlag> flags, int pos, int lastkey, int step) {
        return new KeySpec(notes, flags, new BeginSearch(new Index(pos)), new FindKeys(new Range(lastkey, step, 0)));
    }

    private static List<RedisMessage> entry(String name, CommandMetadata metadata) {
        RedisMessage message = CommandInfoReply.build(name, metadata);
        assertInstanceOf(ArrayRedisMessage.class, message);
        List<RedisMessage> children = ((ArrayRedisMessage) message).children();
        assertEquals(10, children.size());
        return children;
    }

    private static String text(RedisMessage message) {
        assertInstanceOf(FullBulkStringRedisMessage.class, message);
        return ((FullBulkStringRedisMessage) message).content().toString(StandardCharsets.UTF_8);
    }

    private static long integer(RedisMessage message) {
        assertInstanceOf(IntegerRedisMessage.class, message);
        return ((IntegerRedisMessage) message).value();
    }

    private static List<String> statuses(RedisMessage message) {
        assertInstanceOf(SetRedisMessage.class, message);
        return ((SetRedisMessage) message).children().stream().map(child -> {
            assertInstanceOf(SimpleStringRedisMessage.class, child);
            return ((SimpleStringRedisMessage) child).content();
        }).toList();
    }

    private static Map<String, RedisMessage> asMap(RedisMessage message) {
        assertInstanceOf(MapRedisMessage.class, message);
        Map<String, RedisMessage> result = new LinkedHashMap<>();
        ((MapRedisMessage) message).children().forEach((key, value) -> result.put(text(key), value));
        return result;
    }

    @Test
    void shouldRenderTenFieldsInOrder() {
        // Behavior: the entry is name, arity, flags, first key, last key, step, categories, tips, key specs, subcommands
        KeySpec keySpec = keySpec(null, List.of(KeySpecFlag.RO, KeySpecFlag.ACCESS), 1, 0, 1);
        CommandMetadata metadata = metadata(List.of(CommandFlag.READONLY, CommandFlag.FAST), List.of(AclCategory.STRING),
                List.of("REQUEST_POLICY:ALL_SHARDS"), List.of(keySpec), Map.of());

        List<RedisMessage> entry = entry("GET", metadata);

        assertEquals("get", text(entry.get(0)));
        assertEquals(-3, integer(entry.get(1)));
        assertEquals(List.of("readonly", "fast"), statuses(entry.get(2)));
        assertEquals(1, integer(entry.get(3)));
        assertEquals(1, integer(entry.get(4)));
        assertEquals(1, integer(entry.get(5)));
        assertEquals(List.of("@read", "@string", "@fast"), statuses(entry.get(6)));
        assertInstanceOf(SetRedisMessage.class, entry.get(7));
        assertEquals("REQUEST_POLICY:ALL_SHARDS", text(((SetRedisMessage) entry.get(7)).children().iterator().next()));
        assertInstanceOf(SetRedisMessage.class, entry.get(8));
        assertEquals(1, ((SetRedisMessage) entry.get(8)).children().size());
        assertInstanceOf(SetRedisMessage.class, entry.get(9));
        assertTrue(((SetRedisMessage) entry.get(9)).children().isEmpty());
    }

    @Test
    void shouldHideInternalFlags() {
        // Behavior: flags Redis hides from COMMAND INFO are not written
        CommandMetadata metadata = metadata(List.of(CommandFlag.FAST, CommandFlag.SENTINEL, CommandFlag.NO_AUTH), List.of(), List.of(), List.of(), Map.of());

        assertEquals(List.of("fast", "no_auth"), statuses(entry("PING", metadata).get(2)));
    }

    @Test
    void shouldDeriveAclCategoriesFromFlags() {
        // Behavior: read, write, fast, slow, admin, dangerous and blocking are derived from command flags
        assertEquals(List.of("@read", "@fast", "@connection"),
                statuses(entry("PING", metadata(List.of(CommandFlag.READONLY, CommandFlag.FAST), List.of(AclCategory.CONNECTION), List.of(), List.of(), Map.of())).get(6)));
        assertEquals(List.of("@write", "@slow"),
                statuses(entry("SET", metadata(List.of(CommandFlag.WRITE), List.of(), List.of(), List.of(), Map.of())).get(6)));
        assertEquals(List.of("@admin", "@slow", "@dangerous"),
                statuses(entry("KR.ADMIN", metadata(List.of(CommandFlag.ADMIN), List.of(), List.of(), List.of(), Map.of())).get(6)));
        assertEquals(List.of("@slow", "@blocking"),
                statuses(entry("ZWATCH", metadata(List.of(CommandFlag.BLOCKING), List.of(), List.of(), List.of(), Map.of())).get(6)));
    }

    @Test
    void shouldComputeLegacyKeyRangeFromSingleSpec() {
        // Behavior: one key spec maps to first key, absolute last key and step
        List<RedisMessage> single = entry("SET", metadata(List.of(), List.of(), List.of(),
                List.of(keySpec(null, List.of(KeySpecFlag.RW), 1, 0, 1)), Map.of()));
        assertEquals(1, integer(single.get(3)));
        assertEquals(1, integer(single.get(4)));
        assertEquals(1, integer(single.get(5)));

        List<RedisMessage> open = entry("MSET", metadata(List.of(), List.of(), List.of(),
                List.of(keySpec(null, List.of(KeySpecFlag.OW), 1, -1, 2)), Map.of()));
        assertEquals(1, integer(open.get(3)));
        assertEquals(-1, integer(open.get(4)));
        assertEquals(2, integer(open.get(5)));
        assertFalse(statuses(open.get(2)).contains("movablekeys"));
    }

    @Test
    void shouldMergeConsecutiveKeySpecs() {
        // Behavior: consecutive step-1 specs merge into one range without the movablekeys flag
        List<RedisMessage> entry = entry("RENAME", metadata(List.of(CommandFlag.WRITE), List.of(), List.of(),
                List.of(keySpec(null, List.of(KeySpecFlag.RW), 1, 0, 1), keySpec(null, List.of(KeySpecFlag.OW), 2, 0, 1)), Map.of()));

        assertEquals(1, integer(entry.get(3)));
        assertEquals(2, integer(entry.get(4)));
        assertEquals(1, integer(entry.get(5)));
        assertEquals(List.of("write"), statuses(entry.get(2)));
    }

    @Test
    void shouldFlagMovableKeysForNonMergeableSpecs() {
        // Behavior: a spec that cannot merge is skipped, the mergeable ones still give a range and movablekeys is set
        List<RedisMessage> entry = entry("X", metadata(List.of(CommandFlag.WRITE), List.of(), List.of(),
                List.of(keySpec(null, List.of(KeySpecFlag.RW), 1, 0, 1), keySpec(null, List.of(KeySpecFlag.OW), 3, 0, 2)), Map.of()));

        assertEquals(1, integer(entry.get(3)));
        assertEquals(1, integer(entry.get(4)));
        assertEquals(1, integer(entry.get(5)));
        assertEquals(List.of("write", "movablekeys"), statuses(entry.get(2)));
    }

    @Test
    void shouldFlagMovableKeysWhenNoSpecIsMergeable() {
        // Behavior: when every spec is skipped the range is zeros and movablekeys is set
        List<RedisMessage> entry = entry("X", metadata(List.of(CommandFlag.WRITE), List.of(), List.of(),
                List.of(keySpec(null, List.of(KeySpecFlag.RW), 1, 0, 2), keySpec(null, List.of(KeySpecFlag.OW), 3, 0, 2)), Map.of()));

        assertEquals(0, integer(entry.get(3)));
        assertEquals(0, integer(entry.get(4)));
        assertEquals(0, integer(entry.get(5)));
        assertEquals(List.of("write", "movablekeys"), statuses(entry.get(2)));
    }

    @Test
    void shouldFlagMovableKeysForIncompleteSingleSpec() {
        // Behavior: a single incomplete spec keeps its range but adds the movablekeys flag
        List<RedisMessage> entry = entry("X", metadata(List.of(CommandFlag.WRITE), List.of(), List.of(),
                List.of(keySpec(null, List.of(KeySpecFlag.RW, KeySpecFlag.INCOMPLETE), 1, 0, 1)), Map.of()));

        assertEquals(1, integer(entry.get(3)));
        assertEquals(1, integer(entry.get(4)));
        assertEquals(1, integer(entry.get(5)));
        assertEquals(List.of("write", "movablekeys"), statuses(entry.get(2)));
    }

    @Test
    void shouldOrderFlagsLikeRedis() {
        // Behavior: flags are written in the fixed Redis order, not in definition order
        CommandMetadata metadata = metadata(List.of(CommandFlag.FAST, CommandFlag.DENYOOM, CommandFlag.READONLY),
                List.of(), List.of(), List.of(), Map.of());

        assertEquals(List.of("readonly", "denyoom", "fast"), statuses(entry("X", metadata).get(2)));
    }

    @Test
    void shouldOrderAclCategoriesLikeRedis() {
        // Behavior: explicit and derived categories are merged and written in the fixed Redis order
        assertEquals(List.of("@read", "@string", "@fast"),
                statuses(entry("GET", metadata(List.of(CommandFlag.READONLY, CommandFlag.FAST), List.of(AclCategory.STRING), List.of(), List.of(), Map.of())).get(6)));
        assertEquals(List.of("@fast", "@connection"),
                statuses(entry("PING", metadata(List.of(CommandFlag.FAST), List.of(AclCategory.CONNECTION), List.of(), List.of(), Map.of())).get(6)));
        assertEquals(List.of("@read", "@slow", "@bucket"),
                statuses(entry("BUCKET.QUERY", metadata(List.of(CommandFlag.READONLY), List.of(AclCategory.BUCKET), List.of(), List.of(), Map.of())).get(6)));
    }

    @Test
    void shouldOrderKeySpecFlagsLikeRedis() {
        // Behavior: key spec flags are written in the fixed Redis order
        KeySpec keySpec = keySpec(null, List.of(KeySpecFlag.ACCESS, KeySpecFlag.RW), 1, 0, 1);
        List<RedisMessage> entry = entry("SET", metadata(List.of(), List.of(), List.of(), List.of(keySpec), Map.of()));

        Map<String, RedisMessage> spec = asMap(((SetRedisMessage) entry.get(8)).children().iterator().next());
        assertEquals(List.of("RW", "access"), statuses(spec.get("flags")));
    }

    @Test
    void shouldRenderKeywordAndKeynumSpecs() {
        // Behavior: keyword begin_search and keynum find_keys use the Redis map layout
        KeySpec keySpec = new KeySpec(null, List.of(KeySpecFlag.RO), new BeginSearch(new Keyword("KEYS", -2)),
                new FindKeys(new KeyNum(0, 1, 1)));
        List<RedisMessage> entry = entry("X", metadata(List.of(), List.of(), List.of(), List.of(keySpec), Map.of()));

        Map<String, RedisMessage> spec = asMap(((SetRedisMessage) entry.get(8)).children().iterator().next());
        Map<String, RedisMessage> beginSearch = asMap(spec.get("begin_search"));
        assertEquals("keyword", text(beginSearch.get("type")));
        Map<String, RedisMessage> keyword = asMap(beginSearch.get("spec"));
        assertEquals(List.of("keyword", "startfrom"), List.copyOf(keyword.keySet()));
        assertEquals("KEYS", text(keyword.get("keyword")));
        assertEquals(-2, integer(keyword.get("startfrom")));

        Map<String, RedisMessage> findKeys = asMap(spec.get("find_keys"));
        assertEquals("keynum", text(findKeys.get("type")));
        Map<String, RedisMessage> keynum = asMap(findKeys.get("spec"));
        assertEquals(List.of("keynumidx", "firstkey", "keystep"), List.copyOf(keynum.keySet()));
        assertEquals(0, integer(keynum.get("keynumidx")));
        assertEquals(1, integer(keynum.get("firstkey")));
        assertEquals(1, integer(keynum.get("keystep")));
        assertEquals(List.of("movablekeys"), statuses(entry.get(2)));
    }

    @Test
    void shouldRenderUnknownSpecs() {
        // Behavior: an unknown begin_search or find_keys is a map with type unknown and an empty spec
        KeySpec keySpec = new KeySpec(null, List.of(KeySpecFlag.RO), new BeginSearch(null, null), new FindKeys(null, null));
        List<RedisMessage> entry = entry("X", metadata(List.of(), List.of(), List.of(), List.of(keySpec), Map.of()));

        Map<String, RedisMessage> spec = asMap(((SetRedisMessage) entry.get(8)).children().iterator().next());
        Map<String, RedisMessage> beginSearch = asMap(spec.get("begin_search"));
        assertEquals("unknown", text(beginSearch.get("type")));
        assertTrue(asMap(beginSearch.get("spec")).isEmpty());
        Map<String, RedisMessage> findKeys = asMap(spec.get("find_keys"));
        assertEquals("unknown", text(findKeys.get("type")));
        assertTrue(asMap(findKeys.get("spec")).isEmpty());
    }

    @Test
    void shouldReportNoKeysWithoutKeySpecs() {
        // Behavior: a command without key specs reports zero for first key, last key and step
        List<RedisMessage> entry = entry("PING", metadata(List.of(CommandFlag.FAST), List.of(), List.of(), List.of(), Map.of()));

        assertEquals(0, integer(entry.get(3)));
        assertEquals(0, integer(entry.get(4)));
        assertEquals(0, integer(entry.get(5)));
        assertEquals(List.of("fast"), statuses(entry.get(2)));
        assertTrue(((SetRedisMessage) entry.get(8)).children().isEmpty());
    }

    @Test
    void shouldRenderKeySpecMaps() {
        // Behavior: a key spec is a map with notes, flags, begin_search and find_keys in Redis form
        KeySpec keySpec = keySpec("RW and ACCESS", List.of(KeySpecFlag.RW, KeySpecFlag.ACCESS, KeySpecFlag.VARIABLE_FLAGS), 1, 0, 1);
        List<RedisMessage> entry = entry("SET", metadata(List.of(), List.of(), List.of(), List.of(keySpec), Map.of()));

        Map<String, RedisMessage> spec = asMap(((SetRedisMessage) entry.get(8)).children().iterator().next());
        assertEquals(List.of("notes", "flags", "begin_search", "find_keys"), List.copyOf(spec.keySet()));
        assertEquals("RW and ACCESS", text(spec.get("notes")));
        assertEquals(List.of("RW", "access", "variable_flags"), statuses(spec.get("flags")));

        Map<String, RedisMessage> beginSearch = asMap(spec.get("begin_search"));
        assertEquals("index", text(beginSearch.get("type")));
        assertEquals(1, integer(asMap(beginSearch.get("spec")).get("index")));

        Map<String, RedisMessage> findKeys = asMap(spec.get("find_keys"));
        assertEquals("range", text(findKeys.get("type")));
        Map<String, RedisMessage> range = asMap(findKeys.get("spec"));
        assertEquals(List.of("lastkey", "keystep", "limit"), List.copyOf(range.keySet()));
        assertEquals(0, integer(range.get("lastkey")));
        assertEquals(1, integer(range.get("keystep")));
        assertEquals(0, integer(range.get("limit")));
    }

    @Test
    void shouldNestSubcommandsAsInfoEntries() {
        // Behavior: subcommands are nested entries of the same shape named "container|subcommand"
        CommandMetadata describe = metadata(List.of(CommandFlag.ADMIN), List.of(), List.of(), List.of(), Map.of());
        CommandMetadata parent = metadata(List.of(CommandFlag.ADMIN), List.of(), List.of(), List.of(), Map.of("DESCRIBE-SHARD", describe));

        List<RedisMessage> subcommands = ((ArrayRedisMessage) entry("KR.ADMIN", parent).get(9)).children();
        assertEquals(1, subcommands.size());
        List<RedisMessage> sub = ((ArrayRedisMessage) subcommands.getFirst()).children();
        assertEquals(10, sub.size());
        assertEquals("kr.admin|describe-shard", text(sub.get(0)));
        assertInstanceOf(SetRedisMessage.class, sub.get(9));
        assertTrue(((SetRedisMessage) sub.get(9)).children().isEmpty());
    }
}
