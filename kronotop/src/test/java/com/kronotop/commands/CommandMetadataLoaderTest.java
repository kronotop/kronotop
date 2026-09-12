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

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CommandMetadataLoaderTest {

    @Test
    void shouldLoadTopLevelCommands() {
        // Behavior: every file under the directory is parsed and keyed by uppercase command name
        Map<String, CommandMetadata> commands = CommandMetadataLoader.load("test-commands");

        assertEquals(List.of("CLIENT", "PING", "SET"), List.copyOf(commands.keySet()));

        CommandMetadata ping = commands.get("PING");
        assertEquals("Returns the server's liveliness response", ping.summary());
        assertEquals(CommandGroup.CONNECTION, ping.group());
        assertEquals("2026.06-1", ping.since());
        assertEquals(-1, ping.arity());
        assertEquals(List.of(CommandFlag.FAST), ping.commandFlags());
        assertEquals(1, ping.arguments().size());
        assertTrue(ping.arguments().getFirst().optional());
        assertNull(ping.arguments().getFirst().keySpecIndex());
        assertTrue(ping.keySpecs().isEmpty());
        assertTrue(ping.subcommands().isEmpty());
    }

    @Test
    void shouldParseKeySpecsAndArgumentFlags() {
        // Behavior: key specs, display, multiple_token and nested arguments are read, function fields are ignored
        CommandMetadata set = CommandMetadataLoader.load("test-commands").get("SET");

        assertEquals(1, set.keySpecs().size());
        KeySpec keySpec = set.keySpecs().getFirst();
        assertEquals(List.of(KeySpecFlag.RW, KeySpecFlag.ACCESS, KeySpecFlag.UPDATE, KeySpecFlag.VARIABLE_FLAGS), keySpec.flags());
        assertEquals(1, keySpec.beginSearch().index().pos());
        assertEquals(0, keySpec.findKeys().range().lastkey());
        assertEquals(1, keySpec.findKeys().range().step());

        List<Argument> arguments = set.arguments();
        assertEquals(4, arguments.size());
        assertEquals(0, arguments.get(0).keySpecIndex());
        assertEquals("val", arguments.get(1).display());
        assertEquals("oneof", arguments.get(2).type());
        assertEquals(2, arguments.get(2).arguments().size());
        assertEquals("NX", arguments.get(2).arguments().getFirst().token());
        assertTrue(arguments.get(3).multiple());
        assertTrue(arguments.get(3).multipleToken());
        assertEquals(List.of(List.of("2026.06-1", "Initial version.")), set.history());
    }

    @Test
    void shouldLoadReplySchemaAsNestedMap() {
        // Behavior: reply_schema is read as a nested map in definition order; a command without one has an empty map
        Map<String, CommandMetadata> commands = CommandMetadataLoader.load("test-commands");

        assertEquals(Map.of("type", "string"), commands.get("SET").replySchema());
        assertTrue(commands.get("PING").replySchema().isEmpty());

        Map<String, Object> query = CommandMetadataLoader.load().get("BUCKET.QUERY").replySchema();
        assertEquals("object", query.get("type"));
        assertEquals(Boolean.FALSE, query.get("additionalProperties"));
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) query.get("properties");
        assertEquals(List.of("cursor_id", "entries"), List.copyOf(properties.keySet()));
    }

    @Test
    void shouldLinkSubcommandsToContainer() {
        // Behavior: a definition with a container field is attached to the parent command
        Map<String, CommandMetadata> commands = CommandMetadataLoader.load("test-commands");

        CommandMetadata client = commands.get("CLIENT");
        assertEquals(1, client.subcommands().size());
        CommandMetadata setname = client.subcommands().get("SETNAME");
        assertNotNull(setname);
        assertEquals("CLIENT", setname.container());
        assertEquals(3, setname.arity());
        assertEquals("connection-name", setname.arguments().getFirst().name());
        assertFalse(commands.containsKey("SETNAME"));
    }

    @Test
    void shouldReturnEmptyMapForMissingDirectory() {
        // Behavior: a directory that does not exist on the classpath yields no definitions
        assertTrue(CommandMetadataLoader.load("test-commands-does-not-exist").isEmpty());
    }

    @Test
    void shouldFailOnUnknownCommandName() {
        // Behavior: a top-level name that is not in CommandType is rejected
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> CommandMetadataLoader.load("test-commands-unknown-command"));
        assertTrue(e.getMessage().contains("unknown command 'FOO'"));
    }

    @Test
    void shouldFailOnUnknownContainer() {
        // Behavior: a subcommand whose container was not loaded is rejected
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> CommandMetadataLoader.load("test-commands-unknown-container"));
        assertTrue(e.getMessage().contains("unknown container 'NOPE'"));
    }

    @Test
    void shouldFailOnUnknownGroup() {
        // Behavior: a group value outside the fixed list is rejected
        assertThrows(IllegalStateException.class, () -> CommandMetadataLoader.load("test-commands-unknown-group"));
    }

    @Test
    void shouldFailOnUnknownField() {
        // Behavior: a misspelled field is rejected instead of being dropped
        assertThrows(IllegalStateException.class, () -> CommandMetadataLoader.load("test-commands-unknown-field"));
    }

    @Test
    void shouldFailOnUnknownCommandFlag() {
        // Behavior: a command flag outside the fixed list is rejected
        assertThrows(IllegalStateException.class, () -> CommandMetadataLoader.load("test-commands-unknown-flag"));
    }

    @Test
    void shouldFailOnUnknownAclCategory() {
        // Behavior: an ACL category outside the fixed list is rejected
        assertThrows(IllegalStateException.class, () -> CommandMetadataLoader.load("test-commands-unknown-category"));
    }

    @Test
    void shouldFailOnUnknownKeySpecFlag() {
        // Behavior: a key spec flag outside the fixed list is rejected
        assertThrows(IllegalStateException.class, () -> CommandMetadataLoader.load("test-commands-unknown-keyspec-flag"));
    }

    @Test
    void shouldRejectZeroKeyStep() {
        // Behavior: a range step below 1 is rejected at load time
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> CommandMetadataLoader.load("test-commands-invalid-step"));
        assertTrue(e.getMessage().contains("step"));
    }

    @Test
    void shouldRejectKeySpecPosBelowOne() {
        // Behavior: an index pos below 1 is rejected at load time
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> CommandMetadataLoader.load("test-commands-invalid-pos"));
        assertTrue(e.getMessage().contains("pos"));
    }

    @Test
    void shouldRejectKeySpecWithoutFindKeys() {
        // Behavior: a key spec without find_keys is rejected at load time
        IllegalStateException e = assertThrows(IllegalStateException.class,
                () -> CommandMetadataLoader.load("test-commands-missing-find-keys"));
        assertTrue(e.getMessage().contains("find_keys"));
    }

    @Test
    void shouldLoadKeywordAndKeynumKeySpecs() {
        // Behavior: keyword begin_search and keynum find_keys are read with their fields
        CommandMetadata scan = CommandMetadataLoader.load("test-commands-keyspecs").get("SCAN");

        assertEquals(2, scan.keySpecs().size());
        KeySpec keyword = scan.keySpecs().get(0);
        assertEquals(BeginSearch.Type.KEYWORD, keyword.beginSearch().type());
        assertEquals("KEYS", keyword.beginSearch().keyword().keyword());
        assertEquals(-2, keyword.beginSearch().keyword().startfrom());
        assertEquals(FindKeys.Type.RANGE, keyword.findKeys().type());
        assertEquals(-1, keyword.findKeys().range().lastkey());

        KeySpec keynum = scan.keySpecs().get(1);
        assertEquals(BeginSearch.Type.INDEX, keynum.beginSearch().type());
        assertEquals(FindKeys.Type.KEYNUM, keynum.findKeys().type());
        assertEquals(0, keynum.findKeys().keynum().keynumidx());
        assertEquals(1, keynum.findKeys().keynum().firstkey());
        assertEquals(1, keynum.findKeys().keynum().step());
    }

    @Test
    void shouldLoadUnknownKeySpec() {
        // Behavior: "unknown": null for begin_search or find_keys gives the unknown type
        CommandMetadata hkeys = CommandMetadataLoader.load("test-commands-keyspecs").get("HKEYS");

        KeySpec keySpec = hkeys.keySpecs().getFirst();
        assertEquals(BeginSearch.Type.UNKNOWN, keySpec.beginSearch().type());
        assertEquals(FindKeys.Type.UNKNOWN, keySpec.findKeys().type());
    }

    @Test
    void shouldLoadBundledDefinitions() {
        // Behavior: the definitions shipped under commands/ parse, include BUCKET.QUERY and ZSET with its key spec
        Map<String, CommandMetadata> commands = CommandMetadataLoader.load();

        CommandMetadata query = commands.get("BUCKET.QUERY");
        assertNotNull(query);
        assertEquals(CommandGroup.BUCKET, query.group());
        assertEquals(-3, query.arity());
        assertEquals(8, query.arguments().size());
        assertEquals("SORTBY", query.arguments().get(2).token());

        CommandMetadata zset = commands.get("ZSET");
        assertNotNull(zset);
        assertEquals(CommandGroup.ZMAP, zset.group());
        assertEquals(3, zset.arity());
        assertTrue(zset.keySpecs().getFirst().isIndexRange());
    }
}
