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

import com.kronotop.BaseHandlerTest;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CommandHandlerTest extends BaseHandlerTest {

    private Object run(EmbeddedChannel channel, String... args) {
        StringBuilder resp = new StringBuilder("*").append(args.length).append("\r\n");
        for (String arg : args) {
            resp.append("$").append(arg.length()).append("\r\n").append(arg).append("\r\n");
        }
        ByteBuf buf = Unpooled.buffer();
        buf.writeBytes(resp.toString().getBytes(StandardCharsets.US_ASCII));
        return runCommand(channel, buf);
    }

    private static String text(RedisMessage message) {
        assertInstanceOf(FullBulkStringRedisMessage.class, message);
        return ((FullBulkStringRedisMessage) message).content().toString(StandardCharsets.UTF_8);
    }

    private static Map<String, RedisMessage> asMap(RedisMessage message) {
        assertInstanceOf(MapRedisMessage.class, message);
        Map<String, RedisMessage> result = new LinkedHashMap<>();
        ((MapRedisMessage) message).children().forEach((key, value) -> result.put(text(key), value));
        return result;
    }

    private Map<String, RedisMessage> docs(EmbeddedChannel channel, String... names) {
        switchProtocol(RESPVersion.RESP3);
        String[] args = new String[names.length + 2];
        args[0] = "COMMAND";
        args[1] = "DOCS";
        System.arraycopy(names, 0, args, 2, names.length);
        Object response = run(channel, args);
        assertInstanceOf(MapRedisMessage.class, response);
        return asMap((RedisMessage) response);
    }

    @Test
    void shouldReturnDocsForAllCommands() {
        // Behavior: COMMAND DOCS without names returns one entry per loaded command definition
        Map<String, RedisMessage> docs = docs(getChannel());

        assertEquals(instance.getContext().getCommandMetadata().size(), docs.size());
        assertTrue(docs.containsKey("bucket.query"));
        assertTrue(docs.containsKey("set"));
    }

    @Test
    void shouldReturnDocsForRequestedCommands() {
        // Behavior: COMMAND DOCS with a name returns only that command with its arguments
        Map<String, RedisMessage> docs = docs(getChannel(), "bucket.query");

        assertEquals(List.of("bucket.query"), List.copyOf(docs.keySet()));
        Map<String, RedisMessage> command = asMap(docs.get("bucket.query"));
        assertEquals("bucket", text(command.get("group")));
        List<RedisMessage> arguments = ((ArrayRedisMessage) command.get("arguments")).children();
        assertEquals(8, arguments.size());
        Map<String, RedisMessage> sortby = asMap(arguments.get(2));
        assertEquals("SORTBY", text(sortby.get("token")));
        SetRedisMessage flags = (SetRedisMessage) sortby.get("flags");
        assertEquals(1, flags.children().size());
        assertEquals("optional", ((SimpleStringRedisMessage) flags.children().iterator().next()).content());
    }

    @Test
    void shouldMatchNamesCaseInsensitively() {
        // Behavior: command names are matched without regard to case
        Map<String, RedisMessage> upper = docs(getChannel(), "BUCKET.QUERY");
        Map<String, RedisMessage> lower = docs(getChannel(), "bucket.query");

        assertEquals(List.of("bucket.query"), List.copyOf(upper.keySet()));
        assertEquals(upper.keySet(), lower.keySet());
    }

    @Test
    void shouldSkipUnknownCommandNames() {
        // Behavior: names without a definition are skipped, known names are still returned
        Map<String, RedisMessage> docs = docs(getChannel(), "nope", "ping");

        assertEquals(List.of("ping"), List.copyOf(docs.keySet()));
    }

    @Test
    void shouldReturnEmptyMapWhenNoNameMatches() {
        // Behavior: an all-unknown name list yields an empty map, not an error
        assertTrue(docs(getChannel(), "nope").isEmpty());
    }

    @Test
    void shouldDowngradeToFlatArrayInRESP2() {
        // Behavior: on RESP2 the map is flattened to an array of alternating keys and values
        EmbeddedChannel channel = getChannel();
        switchProtocol(RESPVersion.RESP2);

        Object response = run(channel, "COMMAND", "DOCS", "ping");
        assertInstanceOf(ArrayRedisMessage.class, response);
        List<RedisMessage> root = ((ArrayRedisMessage) response).children();
        assertEquals(2, root.size());
        assertEquals("ping", text(root.get(0)));

        assertInstanceOf(ArrayRedisMessage.class, root.get(1));
        List<RedisMessage> command = ((ArrayRedisMessage) root.get(1)).children();
        assertEquals(0, command.size() % 2);
        int argumentsIndex = -1;
        for (int i = 0; i < command.size(); i += 2) {
            if (text(command.get(i)).equals("arguments")) {
                argumentsIndex = i + 1;
            }
        }
        assertTrue(argumentsIndex > 0);
        List<RedisMessage> argument = ((ArrayRedisMessage) ((ArrayRedisMessage) command.get(argumentsIndex)).children().getFirst()).children();
        RedisMessage flags = argument.get(argument.size() - 1);
        assertInstanceOf(ArrayRedisMessage.class, flags);
        RedisMessage flag = ((ArrayRedisMessage) flags).children().getFirst();
        assertInstanceOf(SimpleStringRedisMessage.class, flag);
        assertEquals("optional", ((SimpleStringRedisMessage) flag).content());
    }

    private List<RedisMessage> infoEntries(Object response) {
        assertInstanceOf(ArrayRedisMessage.class, response);
        return ((ArrayRedisMessage) response).children();
    }

    private static long integer(RedisMessage message) {
        assertInstanceOf(IntegerRedisMessage.class, message);
        return ((IntegerRedisMessage) message).value();
    }

    @Test
    void shouldReturnInfoForAllCommandsWithoutSubcommand() {
        // Behavior: COMMAND without a subcommand returns a ten-field entry per loaded definition
        switchProtocol(RESPVersion.RESP3);
        List<RedisMessage> entries = infoEntries(run(getChannel(), "COMMAND"));

        assertEquals(instance.getContext().getCommandMetadata().size(), entries.size());
        for (RedisMessage entry : entries) {
            assertEquals(10, ((ArrayRedisMessage) entry).children().size());
        }
    }

    @Test
    void shouldReturnInfoForAllCommandsWhenNoNameGiven() {
        // Behavior: COMMAND INFO without names returns the same list as COMMAND
        switchProtocol(RESPVersion.RESP3);
        List<RedisMessage> all = infoEntries(run(getChannel(), "COMMAND"));
        List<RedisMessage> info = infoEntries(run(getChannel(), "COMMAND", "INFO"));

        assertEquals(all.size(), info.size());
    }

    @Test
    void shouldReturnInfoForRequestedCommands() {
        // Behavior: COMMAND INFO with names returns entries in request order with arity and key range
        switchProtocol(RESPVersion.RESP3);
        List<RedisMessage> entries = infoEntries(run(getChannel(), "COMMAND", "INFO", "ping", "set"));

        assertEquals(2, entries.size());
        List<RedisMessage> ping = ((ArrayRedisMessage) entries.get(0)).children();
        assertEquals("ping", text(ping.get(0)));
        assertEquals(-1, integer(ping.get(1)));
        List<RedisMessage> set = ((ArrayRedisMessage) entries.get(1)).children();
        assertEquals("set", text(set.get(0)));
        assertEquals(1, integer(set.get(3)));
        assertEquals(1, integer(set.get(4)));
        assertEquals(1, integer(set.get(5)));
    }

    @Test
    void shouldReturnNullForUnknownCommandInInfo() {
        // Behavior: an unknown name yields a null entry, known names are still returned in place
        switchProtocol(RESPVersion.RESP3);
        List<RedisMessage> entries = infoEntries(run(getChannel(), "COMMAND", "INFO", "nope", "ping"));

        assertEquals(2, entries.size());
        assertInstanceOf(NullRedisMessage.class, entries.get(0));
        assertEquals("ping", text(((ArrayRedisMessage) entries.get(1)).children().get(0)));
    }

    @Test
    void shouldReturnCommandCount() {
        // Behavior: COMMAND COUNT returns the number of loaded command definitions
        Object response = run(getChannel(), "COMMAND", "COUNT");

        assertEquals(instance.getContext().getCommandMetadata().size(), integer((RedisMessage) response));
    }

    @Test
    void shouldDowngradeInfoSetsToArraysInRESP2() {
        // Behavior: on RESP2 the flag and category sets arrive as arrays of simple strings
        switchProtocol(RESPVersion.RESP2);
        List<RedisMessage> entries = infoEntries(run(getChannel(), "COMMAND", "INFO", "ping"));

        List<RedisMessage> ping = ((ArrayRedisMessage) entries.getFirst()).children();
        assertInstanceOf(ArrayRedisMessage.class, ping.get(2));
        RedisMessage flag = ((ArrayRedisMessage) ping.get(2)).children().getFirst();
        assertInstanceOf(SimpleStringRedisMessage.class, flag);
        assertEquals("fast", ((SimpleStringRedisMessage) flag).content());
        assertInstanceOf(ArrayRedisMessage.class, ping.get(6));
        List<RedisMessage> categories = ((ArrayRedisMessage) ping.get(6)).children();
        assertEquals("@fast", ((SimpleStringRedisMessage) categories.get(0)).content());
        assertEquals("@connection", ((SimpleStringRedisMessage) categories.get(1)).content());
    }

    @Test
    void shouldReturnEmptySetForSubcommandsInInfo() {
        // Behavior: a command without subcommands has an empty set in the tenth field
        switchProtocol(RESPVersion.RESP3);
        List<RedisMessage> entries = infoEntries(run(getChannel(), "COMMAND", "INFO", "ping"));

        List<RedisMessage> ping = ((ArrayRedisMessage) entries.getFirst()).children();
        assertInstanceOf(SetRedisMessage.class, ping.get(9));
        assertTrue(((SetRedisMessage) ping.get(9)).children().isEmpty());
    }

    @Test
    void shouldReturnNullForPipeNameWithoutSubcommands() {
        // Behavior: "name|sub" for a command that has no such subcommand gives a null entry
        switchProtocol(RESPVersion.RESP3);
        List<RedisMessage> entries = infoEntries(run(getChannel(), "COMMAND", "INFO", "ping|x"));

        assertEquals(1, entries.size());
        assertInstanceOf(NullRedisMessage.class, entries.getFirst());
    }

    private List<String> names(Object response) {
        assertInstanceOf(ArrayRedisMessage.class, response);
        return ((ArrayRedisMessage) response).children().stream().map(CommandHandlerTest::text).toList();
    }

    @Test
    void shouldListAllCommandNames() {
        // Behavior: COMMAND LIST returns every command name, subcommands included, as lowercase bulk strings
        List<String> names = names(run(getChannel(), "COMMAND", "LIST"));

        int expected = 0;
        for (var metadata : instance.getContext().getCommandMetadata().values()) {
            expected += 1 + metadata.subcommands().size();
        }
        assertEquals(expected, names.size());
        assertTrue(names.contains("bucket.query"));
        assertTrue(names.contains("ping"));
    }

    @Test
    void shouldListByPattern() {
        // Behavior: FILTERBY PATTERN keeps the names that match the glob pattern, without regard to case
        assertEquals(List.of("bucket.query"), names(run(getChannel(), "COMMAND", "LIST", "FILTERBY", "PATTERN", "BUCKET.*")));
    }

    @Test
    void shouldListByAclCategory() {
        // Behavior: FILTERBY ACLCAT keeps the commands in that category, explicit or derived, unknown category gives nothing
        List<String> connection = names(run(getChannel(), "COMMAND", "LIST", "filterby", "aclcat", "connection"));
        assertTrue(connection.contains("auth"));
        assertTrue(connection.contains("ping"));
        assertFalse(connection.contains("get"));

        List<String> read = names(run(getChannel(), "COMMAND", "LIST", "FILTERBY", "ACLCAT", "read"));
        assertTrue(read.contains("get"));
        assertTrue(read.contains("bucket.query"));
        assertFalse(read.contains("set"));

        assertTrue(names(run(getChannel(), "COMMAND", "LIST", "FILTERBY", "ACLCAT", "nope")).isEmpty());
    }

    @Test
    void shouldReturnEmptyListForModuleFilter() {
        // Behavior: there are no modules, FILTERBY MODULE gives an empty array
        assertTrue(names(run(getChannel(), "COMMAND", "LIST", "FILTERBY", "MODULE", "x")).isEmpty());
    }

    @Test
    void shouldRejectBadListFilter() {
        // Behavior: an unknown filter type or a wrong argument count is a syntax error
        for (String[] args : new String[][]{
                {"COMMAND", "LIST", "FILTERBY", "FOO", "x"},
                {"COMMAND", "LIST", "FILTERBY", "PATTERN"},
                {"COMMAND", "LIST", "x"}}) {
            Object response = run(getChannel(), args);
            assertInstanceOf(ErrorRedisMessage.class, response);
            assertEquals("ERR syntax error", ((ErrorRedisMessage) response).content());
        }
    }

    @Test
    void shouldReturnKeysForGetKeys() {
        // Behavior: COMMAND GETKEYS returns the key arguments as the client sent them
        assertEquals(List.of("Foo"), names(run(getChannel(), "COMMAND", "GETKEYS", "set", "Foo", "bar")));
        assertEquals(List.of("k"), names(run(getChannel(), "COMMAND", "GETKEYS", "GET", "k")));
    }

    @Test
    void shouldReturnKeysAndFlagsForGetKeysAndFlags() {
        // Behavior: COMMAND GETKEYSANDFLAGS pairs each key with the flags of its key spec
        switchProtocol(RESPVersion.RESP3);
        Object response = run(getChannel(), "COMMAND", "GETKEYSANDFLAGS", "get", "foo");

        List<RedisMessage> entries = ((ArrayRedisMessage) response).children();
        assertEquals(1, entries.size());
        List<RedisMessage> pair = ((ArrayRedisMessage) entries.getFirst()).children();
        assertEquals("foo", text(pair.get(0)));
        assertInstanceOf(SetRedisMessage.class, pair.get(1));
        List<String> flags = ((SetRedisMessage) pair.get(1)).children().stream()
                .map(flag -> ((SimpleStringRedisMessage) flag).content()).toList();
        assertEquals(List.of("RO", "access"), flags);
    }

    @Test
    void shouldRejectGetKeysForUnknownCommand() {
        // Behavior: an unknown command name is rejected
        Object response = run(getChannel(), "COMMAND", "GETKEYS", "nope", "a");

        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals("ERR Invalid command specified", ((ErrorRedisMessage) response).content());
    }

    @Test
    void shouldRejectGetKeysForCommandWithoutKeys() {
        // Behavior: a command without key specs is rejected
        Object response = run(getChannel(), "COMMAND", "GETKEYS", "ping", "a");

        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals("ERR The command has no key arguments", ((ErrorRedisMessage) response).content());
    }

    @Test
    void shouldRejectGetKeysWithWrongArity() {
        // Behavior: an argument count that does not fit the command arity is rejected
        Object response = run(getChannel(), "COMMAND", "GETKEYS", "get", "a", "b");

        assertInstanceOf(ErrorRedisMessage.class, response);
        assertEquals("ERR Invalid number of arguments specified for command", ((ErrorRedisMessage) response).content());
    }

    @Test
    void shouldReturnHelpLines() {
        // Behavior: COMMAND HELP returns simple strings with a header line and a HELP footer
        Object response = run(getChannel(), "COMMAND", "HELP");

        assertInstanceOf(ArrayRedisMessage.class, response);
        List<RedisMessage> lines = ((ArrayRedisMessage) response).children();
        for (RedisMessage line : lines) {
            assertInstanceOf(SimpleStringRedisMessage.class, line);
        }
        assertEquals("COMMAND <subcommand> [<arg> [value] [opt] ...]. Subcommands are:", ((SimpleStringRedisMessage) lines.getFirst()).content());
        assertEquals("HELP", ((SimpleStringRedisMessage) lines.get(lines.size() - 2)).content());
        assertEquals("    Print this help.", ((SimpleStringRedisMessage) lines.getLast()).content());
    }

    @Test
    void shouldRejectWrongArityForSubcommand() {
        // Behavior: a subcommand with the wrong argument count is rejected with the Redis error text
        for (String[] args : new String[][]{
                {"COMMAND", "COUNT", "x"},
                {"COMMAND", "HELP", "x"},
                {"COMMAND", "GETKEYS"}}) {
            Object response = run(getChannel(), args);
            assertInstanceOf(ErrorRedisMessage.class, response);
            assertEquals("ERR wrong number of arguments for 'command|" + args[1].toLowerCase() + "' command",
                    ((ErrorRedisMessage) response).content());
        }
    }

    @Test
    void shouldRejectUnknownSubcommand() {
        // Behavior: an unknown COMMAND subcommand returns an error
        Object response = run(getChannel(), "COMMAND", "FOO");

        assertInstanceOf(ErrorRedisMessage.class, response);
        assertTrue(((ErrorRedisMessage) response).content().contains("unknown subcommand 'FOO'. Try COMMAND HELP."));
    }
}
