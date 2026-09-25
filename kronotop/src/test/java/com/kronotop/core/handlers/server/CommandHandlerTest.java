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
import com.kronotop.commands.CommandMetadata;
import com.kronotop.server.KronotopChannelDuplexHandler;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.ServerKind;
import com.kronotop.server.resp3.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

    private EmbeddedChannel newInternalChannel() {
        EmbeddedChannel channel = new EmbeddedChannel(
                new RedisDecoder(false),
                new RedisBulkStringAggregator(),
                new RedisArrayAggregator(),
                new RedisMapAggregator(),
                new KronotopChannelDuplexHandler(context, context.getHandlers(ServerKind.INTERNAL), ServerKind.INTERNAL)
        );
        run(channel, "HELLO", "3");
        return channel;
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
        // Behavior: COMMAND DOCS without names returns one entry per command definition of the external server
        Map<String, RedisMessage> docs = docs(getChannel());

        assertEquals(context.getCommandMetadata(ServerKind.EXTERNAL).size(), docs.size());
        assertTrue(docs.containsKey("bucket.query"));
        assertTrue(docs.containsKey("zset"));
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
        // Behavior: COMMAND without a subcommand returns a ten-field entry per definition of the external server
        switchProtocol(RESPVersion.RESP3);
        List<RedisMessage> entries = infoEntries(run(getChannel(), "COMMAND"));

        assertEquals(context.getCommandMetadata(ServerKind.EXTERNAL).size(), entries.size());
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
        List<RedisMessage> entries = infoEntries(run(getChannel(), "COMMAND", "INFO", "ping", "zset"));

        assertEquals(2, entries.size());
        List<RedisMessage> ping = ((ArrayRedisMessage) entries.get(0)).children();
        assertEquals("ping", text(ping.get(0)));
        assertEquals(-1, integer(ping.get(1)));
        List<RedisMessage> zset = ((ArrayRedisMessage) entries.get(1)).children();
        assertEquals("zset", text(zset.get(0)));
        assertEquals(3, integer(zset.get(1)));
        assertEquals(1, integer(zset.get(3)));
        assertEquals(1, integer(zset.get(4)));
        assertEquals(1, integer(zset.get(5)));
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
        // Behavior: COMMAND COUNT returns the number of command definitions of the external server
        Object response = run(getChannel(), "COMMAND", "COUNT");

        assertEquals(context.getCommandMetadata(ServerKind.EXTERNAL).size(), integer((RedisMessage) response));
    }

    @Test
    void shouldCountOnlyInternalCommandsOnInternalServer() {
        // Behavior: on the internal server COMMAND COUNT returns the number of definitions of the internal server
        Object response = run(newInternalChannel(), "COMMAND", "COUNT");

        assertEquals(context.getCommandMetadata(ServerKind.INTERNAL).size(), integer((RedisMessage) response));
    }

    @Test
    void shouldReturnNullInfoForCommandNotOnServer() {
        // Behavior: on the internal server COMMAND INFO gives a null entry for a command that only the external server exposes
        List<RedisMessage> entries = infoEntries(run(newInternalChannel(), "COMMAND", "INFO", "bucket.query", "ping"));

        assertEquals(2, entries.size());
        assertInstanceOf(NullRedisMessage.class, entries.get(0));
        assertEquals("ping", text(((ArrayRedisMessage) entries.get(1)).children().get(0)));
    }

    @Test
    void shouldMatchRegisteredCommandsPerServer() {
        // Behavior: for every server kind the definitions match the registered handlers: every definition has a handler, and every handler with a definition lists that server kind
        for (ServerKind kind : ServerKind.values()) {
            Map<String, CommandMetadata> definitions = context.getCommandMetadata(kind);
            Set<String> registered = context.getHandlers(kind).getCommands();
            for (String name : definitions.keySet()) {
                assertTrue(registered.contains(name), kind + " has no handler for " + name);
            }
            for (String name : registered) {
                boolean defined = false;
                for (ServerKind other : ServerKind.values()) {
                    defined |= context.getCommandMetadata(other).containsKey(name);
                }
                if (defined) {
                    assertTrue(definitions.containsKey(name), name + " does not list " + kind);
                }
            }
        }
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
    void shouldReturnKrAdminSubcommands() {
        // Behavior: on the internal server COMMAND INFO KR.ADMIN nests twelve subcommands and COMMAND DOCS KR.ADMIN|ROUTE lists five arguments
        EmbeddedChannel internal = newInternalChannel();
        List<RedisMessage> entries = infoEntries(run(internal, "COMMAND", "INFO", "kr.admin"));

        List<RedisMessage> krAdmin = ((ArrayRedisMessage) entries.getFirst()).children();
        assertEquals(-2, integer(krAdmin.get(1)));
        assertInstanceOf(ArrayRedisMessage.class, krAdmin.get(9));
        List<String> subcommands = ((ArrayRedisMessage) krAdmin.get(9)).children().stream()
                .map(sub -> text(((ArrayRedisMessage) sub).children().getFirst()))
                .toList();
        assertEquals(12, subcommands.size());
        assertTrue(subcommands.contains("kr.admin|initialize-cluster"));
        assertTrue(subcommands.contains("kr.admin|drop-cluster"));

        Map<String, RedisMessage> docs = docs(internal, "kr.admin|route");
        Map<String, RedisMessage> route = asMap(docs.get("kr.admin|route"));
        assertEquals("cluster", text(route.get("group")));
        assertEquals(5, ((ArrayRedisMessage) route.get("arguments")).children().size());
    }

    @Test
    void shouldReturnVolumeCommandDefinitions() {
        // Behavior: on the internal server COMMAND INFO nests nine VOLUME.ADMIN and two VOLUME.INSPECT subcommands, and COMMAND DOCS exposes their arguments
        EmbeddedChannel internal = newInternalChannel();
        List<RedisMessage> entries = infoEntries(run(internal, "COMMAND", "INFO", "volume.admin", "volume.inspect"));
        assertEquals(2, entries.size());

        List<RedisMessage> volumeAdmin = ((ArrayRedisMessage) entries.get(0)).children();
        assertEquals(-2, integer(volumeAdmin.get(1)));
        List<String> adminSubcommands = ((ArrayRedisMessage) volumeAdmin.get(9)).children().stream()
                .map(sub -> text(((ArrayRedisMessage) sub).children().getFirst()))
                .toList();
        assertEquals(9, adminSubcommands.size());
        assertTrue(adminSubcommands.contains("volume.admin|vacuum"));
        assertTrue(adminSubcommands.contains("volume.admin|set-status"));

        List<RedisMessage> volumeInspect = ((ArrayRedisMessage) entries.get(1)).children();
        assertEquals(-2, integer(volumeInspect.get(1)));
        List<String> inspectSubcommands = ((ArrayRedisMessage) volumeInspect.get(9)).children().stream()
                .map(sub -> text(((ArrayRedisMessage) sub).children().getFirst()))
                .toList();
        assertEquals(2, inspectSubcommands.size());
        assertTrue(inspectSubcommands.contains("volume.inspect|cursor"));
        assertTrue(inspectSubcommands.contains("volume.inspect|replication"));

        Map<String, RedisMessage> docs = docs(internal, "volume.admin|vacuum", "volume.inspect|replication");
        Map<String, RedisMessage> vacuum = asMap(docs.get("volume.admin|vacuum"));
        assertEquals("volume", text(vacuum.get("group")));
        List<RedisMessage> vacuumArguments = ((ArrayRedisMessage) vacuum.get("arguments")).children();
        assertEquals(3, vacuumArguments.size());
        assertEquals(4, ((ArrayRedisMessage) asMap(vacuumArguments.getFirst()).get("arguments")).children().size());

        Map<String, RedisMessage> replication = asMap(docs.get("volume.inspect|replication"));
        assertEquals("volume", text(replication.get("group")));
        assertEquals(2, ((ArrayRedisMessage) replication.get("arguments")).children().size());
    }

    @Test
    void shouldReturnSegmentAndChangeLogCommandDefinitions() {
        // Behavior: on the internal server COMMAND INFO lists the five segment and changelog commands with their arity, and COMMAND DOCS exposes their groups and arguments
        EmbeddedChannel internal = newInternalChannel();
        List<RedisMessage> entries = infoEntries(run(internal, "COMMAND", "INFO",
                "segment.insert", "segment.range", "segment.tailpointer", "changelog.range", "changelog.watch"));
        assertEquals(5, entries.size());

        List<Long> arities = entries.stream()
                .map(entry -> integer(((ArrayRedisMessage) entry).children().get(1)))
                .toList();
        assertEquals(List.of(-5L, -5L, 3L, -5L, 3L), arities);

        Map<String, RedisMessage> docs = docs(internal, "segment.range", "changelog.range");
        Map<String, RedisMessage> segmentRange = asMap(docs.get("segment.range"));
        assertEquals("segment", text(segmentRange.get("group")));
        List<RedisMessage> rangeArguments = ((ArrayRedisMessage) segmentRange.get("arguments")).children();
        assertEquals(3, rangeArguments.size());
        assertEquals(2, ((ArrayRedisMessage) asMap(rangeArguments.get(2)).get("arguments")).children().size());

        Map<String, RedisMessage> changeLogRange = asMap(docs.get("changelog.range"));
        assertEquals("changelog", text(changeLogRange.get("group")));
        assertEquals(6, ((ArrayRedisMessage) changeLogRange.get("arguments")).children().size());
    }

    @Test
    void shouldReturnTransactionCommandDefinitions() {
        // Behavior: COMMAND INFO lists all six transaction commands and COMMAND DOCS COMMIT exposes the optional RETURNING argument
        switchProtocol(RESPVersion.RESP3);
        List<RedisMessage> entries = infoEntries(run(getChannel(), "COMMAND", "INFO",
                "begin", "commit", "rollback", "snapshotread", "getreadversion", "getapproximatesize"));
        assertEquals(6, entries.size());
        for (RedisMessage entry : entries) {
            assertInstanceOf(ArrayRedisMessage.class, entry);
        }

        List<RedisMessage> commit = ((ArrayRedisMessage) entries.get(1)).children();
        assertEquals("commit", text(commit.getFirst()));
        assertEquals(-1, integer(commit.get(1)));

        Map<String, RedisMessage> docs = docs(getChannel(), "commit", "snapshotread");
        Map<String, RedisMessage> commitDocs = asMap(docs.get("commit"));
        assertEquals("transactions", text(commitDocs.get("group")));
        List<RedisMessage> arguments = ((ArrayRedisMessage) commitDocs.get("arguments")).children();
        assertEquals(1, arguments.size());
        Map<String, RedisMessage> returning = asMap(arguments.getFirst());
        assertEquals("RETURNING", text(returning.get("token")));
        assertEquals(2, ((ArrayRedisMessage) returning.get("arguments")).children().size());

        Map<String, RedisMessage> snapshotRead = asMap(docs.get("snapshotread"));
        assertEquals(1, ((ArrayRedisMessage) snapshotRead.get("arguments")).children().size());
    }

    @Test
    void shouldReturnSessionAndServerCommandDefinitions() {
        // Behavior: COMMAND INFO lists the session, connection and server commands with their subcommands and COMMAND DOCS exposes their arguments
        switchProtocol(RESPVersion.RESP3);
        List<RedisMessage> entries = infoEntries(run(getChannel(), "COMMAND", "INFO",
                "session.attribute", "session.close", "hello", "client", "echo", "command", "info", "time", "tick"));
        assertEquals(9, entries.size());
        for (RedisMessage entry : entries) {
            assertInstanceOf(ArrayRedisMessage.class, entry);
        }

        Map<String, Integer> arities = Map.of("session.attribute", -2, "client", -2, "command", -1);
        Map<String, Integer> subcommandCounts = Map.of("session.attribute", 2, "client", 2, "command", 7);
        for (RedisMessage entry : entries) {
            List<RedisMessage> fields = ((ArrayRedisMessage) entry).children();
            String name = text(fields.getFirst());
            if (subcommandCounts.containsKey(name)) {
                assertEquals(arities.get(name), (int) integer(fields.get(1)), name);
                assertEquals(subcommandCounts.get(name), ((ArrayRedisMessage) fields.get(9)).children().size(), name);
            }
        }

        Map<String, RedisMessage> docs = docs(getChannel(), "hello", "session.attribute|set", "command|list", "tick");
        Map<String, RedisMessage> hello = asMap(docs.get("hello"));
        assertEquals("connection", text(hello.get("group")));
        assertEquals(3, ((ArrayRedisMessage) hello.get("arguments")).children().size());

        Map<String, RedisMessage> set = asMap(docs.get("session.attribute|set"));
        assertEquals("session", text(set.get("group")));
        List<RedisMessage> setArguments = ((ArrayRedisMessage) set.get("arguments")).children();
        assertEquals(2, setArguments.size());
        assertEquals(4, ((ArrayRedisMessage) asMap(setArguments.getFirst()).get("arguments")).children().size());

        Map<String, RedisMessage> list = asMap(docs.get("command|list"));
        assertEquals("server", text(list.get("group")));
        Map<String, RedisMessage> filterBy = asMap(((ArrayRedisMessage) list.get("arguments")).children().getFirst());
        assertEquals("FILTERBY", text(filterBy.get("token")));
        assertEquals(3, ((ArrayRedisMessage) filterBy.get("arguments")).children().size());

        Map<String, RedisMessage> tick = asMap(docs.get("tick"));
        assertEquals("transactions", text(tick.get("group")));
        assertEquals(1, ((ArrayRedisMessage) tick.get("arguments")).children().size());
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
        // Behavior: COMMAND LIST returns every command name of the external server, subcommands included, as lowercase bulk strings
        List<String> names = names(run(getChannel(), "COMMAND", "LIST"));

        int expected = 0;
        for (var metadata : context.getCommandMetadata(ServerKind.EXTERNAL).values()) {
            expected += 1 + metadata.subcommands().size();
        }
        assertEquals(expected, names.size());
        assertTrue(names.contains("bucket.query"));
        assertTrue(names.contains("ping"));
        assertFalse(names.contains("kr.admin"));
    }

    @Test
    void shouldListOnlyInternalCommandsOnInternalServer() {
        // Behavior: on the internal server COMMAND LIST returns the commands of the internal server and skips external-only commands
        List<String> names = names(run(newInternalChannel(), "COMMAND", "LIST"));

        assertTrue(names.contains("kr.admin"));
        assertTrue(names.contains("kr.admin|route"));
        assertTrue(names.contains("ping"));
        assertFalse(names.contains("bucket.query"));
    }

    @Test
    void shouldListByPattern() {
        // Behavior: FILTERBY PATTERN keeps the names that match the glob pattern, subcommands included, without regard to case
        List<String> names = names(run(getChannel(), "COMMAND", "LIST", "FILTERBY", "PATTERN", "BUCKET.*"));

        assertEquals(21, names.size());
        assertTrue(names.contains("bucket.query"));
        assertTrue(names.contains("bucket.index"));
        assertTrue(names.contains("bucket.index|create"));
        assertFalse(names.contains("query"));
    }

    @Test
    void shouldListByAclCategory() {
        // Behavior: FILTERBY ACLCAT keeps the commands in that category, explicit or derived, unknown category gives nothing
        List<String> connection = names(run(getChannel(), "COMMAND", "LIST", "filterby", "aclcat", "connection"));
        assertTrue(connection.contains("auth"));
        assertTrue(connection.contains("ping"));
        assertFalse(connection.contains("zget"));

        List<String> read = names(run(getChannel(), "COMMAND", "LIST", "FILTERBY", "ACLCAT", "read"));
        assertTrue(read.contains("zget"));
        assertTrue(read.contains("bucket.query"));
        assertFalse(read.contains("zset"));

        List<String> zmap = names(run(getChannel(), "COMMAND", "LIST", "FILTERBY", "ACLCAT", "zmap"));
        assertEquals(18, zmap.size());
        assertTrue(zmap.contains("zgetrange"));
        assertTrue(zmap.contains("zinc.i64"));

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
        assertEquals(List.of("Foo"), names(run(getChannel(), "COMMAND", "GETKEYS", "zset", "Foo", "bar")));
        assertEquals(List.of("k"), names(run(getChannel(), "COMMAND", "GETKEYS", "ZGET", "k")));
    }

    @Test
    void shouldReturnKeysAndFlagsForGetKeysAndFlags() {
        // Behavior: COMMAND GETKEYSANDFLAGS pairs each key with the flags of its key spec
        switchProtocol(RESPVersion.RESP3);
        Object response = run(getChannel(), "COMMAND", "GETKEYSANDFLAGS", "zget", "foo");

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
    void shouldRejectGetKeysForRangeCommands() {
        // Behavior: the ZMap range commands take boundaries, not keys, so they carry no key specs
        for (String[] args : new String[][]{
                {"COMMAND", "GETKEYS", "zgetrange", "a", "b"},
                {"COMMAND", "GETKEYS", "zdelrange", "a", "b"},
                {"COMMAND", "GETKEYS", "zgetrangesize", "a", "b"}}) {
            Object response = run(getChannel(), args);
            assertInstanceOf(ErrorRedisMessage.class, response);
            assertEquals("ERR The command has no key arguments", ((ErrorRedisMessage) response).content());
        }
    }

    @Test
    void shouldRejectGetKeysWithWrongArity() {
        // Behavior: an argument count that does not fit the command arity is rejected
        Object response = run(getChannel(), "COMMAND", "GETKEYS", "zget", "a", "b");

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
