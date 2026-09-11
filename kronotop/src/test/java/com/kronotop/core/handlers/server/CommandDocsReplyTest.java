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
import com.kronotop.commands.CommandGroup;
import com.kronotop.commands.CommandMetadata;
import com.kronotop.commands.DocFlag;
import com.kronotop.server.resp3.*;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class CommandDocsReplyTest {

    private static CommandMetadata metadata(List<Argument> arguments, List<List<String>> history,
                                            Map<String, CommandMetadata> subcommands) {
        return new CommandMetadata("Summary", "O(1)", CommandGroup.BUCKET, "2026.06-1", -3, null,
                null, null, null, history, null, null, null, null, null, arguments, subcommands);
    }

    private static Argument argument(String name, String type, String display, Integer keySpecIndex, String token,
                                     boolean optional, boolean multiple, boolean multipleToken, List<Argument> nested) {
        return new Argument(name, type, display, keySpecIndex, token, null, null, null, optional, multiple, multipleToken, nested);
    }

    private static Map<String, RedisMessage> asMap(RedisMessage message) {
        assertInstanceOf(MapRedisMessage.class, message);
        Map<String, RedisMessage> result = new java.util.LinkedHashMap<>();
        ((MapRedisMessage) message).children().forEach((key, value) -> result.put(text(key), value));
        return result;
    }

    private static String text(RedisMessage message) {
        assertInstanceOf(FullBulkStringRedisMessage.class, message);
        return ((FullBulkStringRedisMessage) message).content().toString(StandardCharsets.UTF_8);
    }

    private static List<String> flags(RedisMessage message) {
        assertInstanceOf(SetRedisMessage.class, message);
        return ((SetRedisMessage) message).children().stream().map(child -> {
            assertInstanceOf(SimpleStringRedisMessage.class, child);
            return ((SimpleStringRedisMessage) child).content();
        }).toList();
    }

    @Test
    void shouldOmitEmptyFieldsButKeepGroup() {
        // Behavior: null and empty fields are left out, group is always written
        CommandMetadata metadata = new CommandMetadata(null, null, CommandGroup.CONNECTION, null, -1, null,
                null, null, null, null, null, null, null, null, null, null, null);

        Map<RedisMessage, RedisMessage> root = CommandDocsReply.build(Map.of("PING", metadata));

        assertEquals(1, root.size());
        Map<String, RedisMessage> command = asMap(root.values().iterator().next());
        assertEquals(List.of("group"), List.copyOf(command.keySet()));
        assertEquals("connection", text(command.get("group")));
    }

    @Test
    void shouldLowercaseCommandNamesAndOrderFields() {
        // Behavior: the top-level key is the lowercase command name and fields follow the Redis order
        CommandMetadata metadata = metadata(List.of(), List.of(), Map.of());

        Map<RedisMessage, RedisMessage> root = CommandDocsReply.build(Map.of("BUCKET.QUERY", metadata));

        assertEquals("bucket.query", text(root.keySet().iterator().next()));
        Map<String, RedisMessage> command = asMap(root.values().iterator().next());
        assertEquals(List.of("summary", "since", "group", "complexity"), List.copyOf(command.keySet()));
        assertEquals("Summary", text(command.get("summary")));
        assertEquals("2026.06-1", text(command.get("since")));
        assertEquals("bucket", text(command.get("group")));
        assertEquals("O(1)", text(command.get("complexity")));
    }

    @Test
    void shouldRenderArgumentsInRedisOrder() {
        // Behavior: a plain argument carries display_text, key_spec_index, token and flags as simple strings
        Argument key = argument("key", "key", null, 0, null, false, false, false, List.of());
        Argument field = argument("field", "string", "fld", null, "FIELD", true, true, true, List.of());
        CommandMetadata metadata = metadata(List.of(key, field), List.of(), Map.of());

        Map<String, RedisMessage> command = asMap(CommandDocsReply.build(Map.of("SET", metadata)).values().iterator().next());
        assertInstanceOf(ArrayRedisMessage.class, command.get("arguments"));
        List<RedisMessage> arguments = ((ArrayRedisMessage) command.get("arguments")).children();
        assertEquals(2, arguments.size());

        Map<String, RedisMessage> first = asMap(arguments.get(0));
        assertEquals(List.of("name", "type", "display_text", "key_spec_index"), List.copyOf(first.keySet()));
        assertEquals("key", text(first.get("display_text")));
        assertEquals(0, ((IntegerRedisMessage) first.get("key_spec_index")).value());

        Map<String, RedisMessage> second = asMap(arguments.get(1));
        assertEquals(List.of("name", "type", "display_text", "token", "flags"), List.copyOf(second.keySet()));
        assertEquals("fld", text(second.get("display_text")));
        assertEquals("FIELD", text(second.get("token")));
        assertEquals(List.of("optional", "multiple", "multiple_token"), flags(second.get("flags")));
    }

    @Test
    void shouldNestOneofAndBlockArguments() {
        // Behavior: oneof and block arguments have no display_text and carry their nested arguments
        Argument asc = argument("asc", "pure-token", null, null, "ASC", false, false, false, List.of());
        Argument desc = argument("desc", "pure-token", null, null, "DESC", false, false, false, List.of());
        Argument direction = argument("direction", "oneof", null, null, null, false, false, false, List.of(asc, desc));
        Argument field = argument("field", "string", null, null, null, false, false, false, List.of());
        Argument sortby = argument("sortby", "block", null, null, "SORTBY", true, false, false, List.of(field, direction));
        CommandMetadata metadata = metadata(List.of(sortby), List.of(), Map.of());

        Map<String, RedisMessage> command = asMap(CommandDocsReply.build(Map.of("BUCKET.QUERY", metadata)).values().iterator().next());
        Map<String, RedisMessage> block = asMap(((ArrayRedisMessage) command.get("arguments")).children().getFirst());
        assertEquals(List.of("name", "type", "token", "flags", "arguments"), List.copyOf(block.keySet()));
        assertEquals("block", text(block.get("type")));

        List<RedisMessage> nested = ((ArrayRedisMessage) block.get("arguments")).children();
        assertEquals(2, nested.size());
        Map<String, RedisMessage> oneof = asMap(nested.get(1));
        assertEquals(List.of("name", "type", "arguments"), List.copyOf(oneof.keySet()));
        List<RedisMessage> tokens = ((ArrayRedisMessage) oneof.get("arguments")).children();
        assertEquals("ASC", text(asMap(tokens.get(0)).get("token")));
        assertEquals("DESC", text(asMap(tokens.get(1)).get("token")));
    }

    @Test
    void shouldRenderHistoryAsSetOfPairs() {
        // Behavior: history is a set, each entry an array of version and description strings
        CommandMetadata metadata = metadata(List.of(), List.of(List.of("2026.06-1", "Added LIMIT.")), Map.of());

        Map<String, RedisMessage> command = asMap(CommandDocsReply.build(Map.of("BUCKET.QUERY", metadata)).values().iterator().next());
        assertInstanceOf(SetRedisMessage.class, command.get("history"));
        List<RedisMessage> history = List.copyOf(((SetRedisMessage) command.get("history")).children());
        assertEquals(1, history.size());
        List<RedisMessage> pair = ((ArrayRedisMessage) history.getFirst()).children();
        assertEquals("2026.06-1", text(pair.get(0)));
        assertEquals("Added LIMIT.", text(pair.get(1)));
    }

    @Test
    void shouldRenderReplySchemaAsNestedMap() {
        // Behavior: reply_schema follows history and comes before arguments; objects become maps, arrays become
        // arrays, booleans, integers and strings keep their type
        Map<String, Object> entries = new java.util.LinkedHashMap<>();
        entries.put("type", "array");
        entries.put("items", Map.of("type", "string"));
        Map<String, Object> properties = new java.util.LinkedHashMap<>();
        properties.put("cursor_id", Map.of("type", "integer"));
        properties.put("entries", entries);
        Map<String, Object> schema = new java.util.LinkedHashMap<>();
        schema.put("type", "object");
        schema.put("additionalProperties", false);
        schema.put("minProperties", 2);
        schema.put("required", List.of("cursor_id", "entries"));
        schema.put("properties", properties);
        Argument bucket = argument("bucket", "string", null, null, null, false, false, false, List.of());
        CommandMetadata metadata = new CommandMetadata("Summary", null, CommandGroup.BUCKET, null, -3, null,
                null, null, null, List.of(List.of("2026.06-1", "Initial version.")), null, null, null, null,
                schema, List.of(bucket), null);

        Map<String, RedisMessage> command = asMap(CommandDocsReply.build(Map.of("BUCKET.QUERY", metadata)).values().iterator().next());
        assertEquals(List.of("summary", "group", "history", "reply_schema", "arguments"), List.copyOf(command.keySet()));

        Map<String, RedisMessage> reply = asMap(command.get("reply_schema"));
        assertEquals(List.of("type", "additionalProperties", "minProperties", "required", "properties"), List.copyOf(reply.keySet()));
        assertEquals("object", text(reply.get("type")));
        assertFalse(((BooleanRedisMessage) reply.get("additionalProperties")).value());
        assertEquals(2, ((IntegerRedisMessage) reply.get("minProperties")).value());
        List<RedisMessage> required = ((ArrayRedisMessage) reply.get("required")).children();
        assertEquals("cursor_id", text(required.get(0)));
        assertEquals("entries", text(required.get(1)));

        Map<String, RedisMessage> nested = asMap(asMap(reply.get("properties")).get("entries"));
        assertEquals("array", text(nested.get("type")));
        assertEquals("string", text(asMap(nested.get("items")).get("type")));
    }

    @Test
    void shouldOrderDocFlagsLikeRedis() {
        // Behavior: doc flags are written in the fixed Redis order as simple strings
        CommandMetadata metadata = new CommandMetadata("Summary", null, CommandGroup.BUCKET, null, -1, null,
                List.of(DocFlag.SYSCMD, DocFlag.DEPRECATED), null, null, null, null, null, null, null, null, null, null);

        Map<String, RedisMessage> command = asMap(CommandDocsReply.build(Map.of("X", metadata)).values().iterator().next());
        assertEquals(List.of("deprecated", "syscmd"), flags(command.get("doc_flags")));
    }

    @Test
    void shouldKeySubcommandsAsContainerPipeSub() {
        // Behavior: subcommands are a map keyed by lowercase "container|subcommand"
        Argument shard = argument("shard-id", "integer", null, null, null, false, false, false, List.of());
        CommandMetadata describe = metadata(List.of(shard), List.of(), Map.of());
        CommandMetadata parent = metadata(List.of(), List.of(), Map.of("DESCRIBE-SHARD", describe));

        Map<String, RedisMessage> command = asMap(CommandDocsReply.build(Map.of("KR.ADMIN", parent)).values().iterator().next());
        Map<String, RedisMessage> subcommands = asMap(command.get("subcommands"));
        assertEquals(List.of("kr.admin|describe-shard"), List.copyOf(subcommands.keySet()));
        Map<String, RedisMessage> sub = asMap(subcommands.get("kr.admin|describe-shard"));
        Map<String, RedisMessage> argument = asMap(((ArrayRedisMessage) sub.get("arguments")).children().getFirst());
        assertEquals("shard-id", text(argument.get("name")));
    }
}
