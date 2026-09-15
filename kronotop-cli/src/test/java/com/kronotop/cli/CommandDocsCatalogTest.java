/*
 * Copyright (c) 2023-2026 Burak Sezer
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

package com.kronotop.cli;

import com.kronotop.resp.RespValue;
import org.jline.console.ArgDesc;
import org.jline.console.CmdDesc;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommandDocsCatalogTest {

    private static RespValue str(String value) {
        return new RespValue.BlobString(value);
    }

    private static RespValue map(Object... kv) {
        Map<RespValue, RespValue> out = new LinkedHashMap<>();
        for (int i = 0; i < kv.length; i += 2) {
            out.put(str((String) kv[i]), (RespValue) kv[i + 1]);
        }
        return new RespValue.RespMap(out);
    }

    private static RespValue array(RespValue... values) {
        return new RespValue.Array(List.of(values));
    }

    private static RespValue flags(String... values) {
        LinkedHashSet<RespValue> out = new LinkedHashSet<>();
        for (String v : values) {
            out.add(str(v));
        }
        return new RespValue.RespSet(out);
    }

    private static RespValue scalar(String name) {
        return map("name", str(name), "type", str("string"), "display_text", str(name));
    }

    private static RespValue pureToken(String name, String token) {
        return map("name", str(name), "type", str("pure-token"), "display_text", str(name), "token", str(token));
    }

    private static RespValue sortBlock(String name, String token) {
        return map(
                "name", str(name),
                "type", str("block"),
                "token", str(token),
                "flags", flags("optional"),
                "arguments", array(
                        scalar("field"),
                        map("name", str("direction"), "type", str("oneof"),
                                "arguments", array(pureToken("asc", "ASC"), pureToken("desc", "DESC")))));
    }

    private static RespValue optionalScalar(String name, String token) {
        return map("name", str(name), "type", str("string"), "display_text", str(name),
                "token", str(token), "flags", flags("optional"));
    }

    private static RespValue bucketQueryDocs() {
        return map("bucket.query", map(
                "summary", str("Queries documents from a bucket."),
                "arguments", array(
                        scalar("bucket"),
                        scalar("query"),
                        sortBlock("sortby", "SORTBY"),
                        sortBlock("resultsort", "RESULTSORT"),
                        optionalScalar("spec", "PROJECTION"),
                        optionalScalar("count", "BATCH"),
                        optionalScalar("limit", "LIMIT"),
                        optionalScalar("spec", "COLLATION"))));
    }

    private static RespValue clientDocs() {
        return map("client", map(
                "summary", str("Container command."),
                "subcommands", map(
                        "client|setname", map("arguments", array(scalar("connection-name"))),
                        "client|setinfo", map("arguments", array(scalar("attribute"), scalar("value"))))));
    }

    private static List<String> names(CmdDesc desc) {
        List<String> out = new ArrayList<>();
        for (ArgDesc arg : desc.getArgsDesc()) {
            out.add(arg.getName());
        }
        return out;
    }

    /**
     * Converts a RESP3 reply to the flat RESP2 shape, the same way the server does.
     */
    private static RespValue toResp2(RespValue value) {
        if (value instanceof RespValue.RespMap(Map<RespValue, RespValue> values)) {
            List<RespValue> flat = new ArrayList<>();
            values.forEach((k, v) -> {
                flat.add(toResp2(k));
                flat.add(toResp2(v));
            });
            return new RespValue.Array(flat);
        }
        if (value instanceof RespValue.RespSet(java.util.Set<RespValue> values)) {
            return new RespValue.Array(values.stream().map(CommandDocsCatalogTest::toResp2).toList());
        }
        if (value instanceof RespValue.Array(List<RespValue> values)) {
            return new RespValue.Array(values.stream().map(CommandDocsCatalogTest::toResp2).toList());
        }
        return value;
    }

    @Test
    void shouldRenderBucketQueryArguments() {
        // Behavior: tokens, optional blocks, and oneof choices render as one hint token per typed word
        CommandDocsCatalog catalog = new CommandDocsCatalog(bucketQueryDocs());
        assertEquals(List.of(
                "bucket", "query",
                "[SORTBY", "field", "ASC|DESC]", "[RESULTSORT", "field", "ASC|DESC]",
                "[PROJECTION", "spec]", "[BATCH", "count]", "[LIMIT", "limit]", "[COLLATION", "spec]"
        ), catalog.usage("bucket.query"));
    }

    @Test
    void shouldLookupCommandCaseInsensitively() {
        // Behavior: the typed command name matches the lowercase catalog key
        CommandDocsCatalog catalog = new CommandDocsCatalog(bucketQueryDocs());
        CmdDesc desc = catalog.lookup(List.of("BUCKET.QUERY"));
        assertEquals("bucket", names(desc).get(0));
        assertTrue(desc.isCommand());
    }

    @Test
    void shouldParseResp2FlatArrayReply() {
        // Behavior: the RESP2 key/value array yields the same hints as the RESP3 map
        CommandDocsCatalog resp3 = new CommandDocsCatalog(bucketQueryDocs());
        CommandDocsCatalog resp2 = new CommandDocsCatalog(toResp2(bucketQueryDocs()));
        assertEquals(resp3.usage("bucket.query"), resp2.usage("bucket.query"));
    }

    @Test
    void shouldRenderSubcommandHintWithSubcommandToken() {
        // Behavior: a container command with a typed subcommand shows the subcommand and its arguments
        CommandDocsCatalog catalog = new CommandDocsCatalog(clientDocs());
        assertTrue(catalog.isContainer("CLIENT"));
        assertEquals(List.of("SETNAME", "connection-name"), names(catalog.lookup(List.of("CLIENT", "setname"))));
        assertEquals(List.of("SETINFO", "attribute", "value"), names(catalog.lookup(List.of("client", "SETINFO"))));
    }

    @Test
    void shouldShowSubcommandPlaceholderWhenSubcommandMissing() {
        // Behavior: a container command without a known subcommand shows a placeholder
        CommandDocsCatalog catalog = new CommandDocsCatalog(clientDocs());
        assertEquals(List.of("subcommand"), names(catalog.lookup(List.of("CLIENT"))));
        assertEquals(List.of("subcommand"), names(catalog.lookup(List.of("CLIENT", "nope"))));
    }

    @Test
    void shouldReturnNullForUnknownCommand() {
        // Behavior: an unknown command has no hint
        CommandDocsCatalog catalog = new CommandDocsCatalog(bucketQueryDocs());
        assertNull(catalog.lookup(List.of("NOPE", "x")));
        assertNull(catalog.lookup(List.of()));
    }

    @Test
    void shouldRenderMultipleFlagWithEllipsis() {
        // Behavior: a repeatable argument gets a trailing ellipsis inside the optional brackets
        RespValue docs = map("mget", map("arguments", array(
                map("name", str("key"), "type", str("key"), "display_text", str("key"),
                        "flags", flags("optional", "multiple")))));
        CommandDocsCatalog catalog = new CommandDocsCatalog(docs);
        assertEquals(List.of("[key...]"), catalog.usage("MGET"));
    }

    @Test
    void shouldReturnDocForTopLevelCommand() {
        // Behavior: doc returns the uppercase name, usage, summary, since and group of a top-level command
        CommandDocsCatalog catalog = new CommandDocsCatalog(map("bucket.query", map(
                "summary", str("Queries documents."),
                "since", str("2026.06-1"),
                "group", str("bucket"),
                "arguments", array(scalar("bucket")))));
        CommandDocsCatalog.CommandDoc doc = catalog.doc(List.of("Bucket.Query"));
        assertEquals("BUCKET.QUERY", doc.name());
        assertEquals(List.of("bucket"), doc.usage());
        assertEquals("Queries documents.", doc.summary());
        assertEquals("2026.06-1", doc.since());
        assertEquals("bucket", doc.group());
    }

    @Test
    void shouldReturnDocForSubcommand() {
        // Behavior: the second word selects the subcommand, its name joins container and subcommand with a space
        CommandDocsCatalog catalog = new CommandDocsCatalog(clientDocs());
        CommandDocsCatalog.CommandDoc doc = catalog.doc(List.of("client", "setname"));
        assertEquals("CLIENT SETNAME", doc.name());
        assertEquals(List.of("connection-name"), doc.usage());
    }

    @Test
    void shouldFallBackToContainerDocForUnknownSubcommand() {
        // Behavior: an unknown second word returns the container's own entry
        CommandDocsCatalog catalog = new CommandDocsCatalog(clientDocs());
        CommandDocsCatalog.CommandDoc doc = catalog.doc(List.of("CLIENT", "nope"));
        assertEquals("CLIENT", doc.name());
        assertEquals("Container command.", doc.summary());
    }

    @Test
    void shouldReturnNullDocForUnknownCommand() {
        // Behavior: an unknown command and an empty word list have no help entry
        CommandDocsCatalog catalog = new CommandDocsCatalog(clientDocs());
        assertNull(catalog.doc(List.of("NOPE")));
        assertNull(catalog.doc(List.of()));
    }

    @Test
    void shouldUseEmptyStringForMissingFields() {
        // Behavior: fields the server omits are empty strings, not null
        CommandDocsCatalog catalog = new CommandDocsCatalog(map("ping", map()));
        CommandDocsCatalog.CommandDoc doc = catalog.doc(List.of("ping"));
        assertEquals("", doc.summary());
        assertEquals("", doc.since());
        assertEquals("", doc.group());
        assertEquals("", doc.usageLine());
    }

    @Test
    void shouldBuildEmptyCatalogFromErrorReply() {
        // Behavior: an error or empty reply yields a catalog with no hints and no exception
        CommandDocsCatalog fromError = new CommandDocsCatalog(new RespValue.SimpleError("ERR", "nope"));
        assertNull(fromError.lookup(List.of("BUCKET.QUERY")));
        CommandDocsCatalog fromEmpty = new CommandDocsCatalog(new RespValue.RespMap(Map.of()));
        assertNull(fromEmpty.lookup(List.of("BUCKET.QUERY")));
        assertFalse(fromEmpty.isContainer("client"));
    }
}
