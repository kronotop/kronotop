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
import org.jline.console.CmdDesc;
import org.jline.console.CmdLine;
import org.jline.reader.Candidate;
import org.jline.reader.ParsedLine;
import org.jline.reader.Parser;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CommandCompleterTest {

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

    private static CommandDocsCatalog catalog() {
        return new CommandDocsCatalog(map(
                "bucket.query", map("summary", str("q")),
                "bucket.insert", map("summary", str("i")),
                "client", map("subcommands", map(
                        "client|setname", map("summary", str("s")),
                        "client|setinfo", map("summary", str("s"))))));
    }

    private static List<String> complete(CommandCompleter completer, String line) {
        ParsedLine parsed = new MultiLineParser().parse(line, line.length(), Parser.ParseContext.COMPLETE);
        List<Candidate> candidates = new ArrayList<>();
        completer.complete(null, parsed, candidates);
        List<String> values = new ArrayList<>();
        for (Candidate candidate : candidates) {
            values.add(candidate.value());
        }
        return values;
    }

    @Test
    void shouldOfferUppercaseCommandNamesForFirstWord() {
        // Behavior: the first word completes against all top-level command names in uppercase
        CommandCompleter completer = new CommandCompleter();
        completer.setCatalog(catalog());
        List<String> values = complete(completer, "bucket.q");
        assertTrue(values.contains("BUCKET.QUERY"));
        assertTrue(values.contains("BUCKET.INSERT"));
        assertTrue(values.contains("CLIENT"));
    }

    @Test
    void shouldOfferSubcommandsForSecondWordOfContainer() {
        // Behavior: the second word of a container command completes against its subcommands
        CommandCompleter completer = new CommandCompleter();
        completer.setCatalog(catalog());
        assertEquals(List.of("SETNAME", "SETINFO"), complete(completer, "client set"));
    }

    @Test
    void shouldOfferNothingForSecondWordOfPlainCommand() {
        // Behavior: a plain command has no completion for its arguments
        CommandCompleter completer = new CommandCompleter();
        completer.setCatalog(catalog());
        assertEquals(List.of(), complete(completer, "BUCKET.QUERY us"));
    }

    @Test
    void shouldOfferNothingWithoutCatalog() {
        // Behavior: no catalog means no candidates and no exception
        CommandCompleter completer = new CommandCompleter();
        assertEquals(List.of(), complete(completer, "bucket.q"));
    }

    private static CmdLine cmdLine(List<String> words) {
        return new CmdLine(String.join(" ", words), "", "", words, CmdLine.DescriptionType.COMMAND);
    }

    @Test
    void shouldReturnNullHintWithoutCatalog() {
        // Behavior: lookup returns null before the catalog is loaded
        CommandCompleter completer = new CommandCompleter();
        assertNull(completer.lookup(cmdLine(List.of("CLIENT"))));
    }

    @Test
    void shouldReturnHintAfterCatalogIsSet() {
        // Behavior: lookup delegates to the catalog once it is loaded
        CommandCompleter completer = new CommandCompleter();
        completer.setCatalog(catalog());
        CmdDesc desc = completer.lookup(cmdLine(List.of("CLIENT")));
        assertEquals(1, desc.getArgsDesc().size());
        assertEquals("subcommand", desc.getArgsDesc().get(0).getName());
    }
}
