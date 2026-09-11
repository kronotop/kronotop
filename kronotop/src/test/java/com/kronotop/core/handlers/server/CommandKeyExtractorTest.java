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
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class CommandKeyExtractorTest {

    private static CommandMetadata metadata(int arity, List<KeySpec> keySpecs) {
        return new CommandMetadata("Summary", null, CommandGroup.STRING, "2026.06-1", arity, null,
                null, null, null, null, null, null, null, keySpecs, null, null, null);
    }

    private static KeySpec range(List<KeySpecFlag> flags, int pos, int lastkey, int step, int limit) {
        return new KeySpec(null, flags, new BeginSearch(new Index(pos)), new FindKeys(new Range(lastkey, step, limit)));
    }

    private static List<Integer> positions(List<CommandKeyExtractor.Key> keys) {
        assertNotNull(keys);
        return keys.stream().map(CommandKeyExtractor.Key::pos).toList();
    }

    @Test
    void shouldExtractIndexRangeKeys() {
        // Behavior: an index begin_search with a fixed range yields the positions in that range
        CommandMetadata get = metadata(2, List.of(range(List.of(KeySpecFlag.RO), 1, 0, 1, 0)));

        List<CommandKeyExtractor.Key> keys = CommandKeyExtractor.extract(get, List.of("get", "foo"));

        assertEquals(List.of(1), positions(keys));
        assertEquals(List.of(KeySpecFlag.RO), keys.getFirst().flags());
    }

    @Test
    void shouldExtractNegativeLastKey() {
        // Behavior: a negative lastkey counts from the end and the step skips values between keys
        CommandMetadata mset = metadata(-3, List.of(range(List.of(KeySpecFlag.OW), 1, -1, 2, 0)));

        assertEquals(List.of(1, 3), positions(CommandKeyExtractor.extract(mset, List.of("mset", "a", "1", "b", "2"))));
    }

    @Test
    void shouldExtractKeysWithLimit() {
        // Behavior: with a limit the range covers the first part of the remaining arguments
        CommandMetadata lmpop = metadata(-4, List.of(range(List.of(KeySpecFlag.RW), 2, -1, 1, 2)));

        assertEquals(List.of(2, 3), positions(CommandKeyExtractor.extract(lmpop, List.of("x", "2", "k1", "k2", "LEFT", "COUNT"))));
    }

    @Test
    void shouldExtractKeywordKeys() {
        // Behavior: a keyword begin_search finds the keyword from the end and takes the keys after it
        KeySpec spec = new KeySpec(null, List.of(KeySpecFlag.RW), new BeginSearch(new Keyword("KEYS", -2)),
                new FindKeys(new Range(-1, 1, 0)));
        CommandMetadata migrate = metadata(-6, List.of(spec));

        List<Integer> positions = positions(CommandKeyExtractor.extract(migrate,
                List.of("migrate", "host", "6379", "", "0", "5000", "keys", "k1", "k2")));

        assertEquals(List.of(7, 8), positions);
    }

    @Test
    void shouldSkipKeywordSpecWhenKeywordIsMissing() {
        // Behavior: when the keyword is not present the spec is skipped, and with no key found the result is null
        KeySpec spec = new KeySpec(null, List.of(KeySpecFlag.RW), new BeginSearch(new Keyword("KEYS", -2)),
                new FindKeys(new Range(-1, 1, 0)));
        CommandMetadata migrate = metadata(-6, List.of(spec));

        assertNull(CommandKeyExtractor.extract(migrate, List.of("migrate", "host", "6379", "", "0", "5000")));
    }

    @Test
    void shouldReturnNullWhenNoKeyIsFound() {
        // Behavior: a command whose specs yield no key is treated like an invalid spec
        CommandMetadata metadata = metadata(-2, List.of(range(List.of(KeySpecFlag.NOT_KEY), 1, 0, 1, 0)));

        assertNull(CommandKeyExtractor.extract(metadata, List.of("x", "a")));
    }

    @Test
    void shouldExtractKeynumKeys() {
        // Behavior: a keynum find_keys reads the key count from the arguments
        KeySpec spec = new KeySpec(null, List.of(KeySpecFlag.RW), new BeginSearch(new Index(2)),
                new FindKeys(new KeyNum(0, 1, 1)));
        CommandMetadata eval = metadata(-3, List.of(spec));

        assertEquals(List.of(3, 4), positions(CommandKeyExtractor.extract(eval, List.of("eval", "script", "2", "k1", "k2", "arg"))));
        assertNull(CommandKeyExtractor.extract(eval, List.of("eval", "script", "x", "k1")));
        assertNull(CommandKeyExtractor.extract(eval, List.of("eval", "script", "3", "k1")));
    }

    @Test
    void shouldSkipNotKeySpecs() {
        // Behavior: specs flagged NOT_KEY do not contribute keys
        CommandMetadata metadata = metadata(-3, List.of(
                range(List.of(KeySpecFlag.NOT_KEY), 1, 0, 1, 0),
                range(List.of(KeySpecFlag.RO), 2, 0, 1, 0)));

        assertEquals(List.of(2), positions(CommandKeyExtractor.extract(metadata, List.of("x", "a", "b"))));
    }

    @Test
    void shouldFailOnIncompleteSpec() {
        // Behavior: an incomplete spec makes the extraction fail after its keys were collected
        CommandMetadata metadata = metadata(-2, List.of(range(List.of(KeySpecFlag.RO, KeySpecFlag.INCOMPLETE), 1, 0, 1, 0)));

        assertNull(CommandKeyExtractor.extract(metadata, List.of("x", "a")));
    }

    @Test
    void shouldFailOnOutOfRange() {
        // Behavior: a range that points past the arguments is invalid
        CommandMetadata metadata = metadata(-2, List.of(range(List.of(KeySpecFlag.RO), 1, 1, 1, 0)));

        assertNull(CommandKeyExtractor.extract(metadata, List.of("x", "a")));
    }

    @Test
    void shouldFailOnUnknownSpec() {
        // Behavior: an unknown begin_search or find_keys type is invalid
        KeySpec spec = new KeySpec(null, List.of(KeySpecFlag.RO), new BeginSearch(null, null), new FindKeys(null, null));

        assertNull(CommandKeyExtractor.extract(metadata(-2, List.of(spec)), List.of("x", "a")));
    }
}
