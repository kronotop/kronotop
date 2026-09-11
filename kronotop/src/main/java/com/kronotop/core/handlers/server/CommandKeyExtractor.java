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

import java.util.ArrayList;
import java.util.List;

/**
 * Finds the key positions of a full command using its key specs, with the same rules Redis uses
 * for COMMAND GETKEYS.
 */
public final class CommandKeyExtractor {
    private CommandKeyExtractor() {
    }

    /**
     * Returns the keys found in argv, or null when a key spec cannot be applied to the arguments
     * or no key is found. argv[0] is the command name. For a subcommand, argv[1] is the subcommand name.
     */
    public static List<Key> extract(CommandMetadata metadata, List<String> argv) {
        int argc = argv.size();
        List<Key> keys = new ArrayList<>();
        for (KeySpec spec : metadata.keySpecs()) {
            if (spec.flags().contains(KeySpecFlag.NOT_KEY)) {
                continue;
            }
            int first = 0;
            BeginSearch beginSearch = spec.beginSearch();
            switch (beginSearch.type()) {
                case INDEX -> first = beginSearch.index().pos();
                case KEYWORD -> {
                    Keyword keyword = beginSearch.keyword();
                    int startIndex = keyword.startfrom() > 0 ? keyword.startfrom() : argc + keyword.startfrom();
                    int endIndex = keyword.startfrom() > 0 ? argc - 1 : 1;
                    for (int i = startIndex; i != endIndex; i = startIndex <= endIndex ? i + 1 : i - 1) {
                        if (i >= argc || i < 1) {
                            break;
                        }
                        if (argv.get(i).equalsIgnoreCase(keyword.keyword())) {
                            first = i + 1;
                            break;
                        }
                    }
                    if (first == 0) {
                        continue;
                    }
                }
                case UNKNOWN -> {
                    return null;
                }
            }

            int last;
            int step;
            FindKeys findKeys = spec.findKeys();
            switch (findKeys.type()) {
                case RANGE -> {
                    Range range = findKeys.range();
                    step = range.step();
                    if (range.lastkey() >= 0) {
                        last = first + range.lastkey();
                    } else if (range.limit() == 0) {
                        last = argc + range.lastkey();
                    } else {
                        last = first + ((argc - first) / range.limit() + range.lastkey());
                    }
                }
                case KEYNUM -> {
                    KeyNum keynum = findKeys.keynum();
                    step = keynum.step();
                    int keynumidx = first + keynum.keynumidx();
                    if (keynumidx >= argc || keynumidx < 0) {
                        return null;
                    }
                    long numkeys;
                    try {
                        numkeys = Long.parseLong(argv.get(keynumidx));
                    } catch (NumberFormatException e) {
                        return null;
                    }
                    if (numkeys < 0) {
                        return null;
                    }
                    first += keynum.firstkey();
                    if (step <= 0 || first < 0 || numkeys - 1 > (argc - 1 - first) / step) {
                        return null;
                    }
                    last = (int) (first + (numkeys - 1) * step);
                }
                default -> {
                    return null;
                }
            }

            if (last >= argc || last < first || first >= argc) {
                return null;
            }
            for (int i = first; i <= last; i += step) {
                keys.add(new Key(i, spec.flags()));
            }
            if (spec.flags().contains(KeySpecFlag.INCOMPLETE)) {
                return null;
            }
        }
        return keys.isEmpty() ? null : keys;
    }

    /**
     * A key position in argv with the flags of the spec that found it.
     */
    public record Key(int pos, List<KeySpecFlag> flags) {
    }
}
