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

package com.kronotop.server;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class CommandTypeTest {

    static Stream<Arguments> allCommandTypes() {
        return Stream.of(CommandType.values())
                .map(Arguments::of);
    }

    // Behavior: Verifies simple Redis commands without dots parse correctly.
    @Test
    void shouldParseSimpleCommands() {
        assertEquals(CommandType.PING, CommandType.parse("PING"));
        assertEquals(CommandType.GET, CommandType.parse("GET"));
        assertEquals(CommandType.SET, CommandType.parse("SET"));
        assertEquals(CommandType.HGET, CommandType.parse("HGET"));
    }

    // Behavior: Verifies commands with dot notation parse to their enum constants.
    @Test
    void shouldParseDotNotationCommands() {
        assertEquals(CommandType.BUCKET_INSERT, CommandType.parse("BUCKET.INSERT"));
        assertEquals(CommandType.BUCKET_QUERY, CommandType.parse("BUCKET.QUERY"));
        assertEquals(CommandType.ZGET_I64, CommandType.parse("ZGET.I64"));
        assertEquals(CommandType.ZGET_F64, CommandType.parse("ZGET.F64"));
        assertEquals(CommandType.ZGET_D128, CommandType.parse("ZGET.D128"));
        assertEquals(CommandType.KR_ADMIN, CommandType.parse("KR.ADMIN"));
    }

    // Behavior: Verifies unknown commands return null.
    @Test
    void shouldReturnNullForUnknownCommands() {
        assertNull(CommandType.parse("UNKNOWN"));
        assertNull(CommandType.parse("INVALID.COMMAND"));
        assertNull(CommandType.parse(""));
    }

    // Behavior: Verifies getCommandName returns the original wire format.
    @Test
    void shouldReturnCorrectCommandNames() {
        assertEquals("PING", CommandType.PING.getCommandName());
        assertEquals("BUCKET.INSERT", CommandType.BUCKET_INSERT.getCommandName());
        assertEquals("ZGET.I64", CommandType.ZGET_I64.getCommandName());
        assertEquals("KR.ADMIN", CommandType.KR_ADMIN.getCommandName());
    }

    // Behavior: Verifies all enum constants can be parsed from their command names.
    @ParameterizedTest
    @MethodSource("allCommandTypes")
    void shouldParseAllCommandTypes(CommandType commandType) {
        CommandType parsed = CommandType.parse(commandType.getCommandName());
        assertEquals(commandType, parsed,
                String.format("CommandType.parse(\"%s\") should return %s",
                        commandType.getCommandName(), commandType));
    }
}
