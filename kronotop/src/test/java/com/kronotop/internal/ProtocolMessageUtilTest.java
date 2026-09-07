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

package com.kronotop.internal;

import com.kronotop.server.IllegalCommandArgumentException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtocolMessageUtilTest {

    @Test
    void shouldAcceptDistinctArguments() {
        // Behavior: markArgumentSeen accepts each keyword once and returns a bitmask that
        // carries every keyword seen so far.
        long seen = 0;
        seen = ProtocolMessageUtil.markArgumentSeen(seen, Keyword.LIMIT);
        seen = ProtocolMessageUtil.markArgumentSeen(seen, Keyword.REVERSE);
        seen = ProtocolMessageUtil.markArgumentSeen(seen, Keyword.SELECTOR);

        assertEquals(0b111, seen);
    }

    @Test
    void shouldRejectRepeatedArgument() {
        // Behavior: A keyword that is already in the bitmask is rejected.
        long seen = ProtocolMessageUtil.markArgumentSeen(0, Keyword.LIMIT);

        IllegalCommandArgumentException exception = assertThrows(
                IllegalCommandArgumentException.class,
                () -> ProtocolMessageUtil.markArgumentSeen(seen, Keyword.LIMIT)
        );
        assertEquals("Duplicate 'LIMIT' argument", exception.getMessage());
    }

    @Test
    void shouldUseWireNameInErrorMessage() {
        // Behavior: The explicit name overload reports the keyword as it is written on the wire,
        // not the enum constant name.
        long seen = ProtocolMessageUtil.markArgumentSeen(0, Keyword.SELECTOR, "BEGIN-KEY-SELECTOR");

        IllegalCommandArgumentException exception = assertThrows(
                IllegalCommandArgumentException.class,
                () -> ProtocolMessageUtil.markArgumentSeen(seen, Keyword.SELECTOR, "BEGIN-KEY-SELECTOR")
        );
        assertEquals("Duplicate 'BEGIN-KEY-SELECTOR' argument", exception.getMessage());
    }

    @Test
    void shouldReturnValueThatFollowsKeyword() {
        // Behavior: requireValue returns the buffer right after the keyword index.
        ByteBuf keyword = Unpooled.copiedBuffer("LIMIT", StandardCharsets.US_ASCII);
        ByteBuf value = Unpooled.copiedBuffer("3", StandardCharsets.US_ASCII);
        List<ByteBuf> params = List.of(keyword, value);

        ByteBuf actual = ProtocolMessageUtil.requireValue(
                params, 0, "LIMIT", ProtocolMessageUtil.POSITIVE_INTEGER);

        assertSame(value, actual);
    }

    @Test
    void shouldRejectKeywordWithoutValue() {
        // Behavior: A keyword that is the last argument fails with the expected value in the message.
        List<ByteBuf> params = List.of(Unpooled.copiedBuffer("LIMIT", StandardCharsets.US_ASCII));

        IllegalCommandArgumentException exception = assertThrows(
                IllegalCommandArgumentException.class,
                () -> ProtocolMessageUtil.requireValue(
                        params, 0, "LIMIT", ProtocolMessageUtil.POSITIVE_INTEGER)
        );
        assertEquals("LIMIT argument must be followed by a positive integer", exception.getMessage());
    }

    @Test
    void shouldBuildIllegalValueError() {
        // Behavior: illegalValue formats the keyword and the expected value into one message.
        IllegalCommandArgumentException exception = ProtocolMessageUtil.illegalValue(
                "KEY-SELECTOR", ProtocolMessageUtil.VALID_KEY_SELECTOR);

        assertEquals("KEY-SELECTOR argument must be followed by a valid key selector", exception.getMessage());
    }

    enum Keyword {
        LIMIT,
        REVERSE,
        SELECTOR
    }
}
