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

import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.NullRedisMessage;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RESPUtilTest {

    @Test
    void shouldReturnBulkStringNullForRESP2() {
        // Behavior: RESP2 has no null type, null is a bulk string with a length of -1.
        assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, RESPUtil.nullMessage(RESPVersion.RESP2));
    }

    @Test
    void shouldReturnNullMessageForRESP3() {
        // Behavior: RESP3 has its own null type.
        assertEquals(NullRedisMessage.INSTANCE, RESPUtil.nullMessage(RESPVersion.RESP3));
    }
}
