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

package com.kronotop.bucket.handlers.protocol;

import com.kronotop.KronotopException;
import com.kronotop.server.IllegalCommandArgumentException;
import com.kronotop.server.Request;
import com.kronotop.server.Session;
import com.kronotop.server.resp3.RedisMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;
import io.netty.util.DefaultAttributeMap;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class AbstractBucketMessageTest {

    private final TestBucketMessage message = new TestBucketMessage();

    @Test
    void shouldParseLimitArgument() {
        // Behavior: LIMIT followed by a positive integer sets the limit value.
        Request request = new TestRequest("bucket", "{}", "LIMIT", "10");
        Set<QueryArgumentKey> supported = Set.of(QueryArgumentKey.LIMIT);

        QueryArguments args = message.parseCommonQueryArguments(request, 2, supported);

        assertEquals(10, args.getLimit());
    }

    @Test
    void shouldParseLimitArgumentCaseInsensitive() {
        // Behavior: LIMIT argument parsing is case-insensitive.
        Request request = new TestRequest("bucket", "{}", "limit", "5");
        Set<QueryArgumentKey> supported = Set.of(QueryArgumentKey.LIMIT);

        QueryArguments args = message.parseCommonQueryArguments(request, 2, supported);

        assertEquals(5, args.getLimit());
    }

    @Test
    void shouldParseSortByArgument() {
        // Behavior: SORTBY followed by field name and direction sets sort configuration.
        Request request = new TestRequest("bucket", "{}", "SORTBY", "age", "ASC");
        Set<QueryArgumentKey> supported = Set.of(QueryArgumentKey.SORTBY);

        QueryArguments args = message.parseCommonQueryArguments(request, 2, supported);

        assertEquals("age", args.getSortBy());
        assertEquals(SortDirection.ASC, args.getSortDirection());
    }

    @Test
    void shouldParseSortByWithDescDirection() {
        // Behavior: SORTBY with DESC direction sets descending sort order.
        Request request = new TestRequest("bucket", "{}", "SORTBY", "name", "DESC");
        Set<QueryArgumentKey> supported = Set.of(QueryArgumentKey.SORTBY);

        QueryArguments args = message.parseCommonQueryArguments(request, 2, supported);

        assertEquals("name", args.getSortBy());
        assertEquals(SortDirection.DESC, args.getSortDirection());
    }

    @Test
    void shouldParseSortByArgumentCaseInsensitive() {
        // Behavior: SORTBY argument and direction parsing are case-insensitive.
        Request request = new TestRequest("bucket", "{}", "sortby", "field", "asc");
        Set<QueryArgumentKey> supported = Set.of(QueryArgumentKey.SORTBY);

        QueryArguments args = message.parseCommonQueryArguments(request, 2, supported);

        assertEquals("field", args.getSortBy());
        assertEquals(SortDirection.ASC, args.getSortDirection());
    }

    @Test
    void shouldParseBothLimitAndSortBy() {
        // Behavior: Multiple arguments can be combined in any order.
        Request request = new TestRequest("bucket", "{}", "LIMIT", "20", "SORTBY", "created", "DESC");
        Set<QueryArgumentKey> supported = Set.of(QueryArgumentKey.LIMIT, QueryArgumentKey.SORTBY);

        QueryArguments args = message.parseCommonQueryArguments(request, 2, supported);

        assertEquals(20, args.getLimit());
        assertEquals("created", args.getSortBy());
        assertEquals(SortDirection.DESC, args.getSortDirection());
    }

    @Test
    void shouldReturnEmptyArgumentsWhenNoArgumentsProvided() {
        // Behavior: When no optional arguments are provided, returns defaults (0 limit, null sort).
        Request request = new TestRequest("bucket", "{}");
        Set<QueryArgumentKey> supported = Set.of(QueryArgumentKey.LIMIT, QueryArgumentKey.SORTBY);

        QueryArguments args = message.parseCommonQueryArguments(request, 2, supported);

        assertEquals(0, args.getLimit());
        assertNull(args.getSortBy());
        assertNull(args.getSortDirection());
    }

    @Test
    void shouldThrowOnUnknownArgument() {
        // Behavior: Unknown argument names throw IllegalCommandArgumentException.
        Request request = new TestRequest("bucket", "{}", "UNKNOWN");
        Set<QueryArgumentKey> supported = Set.of(QueryArgumentKey.LIMIT);

        IllegalCommandArgumentException ex = assertThrows(
                IllegalCommandArgumentException.class,
                () -> message.parseCommonQueryArguments(request, 2, supported)
        );

        assertTrue(ex.getMessage().contains("Unknown"));
    }

    @Test
    void shouldThrowWhenLimitMissingValue() {
        // Behavior: LIMIT without a following value throws IllegalCommandArgumentException.
        Request request = new TestRequest("bucket", "{}", "LIMIT");
        Set<QueryArgumentKey> supported = Set.of(QueryArgumentKey.LIMIT);

        IllegalCommandArgumentException ex = assertThrows(
                IllegalCommandArgumentException.class,
                () -> message.parseCommonQueryArguments(request, 2, supported)
        );

        assertTrue(ex.getMessage().contains("LIMIT"));
    }

    @Test
    void shouldThrowWhenLimitIsNegative() {
        // Behavior: LIMIT with a negative value throws IllegalCommandArgumentException.
        Request request = new TestRequest("bucket", "{}", "LIMIT", "-5");
        Set<QueryArgumentKey> supported = Set.of(QueryArgumentKey.LIMIT);

        IllegalCommandArgumentException ex = assertThrows(
                IllegalCommandArgumentException.class,
                () -> message.parseCommonQueryArguments(request, 2, supported)
        );

        assertTrue(ex.getMessage().contains("non-negative"));
    }

    @Test
    void shouldAcceptZeroLimit() {
        // Behavior: LIMIT with zero is valid (means no limit).
        Request request = new TestRequest("bucket", "{}", "LIMIT", "0");
        Set<QueryArgumentKey> supported = Set.of(QueryArgumentKey.LIMIT);

        QueryArguments args = message.parseCommonQueryArguments(request, 2, supported);

        assertEquals(0, args.getLimit());
    }

    @Test
    void shouldThrowWhenSortByMissingFieldAndDirection() {
        // Behavior: SORTBY without field and direction throws IllegalCommandArgumentException.
        Request request = new TestRequest("bucket", "{}", "SORTBY");
        Set<QueryArgumentKey> supported = Set.of(QueryArgumentKey.SORTBY);

        IllegalCommandArgumentException ex = assertThrows(
                IllegalCommandArgumentException.class,
                () -> message.parseCommonQueryArguments(request, 2, supported)
        );

        assertTrue(ex.getMessage().contains("SORTBY"));
    }

    @Test
    void shouldThrowWhenSortByMissingDirection() {
        // Behavior: SORTBY with field but missing direction throws IllegalCommandArgumentException.
        Request request = new TestRequest("bucket", "{}", "SORTBY", "field");
        Set<QueryArgumentKey> supported = Set.of(QueryArgumentKey.SORTBY);

        IllegalCommandArgumentException ex = assertThrows(
                IllegalCommandArgumentException.class,
                () -> message.parseCommonQueryArguments(request, 2, supported)
        );

        assertTrue(ex.getMessage().contains("SORTBY"));
    }

    @Test
    void shouldThrowOnInvalidSortDirection() {
        // Behavior: Invalid sort direction (not ASC or DESC) throws KronotopException.
        Request request = new TestRequest("bucket", "{}", "SORTBY", "field", "INVALID");
        Set<QueryArgumentKey> supported = Set.of(QueryArgumentKey.SORTBY);

        KronotopException ex = assertThrows(
                KronotopException.class,
                () -> message.parseCommonQueryArguments(request, 2, supported)
        );

        assertTrue(ex.getMessage().contains("Invalid sort direction"));
    }

    @Test
    void shouldThrowWhenLimitNotSupported() {
        // Behavior: Using LIMIT when not in the supported set throws UnsupportedArgumentException.
        Request request = new TestRequest("bucket", "{}", "LIMIT", "10");
        Set<QueryArgumentKey> supported = Set.of(QueryArgumentKey.SORTBY);

        UnsupportedArgumentException ex = assertThrows(
                UnsupportedArgumentException.class,
                () -> message.parseCommonQueryArguments(request, 2, supported)
        );

        assertTrue(ex.getMessage().contains("LIMIT"));
    }

    @Test
    void shouldThrowWhenSortByNotSupported() {
        // Behavior: Using SORTBY when not in the supported set throws UnsupportedArgumentException.
        Request request = new TestRequest("bucket", "{}", "SORTBY", "field", "ASC");
        Set<QueryArgumentKey> supported = Set.of(QueryArgumentKey.LIMIT);

        UnsupportedArgumentException ex = assertThrows(
                UnsupportedArgumentException.class,
                () -> message.parseCommonQueryArguments(request, 2, supported)
        );

        assertTrue(ex.getMessage().contains("SORTBY"));
    }

    /**
     * Concrete implementation for testing the protected method.
     */
    private static class TestBucketMessage extends AbstractBucketMessage {
        @Override
        public QueryArguments parseCommonQueryArguments(Request request, int index, Set<QueryArgumentKey> supportedArguments) {
            return super.parseCommonQueryArguments(request, index, supportedArguments);
        }
    }

    /**
     * Minimal Request implementation for testing.
     */
    private static class TestRequest extends DefaultAttributeMap implements Request {
        private final ArrayList<ByteBuf> params;

        TestRequest(String... args) {
            this.params = new ArrayList<>();
            for (String arg : args) {
                params.add(Unpooled.copiedBuffer(arg, CharsetUtil.UTF_8));
            }
        }

        @Override
        public ArrayList<ByteBuf> getParams() {
            return params;
        }

        @Override
        public String getCommand() {
            return "TEST";
        }

        @Override
        public RedisMessage getRedisMessage() {
            return null;
        }

        @Override
        public Session getSession() {
            return null;
        }
    }
}
