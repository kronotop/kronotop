/*
 * Copyright (c) 2023-2025 Burak Sezer
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

package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.planner.Operator;
import org.bson.*;
import org.bson.types.Decimal128;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

class IndexPredicateResolverTest {
    @Test
    void shouldConvertInt32ValForInt32Index() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.INT32);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, new Int32Val(42));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonInt32.class, result);
        assertEquals(42, ((BsonInt32) result).getValue());
    }

    @Test
    void shouldReturnNullWhenInt64ValUsedWithInt32Index() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.INT32);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, new Int64Val(42L));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNull(result);
    }

    @Test
    void shouldConvertInt64ValForInt64Index() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.INT64);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, new Int64Val(42L));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonInt64.class, result);
        assertEquals(42L, ((BsonInt64) result).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithInt64Index() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.INT64);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, new Int32Val(42));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNull(result);
    }

    @Test
    void shouldConvertStringValForStringIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.STRING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, new StringVal("hello"));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonString.class, result);
        assertEquals("hello", ((BsonString) result).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithStringIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.STRING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, new Int32Val(42));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNull(result);
    }

    @Test
    void shouldConvertDoubleValForDoubleIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.DOUBLE);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, new DoubleVal(3.14));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonDouble.class, result);
        assertEquals(3.14, ((BsonDouble) result).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithDoubleIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.DOUBLE);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, new Int32Val(42));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNull(result);
    }

    @Test
    void shouldConvertBinaryValForBinaryIndex() {
        byte[] data = new byte[]{0x01, 0x02, 0x03, 0x04};
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.BINARY);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, new BinaryVal(data));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonBinary.class, result);
        assertArrayEquals(data, ((BsonBinary) result).getData());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithBinaryIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.BINARY);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, new Int32Val(42));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNull(result);
    }

    @Test
    void shouldConvertBooleanValForBooleanIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.BOOLEAN);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.EQ, new BooleanVal(true));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonBoolean.class, result);
        assertTrue(((BsonBoolean) result).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithBooleanIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.BOOLEAN);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.EQ, new Int32Val(42));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNull(result);
    }

    @Test
    void shouldConvertDateTimeValForDateTimeIndex() {
        long timestamp = 1703462400000L; // 2023-12-25 00:00:00 UTC
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.DATE_TIME);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, new DateTimeVal(timestamp));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonDateTime.class, result);
        assertEquals(timestamp, ((BsonDateTime) result).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithDateTimeIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.DATE_TIME);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, new Int32Val(42));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNull(result);
    }

    @Test
    void shouldConvertTimestampValForTimestampIndex() {
        long timestamp = 7215996951904567296L;
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.TIMESTAMP);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, new TimestampVal(timestamp));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonTimestamp.class, result);
        assertEquals(timestamp, ((BsonTimestamp) result).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithTimestampIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.TIMESTAMP);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, new Int32Val(42));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNull(result);
    }

    @Test
    void shouldConvertDecimal128ValForDecimal128Index() {
        BigDecimal value = new BigDecimal("12345.6789");
        IndexDefinition def = new IndexDefinition(1L, "test-index", "field", BsonType.DECIMAL128, null);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, new Decimal128Val(value));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonDecimal128.class, result);
        assertEquals(new Decimal128(value), ((BsonDecimal128) result).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithDecimal128Index() {
        IndexDefinition def = new IndexDefinition(1L, "test-index", "field", BsonType.DECIMAL128, null);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, new Int32Val(42));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node);

        assertNull(result);
    }

    // resolveIndexKeyRange tests

    @Test
    void shouldResolveInt32RangeForInt32Index() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.INT32);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new Int32Val(10), new Int32Val(50), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonInt32.class, result.lower());
        assertInstanceOf(BsonInt32.class, result.upper());
        assertEquals(10, ((BsonInt32) result.lower()).getValue());
        assertEquals(50, ((BsonInt32) result.upper()).getValue());
    }

    @Test
    void shouldReturnNullWhenInt64ValUsedWithInt32RangeIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.INT32);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new Int64Val(10L), new Int64Val(50L), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNull(result);
    }

    @Test
    void shouldResolveInt64RangeForInt64Index() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.INT64);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new Int64Val(100L), new Int64Val(500L), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonInt64.class, result.lower());
        assertInstanceOf(BsonInt64.class, result.upper());
        assertEquals(100L, ((BsonInt64) result.lower()).getValue());
        assertEquals(500L, ((BsonInt64) result.upper()).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithInt64RangeIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.INT64);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new Int32Val(10), new Int32Val(50), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNull(result);
    }

    @Test
    void shouldResolveStringRangeForStringIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.STRING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new StringVal("apple"), new StringVal("orange"), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonString.class, result.lower());
        assertInstanceOf(BsonString.class, result.upper());
        assertEquals("apple", ((BsonString) result.lower()).getValue());
        assertEquals("orange", ((BsonString) result.upper()).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithStringRangeIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.STRING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new Int32Val(10), new Int32Val(50), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNull(result);
    }

    @Test
    void shouldResolveDoubleRangeForDoubleIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.DOUBLE);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new DoubleVal(1.5), new DoubleVal(9.9), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonDouble.class, result.lower());
        assertInstanceOf(BsonDouble.class, result.upper());
        assertEquals(1.5, ((BsonDouble) result.lower()).getValue());
        assertEquals(9.9, ((BsonDouble) result.upper()).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithDoubleRangeIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.DOUBLE);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new Int32Val(10), new Int32Val(50), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNull(result);
    }

    @Test
    void shouldResolveBinaryRangeForBinaryIndex() {
        byte[] lower = new byte[]{0x01, 0x02};
        byte[] upper = new byte[]{0x0A, 0x0B};
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.BINARY);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new BinaryVal(lower), new BinaryVal(upper), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonBinary.class, result.lower());
        assertInstanceOf(BsonBinary.class, result.upper());
        assertArrayEquals(lower, ((BsonBinary) result.lower()).getData());
        assertArrayEquals(upper, ((BsonBinary) result.upper()).getData());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithBinaryRangeIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.BINARY);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new Int32Val(10), new Int32Val(50), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNull(result);
    }

    @Test
    void shouldResolveBooleanRangeForBooleanIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.BOOLEAN);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new BooleanVal(false), new BooleanVal(true), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonBoolean.class, result.lower());
        assertInstanceOf(BsonBoolean.class, result.upper());
        assertFalse(((BsonBoolean) result.lower()).getValue());
        assertTrue(((BsonBoolean) result.upper()).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithBooleanRangeIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.BOOLEAN);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new Int32Val(0), new Int32Val(1), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNull(result);
    }

    @Test
    void shouldResolveDateTimeRangeForDateTimeIndex() {
        long lower = 1703462400000L; // 2023-12-25 00:00:00 UTC
        long upper = 1703548800000L; // 2023-12-26 00:00:00 UTC
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.DATE_TIME);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new DateTimeVal(lower), new DateTimeVal(upper), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonDateTime.class, result.lower());
        assertInstanceOf(BsonDateTime.class, result.upper());
        assertEquals(lower, ((BsonDateTime) result.lower()).getValue());
        assertEquals(upper, ((BsonDateTime) result.upper()).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithDateTimeRangeIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.DATE_TIME);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new Int32Val(10), new Int32Val(50), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNull(result);
    }

    @Test
    void shouldResolveTimestampRangeForTimestampIndex() {
        long lower = 7215996951904567296L;
        long upper = 7215996951904567300L;
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.TIMESTAMP);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new TimestampVal(lower), new TimestampVal(upper), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonTimestamp.class, result.lower());
        assertInstanceOf(BsonTimestamp.class, result.upper());
        assertEquals(lower, ((BsonTimestamp) result.lower()).getValue());
        assertEquals(upper, ((BsonTimestamp) result.upper()).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithTimestampRangeIndex() {
        IndexDefinition def = IndexDefinition.create("test-index", "field", BsonType.TIMESTAMP);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new Int32Val(10), new Int32Val(50), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNull(result);
    }

    @Test
    void shouldResolveDecimal128RangeForDecimal128Index() {
        BigDecimal lower = new BigDecimal("100.50");
        BigDecimal upper = new BigDecimal("999.99");
        IndexDefinition def = new IndexDefinition(1L, "test-index", "field", BsonType.DECIMAL128, null);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new Decimal128Val(lower), new Decimal128Val(upper), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNotNull(result);
        assertInstanceOf(BsonDecimal128.class, result.lower());
        assertInstanceOf(BsonDecimal128.class, result.upper());
        assertEquals(new Decimal128(lower), ((BsonDecimal128) result.lower()).getValue());
        assertEquals(new Decimal128(upper), ((BsonDecimal128) result.upper()).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithDecimal128RangeIndex() {
        IndexDefinition def = new IndexDefinition(1L, "test-index", "field", BsonType.DECIMAL128, null);
        RangeScanPredicate predicate = new RangeScanPredicate("field", new Int32Val(10), new Int32Val(50), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node);

        assertNull(result);
    }
}