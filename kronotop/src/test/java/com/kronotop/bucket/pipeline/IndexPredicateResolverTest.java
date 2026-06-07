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

package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.planner.Operator;
import org.bson.*;
import org.bson.types.Decimal128;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class IndexPredicateResolverTest {

    private static Operand lit(BqlValue v) {
        return new Operand.Literal(v);
    }

    private static Operand param(int index) {
        return new Operand.Param(new ParamRef(index));
    }

    @Test
    void shouldConvertInt32ValForInt32Index() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.INT32, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, lit(new Int32Val(42)));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonInt32.class, result);
        assertEquals(42, ((BsonInt32) result).getValue());
    }

    @Test
    void shouldReturnNullWhenInt64ValUsedWithInt32Index() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.INT32, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, lit(new Int64Val(42L)));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNull(result);
    }

    @Test
    void shouldConvertInt64ValForInt64Index() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.INT64, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, lit(new Int64Val(42L)));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonInt64.class, result);
        assertEquals(42L, ((BsonInt64) result).getValue());
    }

    @Test
    void shouldWidenInt32ValToInt64ForInt64Index() {
        // Behavior: INT32 predicate value is losslessly widened to INT64 for an INT64 index.
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.INT64, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, lit(new Int32Val(42)));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonInt64.class, result);
        assertEquals(42L, ((BsonInt64) result).getValue());
    }

    @Test
    void shouldConvertStringValForStringIndex() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.STRING, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, lit(new StringVal("hello")));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonString.class, result);
        assertEquals("hello", ((BsonString) result).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithStringIndex() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.STRING, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, lit(new Int32Val(42)));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNull(result);
    }

    @Test
    void shouldConvertDoubleValForDoubleIndex() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.DOUBLE, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, lit(new DoubleVal(3.14)));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonDouble.class, result);
        assertEquals(3.14, ((BsonDouble) result).getValue());
    }

    @Test
    void shouldWidenInt32ValToDoubleForDoubleIndex() {
        // Behavior: INT32 predicate value is losslessly widened to DOUBLE for a DOUBLE index.
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.DOUBLE, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, lit(new Int32Val(42)));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonDouble.class, result);
        assertEquals(42.0, ((BsonDouble) result).getValue());
    }

    @Test
    void shouldConvertBinaryValForBinaryIndex() {
        byte[] data = new byte[]{0x01, 0x02, 0x03, 0x04};
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.BINARY, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, lit(new BinaryVal(data)));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonBinary.class, result);
        assertArrayEquals(data, ((BsonBinary) result).getData());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithBinaryIndex() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.BINARY, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, lit(new Int32Val(42)));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNull(result);
    }

    @Test
    void shouldConvertBooleanValForBooleanIndex() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.BOOLEAN, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.EQ, lit(new BooleanVal(true)));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonBoolean.class, result);
        assertTrue(((BsonBoolean) result).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithBooleanIndex() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.BOOLEAN, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.EQ, lit(new Int32Val(42)));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNull(result);
    }

    @Test
    void shouldConvertDateTimeValForDateTimeIndex() {
        long timestamp = 1703462400000L; // 2023-12-25 00:00:00 UTC
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.DATE_TIME, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, lit(new DateTimeVal(timestamp)));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonDateTime.class, result);
        assertEquals(timestamp, ((BsonDateTime) result).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithDateTimeIndex() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.DATE_TIME, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, lit(new Int32Val(42)));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNull(result);
    }

    @Test
    void shouldConvertTimestampValForTimestampIndex() {
        long timestamp = 7215996951904567296L;
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.TIMESTAMP, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, lit(new TimestampVal(timestamp)));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonTimestamp.class, result);
        assertEquals(timestamp, ((BsonTimestamp) result).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithTimestampIndex() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.TIMESTAMP, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, lit(new Int32Val(42)));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNull(result);
    }

    @Test
    void shouldConvertDecimal128ValForDecimal128Index() {
        BigDecimal value = new BigDecimal("12345.6789");
        SingleFieldIndexDefinition def = new SingleFieldIndexDefinition(1L, "test-index", "field", BsonType.DECIMAL128, false, null, null);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, lit(new Decimal128Val(value)));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonDecimal128.class, result);
        assertEquals(new Decimal128(value), ((BsonDecimal128) result).getValue());
    }

    // resolveIndexKeyRange tests

    @Test
    void shouldWidenInt32ValToDecimal128ForDecimal128Index() {
        // Behavior: INT32 predicate value is losslessly widened to DECIMAL128 for a DECIMAL128 index.
        SingleFieldIndexDefinition def = new SingleFieldIndexDefinition(1L, "test-index", "field", BsonType.DECIMAL128, false, null, null);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.LT, lit(new Int32Val(42)));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonDecimal128.class, result);
        assertEquals(new Decimal128(BigDecimal.valueOf(42)), ((BsonDecimal128) result).getValue());
    }

    @Test
    void shouldResolveObjectIdRangeForObjectIdIndex() {
        // Behavior: ObjectIdVal bounds are converted to BsonObjectId for OBJECT_ID range scans.
        ObjectId lower = new ObjectId();
        ObjectId upper = new ObjectId();
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.OBJECT_ID, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new ObjectIdVal(lower)), lit(new ObjectIdVal(upper)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonObjectId.class, result.lower());
        assertInstanceOf(BsonObjectId.class, result.upper());
        assertEquals(lower, ((BsonObjectId) result.lower()).getValue());
        assertEquals(upper, ((BsonObjectId) result.upper()).getValue());
    }

    @Test
    void shouldReturnNullWhenStringValUsedWithObjectIdRangeIndex() {
        // Behavior: StringVal bounds do not match OBJECT_ID index type, so null is returned.
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.OBJECT_ID, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new StringVal("abc")), lit(new StringVal("xyz")), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNull(result);
    }

    @Test
    void shouldResolveInt32RangeForInt32Index() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.INT32, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new Int32Val(10)), lit(new Int32Val(50)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonInt32.class, result.lower());
        assertInstanceOf(BsonInt32.class, result.upper());
        assertEquals(10, ((BsonInt32) result.lower()).getValue());
        assertEquals(50, ((BsonInt32) result.upper()).getValue());
    }

    @Test
    void shouldReturnNullWhenInt64ValUsedWithInt32RangeIndex() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.INT32, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new Int64Val(10L)), lit(new Int64Val(50L)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNull(result);
    }

    @Test
    void shouldResolveInt64RangeForInt64Index() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.INT64, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new Int64Val(100L)), lit(new Int64Val(500L)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonInt64.class, result.lower());
        assertInstanceOf(BsonInt64.class, result.upper());
        assertEquals(100L, ((BsonInt64) result.lower()).getValue());
        assertEquals(500L, ((BsonInt64) result.upper()).getValue());
    }

    @Test
    void shouldWidenInt32BoundsToInt64ForInt64RangeIndex() {
        // Behavior: INT32 range bounds are losslessly widened to INT64 for an INT64 range index.
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.INT64, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new Int32Val(10)), lit(new Int32Val(50)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonInt64.class, result.lower());
        assertInstanceOf(BsonInt64.class, result.upper());
        assertEquals(10L, ((BsonInt64) result.lower()).getValue());
        assertEquals(50L, ((BsonInt64) result.upper()).getValue());
    }

    @Test
    void shouldResolveStringRangeForStringIndex() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.STRING, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new StringVal("apple")), lit(new StringVal("orange")), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonString.class, result.lower());
        assertInstanceOf(BsonString.class, result.upper());
        assertEquals("apple", ((BsonString) result.lower()).getValue());
        assertEquals("orange", ((BsonString) result.upper()).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithStringRangeIndex() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.STRING, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new Int32Val(10)), lit(new Int32Val(50)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNull(result);
    }

    @Test
    void shouldResolveDoubleRangeForDoubleIndex() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.DOUBLE, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new DoubleVal(1.5)), lit(new DoubleVal(9.9)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonDouble.class, result.lower());
        assertInstanceOf(BsonDouble.class, result.upper());
        assertEquals(1.5, ((BsonDouble) result.lower()).getValue());
        assertEquals(9.9, ((BsonDouble) result.upper()).getValue());
    }

    @Test
    void shouldWidenInt32BoundsToDoubleForDoubleRangeIndex() {
        // Behavior: INT32 range bounds are losslessly widened to DOUBLE for a DOUBLE range index.
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.DOUBLE, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new Int32Val(10)), lit(new Int32Val(50)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonDouble.class, result.lower());
        assertInstanceOf(BsonDouble.class, result.upper());
        assertEquals(10.0, ((BsonDouble) result.lower()).getValue());
        assertEquals(50.0, ((BsonDouble) result.upper()).getValue());
    }

    @Test
    void shouldResolveBinaryRangeForBinaryIndex() {
        byte[] lower = new byte[]{0x01, 0x02};
        byte[] upper = new byte[]{0x0A, 0x0B};
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.BINARY, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new BinaryVal(lower)), lit(new BinaryVal(upper)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonBinary.class, result.lower());
        assertInstanceOf(BsonBinary.class, result.upper());
        assertArrayEquals(lower, ((BsonBinary) result.lower()).getData());
        assertArrayEquals(upper, ((BsonBinary) result.upper()).getData());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithBinaryRangeIndex() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.BINARY, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new Int32Val(10)), lit(new Int32Val(50)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNull(result);
    }

    @Test
    void shouldResolveBooleanRangeForBooleanIndex() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.BOOLEAN, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new BooleanVal(false)), lit(new BooleanVal(true)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonBoolean.class, result.lower());
        assertInstanceOf(BsonBoolean.class, result.upper());
        assertFalse(((BsonBoolean) result.lower()).getValue());
        assertTrue(((BsonBoolean) result.upper()).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithBooleanRangeIndex() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.BOOLEAN, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new Int32Val(0)), lit(new Int32Val(1)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNull(result);
    }

    @Test
    void shouldResolveDateTimeRangeForDateTimeIndex() {
        long lower = 1703462400000L; // 2023-12-25 00:00:00 UTC
        long upper = 1703548800000L; // 2023-12-26 00:00:00 UTC
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.DATE_TIME, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new DateTimeVal(lower)), lit(new DateTimeVal(upper)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonDateTime.class, result.lower());
        assertInstanceOf(BsonDateTime.class, result.upper());
        assertEquals(lower, ((BsonDateTime) result.lower()).getValue());
        assertEquals(upper, ((BsonDateTime) result.upper()).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithDateTimeRangeIndex() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.DATE_TIME, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new Int32Val(10)), lit(new Int32Val(50)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNull(result);
    }

    @Test
    void shouldResolveTimestampRangeForTimestampIndex() {
        long lower = 7215996951904567296L;
        long upper = 7215996951904567300L;
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.TIMESTAMP, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new TimestampVal(lower)), lit(new TimestampVal(upper)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonTimestamp.class, result.lower());
        assertInstanceOf(BsonTimestamp.class, result.upper());
        assertEquals(lower, ((BsonTimestamp) result.lower()).getValue());
        assertEquals(upper, ((BsonTimestamp) result.upper()).getValue());
    }

    @Test
    void shouldReturnNullWhenInt32ValUsedWithTimestampRangeIndex() {
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.TIMESTAMP, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new Int32Val(10)), lit(new Int32Val(50)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNull(result);
    }

    @Test
    void shouldResolveDecimal128RangeForDecimal128Index() {
        BigDecimal lower = new BigDecimal("100.50");
        BigDecimal upper = new BigDecimal("999.99");
        SingleFieldIndexDefinition def = new SingleFieldIndexDefinition(1L, "test-index", "field", BsonType.DECIMAL128, false, null, null);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new Decimal128Val(lower)), lit(new Decimal128Val(upper)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonDecimal128.class, result.lower());
        assertInstanceOf(BsonDecimal128.class, result.upper());
        assertEquals(new Decimal128(lower), ((BsonDecimal128) result.lower()).getValue());
        assertEquals(new Decimal128(upper), ((BsonDecimal128) result.upper()).getValue());
    }

    // Parameterized predicate tests

    @Test
    void shouldWidenInt32BoundsToDecimal128ForDecimal128RangeIndex() {
        // Behavior: INT32 range bounds are losslessly widened to DECIMAL128 for a DECIMAL128 range index.
        SingleFieldIndexDefinition def = new SingleFieldIndexDefinition(1L, "test-index", "field", BsonType.DECIMAL128, false, null, null);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new Int32Val(10)), lit(new Int32Val(50)), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(def, node, Collections.emptyList());

        assertNotNull(result);
        assertInstanceOf(BsonDecimal128.class, result.lower());
        assertInstanceOf(BsonDecimal128.class, result.upper());
        assertEquals(new Decimal128(BigDecimal.valueOf(10)), ((BsonDecimal128) result.lower()).getValue());
        assertEquals(new Decimal128(BigDecimal.valueOf(50)), ((BsonDecimal128) result.upper()).getValue());
    }

    @Test
    void shouldResolveParamOperand() {
        // Behavior: Param(0) resolves to the first element in the parameters list.
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.INT32, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.EQ, param(0));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, List.of(new Int32Val(42)));

        assertNotNull(result);
        assertInstanceOf(BsonInt32.class, result);
        assertEquals(42, ((BsonInt32) result).getValue());
    }

    @Test
    void shouldReturnNullWhenResolvedParamTypeMismatches() {
        // Behavior: When a Param resolves to a value with wrong type, null is returned.
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.INT32, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.EQ, param(0));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, List.of(new StringVal("wrong-type")));

        assertNull(result);
    }

    @Test
    void shouldResolveNonZeroParamIndex() {
        // Behavior: Param(1) resolves to the second element in the parameters list.
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.STRING, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "field", Operator.EQ, param(1));
        IndexScanNode node = new IndexScanNode(1, def, predicate);

        BsonValue result = IndexPredicateResolver.resolveIndexKeyValue(def, node, List.of(new Int32Val(100), new StringVal("target")));

        assertNotNull(result);
        assertInstanceOf(BsonString.class, result);
        assertEquals("target", ((BsonString) result).getValue());
    }

    @Test
    void shouldResolveParamBoundsForRangeScan() {
        // Behavior: Both lower and upper bounds can be Param operands resolved from parameters.
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.INT32, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", param(0), param(1), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(
                def, node, List.of(new Int32Val(10), new Int32Val(50)));

        assertNotNull(result);
        assertEquals(10, ((BsonInt32) result.lower()).getValue());
        assertEquals(50, ((BsonInt32) result.upper()).getValue());
    }

    @Test
    void shouldResolveMixedLiteralAndParamBounds() {
        // Behavior: Lower bound can be a Literal while upper bound is a Param.
        SingleFieldIndexDefinition def = SingleFieldIndexDefinition.create("test-index", "field", BsonType.INT32, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("field", lit(new Int32Val(10)), param(0), true, false);
        RangeScanNode node = new RangeScanNode(1, def, predicate);

        IndexPredicateResolver.IndexKeyRange result = IndexPredicateResolver.resolveIndexKeyRange(
                def, node, List.of(new Int32Val(50)));

        assertNotNull(result);
        assertEquals(10, ((BsonInt32) result.lower()).getValue());
        assertEquals(50, ((BsonInt32) result.upper()).getValue());
    }
}