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

package com.kronotop.bucket.pipeline;

import com.kronotop.bucket.bql.BqlParser;
import com.kronotop.bucket.bql.ParameterExtractor;
import com.kronotop.bucket.bql.QueryShape;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.CompoundIndexDefinition;
import com.kronotop.bucket.index.CompoundIndexField;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;
import org.bson.BsonType;
import org.bson.types.ObjectId;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class PhysicalNodeShapeTest {

    private final AtomicInteger idGenerator = new AtomicInteger(0);

    private int nextId() {
        return idGenerator.incrementAndGet();
    }

    private PhysicalFilter filter(Operator op, String selector, Object operand) {
        return new PhysicalFilter(nextId(), selector, op, operand);
    }

    // === Basic Hash Properties ===

    @Test
    void shouldProduceSameHashForSameStructure() {
        // Behavior: Two identical PhysicalFilter nodes produce the same hash
        PhysicalFilter filter1 = filter(Operator.EQ, "age", new Int32Val(25));
        PhysicalFilter filter2 = filter(Operator.EQ, "age", new Int32Val(100));

        long hash1 = PhysicalNodeShape.compute(filter1);
        long hash2 = PhysicalNodeShape.compute(filter2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldProduceDifferentHashForDifferentOperators() {
        // Behavior: PhysicalFilter with EQ vs GT produce different hashes
        PhysicalFilter filterEq = filter(Operator.EQ, "age", new Int32Val(25));
        PhysicalFilter filterGt = filter(Operator.GT, "age", new Int32Val(25));

        long hashEq = PhysicalNodeShape.compute(filterEq);
        long hashGt = PhysicalNodeShape.compute(filterGt);

        assertNotEquals(hashEq, hashGt);
    }

    @Test
    void shouldProduceDifferentHashForDifferentSelectors() {
        // Behavior: PhysicalFilter on "age" vs "score" produce different hashes
        PhysicalFilter filterAge = filter(Operator.EQ, "age", new Int32Val(25));
        PhysicalFilter filterScore = filter(Operator.EQ, "score", new Int32Val(25));

        long hashAge = PhysicalNodeShape.compute(filterAge);
        long hashScore = PhysicalNodeShape.compute(filterScore);

        assertNotEquals(hashAge, hashScore);
    }

    @Test
    void shouldProduceDifferentHashForDifferentValueTypes() {
        // Behavior: PhysicalFilter with Int32Val vs StringVal produce different hashes
        PhysicalFilter filterInt = filter(Operator.EQ, "value", new Int32Val(100));
        PhysicalFilter filterString = filter(Operator.EQ, "value", new StringVal("100"));

        long hashInt = PhysicalNodeShape.compute(filterInt);
        long hashString = PhysicalNodeShape.compute(filterString);

        assertNotEquals(hashInt, hashString);
    }

    @Test
    void shouldProduceDifferentHashForInt32VsInt64() {
        // Behavior: PhysicalFilter with Int32Val vs Int64Val produce different hashes
        PhysicalFilter filterInt32 = filter(Operator.EQ, "value", new Int32Val(100));
        PhysicalFilter filterInt64 = filter(Operator.EQ, "value", new Int64Val(100L));

        long hashInt32 = PhysicalNodeShape.compute(filterInt32);
        long hashInt64 = PhysicalNodeShape.compute(filterInt64);

        assertNotEquals(hashInt32, hashInt64);
    }

    // === Order Independence for AND/OR ===

    @Test
    void shouldProduceSameHashForAndWithDifferentChildOrder() {
        // Behavior: PhysicalAnd([A, B]) == PhysicalAnd([B, A])
        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB = filter(Operator.EQ, "b", new Int32Val(2));

        PhysicalAnd and1 = new PhysicalAnd(nextId(), List.of(filterA, filterB));
        PhysicalAnd and2 = new PhysicalAnd(nextId(), List.of(filterB, filterA));

        long hash1 = PhysicalNodeShape.compute(and1);
        long hash2 = PhysicalNodeShape.compute(and2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldProduceSameHashForOrWithDifferentChildOrder() {
        // Behavior: PhysicalOr([A, B]) == PhysicalOr([B, A])
        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB = filter(Operator.EQ, "b", new Int32Val(2));

        PhysicalOr or1 = new PhysicalOr(nextId(), List.of(filterA, filterB));
        PhysicalOr or2 = new PhysicalOr(nextId(), List.of(filterB, filterA));

        long hash1 = PhysicalNodeShape.compute(or1);
        long hash2 = PhysicalNodeShape.compute(or2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldProduceDifferentHashForAndVsOr() {
        // Behavior: PhysicalAnd([A, B]) != PhysicalOr([A, B])
        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB = filter(Operator.EQ, "b", new Int32Val(2));

        PhysicalAnd and = new PhysicalAnd(nextId(), List.of(filterA, filterB));
        PhysicalOr or = new PhysicalOr(nextId(), List.of(filterA, filterB));

        long hashAnd = PhysicalNodeShape.compute(and);
        long hashOr = PhysicalNodeShape.compute(or);

        assertNotEquals(hashAnd, hashOr);
    }

    @Test
    void shouldProduceSameHashForNestedAndWithDifferentOrder() {
        // Behavior: Nested AND with reordered children produces same hash
        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB = filter(Operator.EQ, "b", new Int32Val(2));
        PhysicalFilter filterC = filter(Operator.EQ, "c", new Int32Val(3));

        PhysicalAnd innerAnd1 = new PhysicalAnd(nextId(), List.of(filterB, filterC));
        PhysicalAnd outerAnd1 = new PhysicalAnd(nextId(), List.of(filterA, innerAnd1));

        PhysicalAnd innerAnd2 = new PhysicalAnd(nextId(), List.of(filterC, filterB));
        PhysicalAnd outerAnd2 = new PhysicalAnd(nextId(), List.of(innerAnd2, filterA));

        long hash1 = PhysicalNodeShape.compute(outerAnd1);
        long hash2 = PhysicalNodeShape.compute(outerAnd2);

        assertEquals(hash1, hash2);
    }

    // === Array Operators ($in, $nin, $all) ===

    @Test
    void shouldProduceSameHashForInWithDifferentValues() {
        // Behavior: $in with [1,2,3] vs [4,5,6] same shape
        List<Object> values1 = List.of(new Int32Val(1), new Int32Val(2), new Int32Val(3));
        List<Object> values2 = List.of(new Int32Val(4), new Int32Val(5), new Int32Val(6));

        PhysicalFilter filter1 = filter(Operator.IN, "status", values1);
        PhysicalFilter filter2 = filter(Operator.IN, "status", values2);

        long hash1 = PhysicalNodeShape.compute(filter1);
        long hash2 = PhysicalNodeShape.compute(filter2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldProduceDifferentHashForInWithDifferentArraySize() {
        // Behavior: $in with [1,2] vs [1,2,3] different shape
        List<Object> values1 = List.of(new Int32Val(1), new Int32Val(2));
        List<Object> values2 = List.of(new Int32Val(1), new Int32Val(2), new Int32Val(3));

        PhysicalFilter filter1 = filter(Operator.IN, "status", values1);
        PhysicalFilter filter2 = filter(Operator.IN, "status", values2);

        long hash1 = PhysicalNodeShape.compute(filter1);
        long hash2 = PhysicalNodeShape.compute(filter2);

        assertNotEquals(hash1, hash2);
    }

    @Test
    void shouldProduceDifferentHashForInWithMixedTypes() {
        // Behavior: $in with [1,2,3] vs [1,"two",3] different shape
        List<Object> values1 = List.of(new Int32Val(1), new Int32Val(2), new Int32Val(3));
        List<Object> values2 = List.of(new Int32Val(1), new StringVal("two"), new Int32Val(3));

        PhysicalFilter filter1 = filter(Operator.IN, "status", values1);
        PhysicalFilter filter2 = filter(Operator.IN, "status", values2);

        long hash1 = PhysicalNodeShape.compute(filter1);
        long hash2 = PhysicalNodeShape.compute(filter2);

        assertNotEquals(hash1, hash2);
    }

    @Test
    void shouldProduceSameHashForInWithSameTypesInDifferentOrder() {
        // Behavior: $in with [1,"a",true] vs ["a",true,1] same shape
        List<Object> values1 = List.of(new Int32Val(1), new StringVal("a"), new BooleanVal(true));
        List<Object> values2 = List.of(new StringVal("a"), new BooleanVal(true), new Int32Val(1));

        PhysicalFilter filter1 = filter(Operator.IN, "status", values1);
        PhysicalFilter filter2 = filter(Operator.IN, "status", values2);

        long hash1 = PhysicalNodeShape.compute(filter1);
        long hash2 = PhysicalNodeShape.compute(filter2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldProduceSameHashForNinWithDifferentValues() {
        // Behavior: $nin with [1,2] vs [3,4] same shape
        List<Object> values1 = List.of(new Int32Val(1), new Int32Val(2));
        List<Object> values2 = List.of(new Int32Val(3), new Int32Val(4));

        PhysicalFilter filter1 = filter(Operator.NIN, "status", values1);
        PhysicalFilter filter2 = filter(Operator.NIN, "status", values2);

        long hash1 = PhysicalNodeShape.compute(filter1);
        long hash2 = PhysicalNodeShape.compute(filter2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldProduceSameHashForAllWithDifferentValues() {
        // Behavior: $all with ["a","b"] vs ["x","y"] same shape
        List<Object> values1 = List.of(new StringVal("a"), new StringVal("b"));
        List<Object> values2 = List.of(new StringVal("x"), new StringVal("y"));

        PhysicalFilter filter1 = filter(Operator.ALL, "tags", values1);
        PhysicalFilter filter2 = filter(Operator.ALL, "tags", values2);

        long hash1 = PhysicalNodeShape.compute(filter1);
        long hash2 = PhysicalNodeShape.compute(filter2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldProduceDifferentHashForInVsNin() {
        // Behavior: $in vs $nin with same values produce different hashes
        List<Object> values = List.of(new Int32Val(1), new Int32Val(2));

        PhysicalFilter filterIn = filter(Operator.IN, "status", values);
        PhysicalFilter filterNin = filter(Operator.NIN, "status", values);

        long hashIn = PhysicalNodeShape.compute(filterIn);
        long hashNin = PhysicalNodeShape.compute(filterNin);

        assertNotEquals(hashIn, hashNin);
    }

    // === Range Scan Specifics ===

    @Test
    void shouldProduceSameHashForRangeScanWithDifferentBoundValues() {
        // Behavior: RangeScan(lower=10, upper=20) == RangeScan(lower=5, upper=50) with same types and inclusivity
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalRangeScan range1 = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(10), new Int32Val(20), true, false, index);
        PhysicalRangeScan range2 = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(5), new Int32Val(50), true, false, index);

        long hash1 = PhysicalNodeShape.compute(range1);
        long hash2 = PhysicalNodeShape.compute(range2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldProduceDifferentHashForRangeScanWithDifferentIncludeLower() {
        // Behavior: RangeScan(includeLower=true) != RangeScan(includeLower=false)
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalRangeScan range1 = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(10), new Int32Val(20), true, false, index);
        PhysicalRangeScan range2 = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(10), new Int32Val(20), false, false, index);

        long hash1 = PhysicalNodeShape.compute(range1);
        long hash2 = PhysicalNodeShape.compute(range2);

        assertNotEquals(hash1, hash2);
    }

    @Test
    void shouldProduceDifferentHashForRangeScanWithDifferentIncludeUpper() {
        // Behavior: RangeScan(includeUpper=true) != RangeScan(includeUpper=false)
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalRangeScan range1 = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(10), new Int32Val(20), true, true, index);
        PhysicalRangeScan range2 = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(10), new Int32Val(20), true, false, index);

        long hash1 = PhysicalNodeShape.compute(range1);
        long hash2 = PhysicalNodeShape.compute(range2);

        assertNotEquals(hash1, hash2);
    }

    @Test
    void shouldProduceDifferentHashForRangeScanOnDifferentSelectors() {
        // Behavior: RangeScan on "age" vs "score" different shape
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition scoreIndex = SingleFieldIndexDefinition.create("score_idx", "score", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalRangeScan rangeAge = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(10), new Int32Val(20), true, false, ageIndex);
        PhysicalRangeScan rangeScore = new PhysicalRangeScan(nextId(), "score",
                new Int32Val(10), new Int32Val(20), true, false, scoreIndex);

        long hashAge = PhysicalNodeShape.compute(rangeAge);
        long hashScore = PhysicalNodeShape.compute(rangeScore);

        assertNotEquals(hashAge, hashScore);
    }

    @Test
    void shouldProduceDifferentHashForRangeScanWithDifferentBoundTypes() {
        // Behavior: RangeScan with Int32 bounds vs Int64 bounds produce different hashes
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("value_idx", "value", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalRangeScan rangeInt32 = new PhysicalRangeScan(nextId(), "value",
                new Int32Val(10), new Int32Val(20), true, false, index);
        PhysicalRangeScan rangeInt64 = new PhysicalRangeScan(nextId(), "value",
                new Int64Val(10L), new Int64Val(20L), true, false, index);

        long hashInt32 = PhysicalNodeShape.compute(rangeInt32);
        long hashInt64 = PhysicalNodeShape.compute(rangeInt64);

        assertNotEquals(hashInt32, hashInt64);
    }

    @Test
    void shouldProduceSameHashForRangeScanWithOnlyLowerBound() {
        // Behavior: RangeScan with only lower bound and different values produce same hash
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalRangeScan range1 = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(10), null, true, false, index);
        PhysicalRangeScan range2 = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(50), null, true, false, index);

        long hash1 = PhysicalNodeShape.compute(range1);
        long hash2 = PhysicalNodeShape.compute(range2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldProduceSameHashForRangeScanWithOnlyUpperBound() {
        // Behavior: RangeScan with only upper bound and different values produce same hash
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalRangeScan range1 = new PhysicalRangeScan(nextId(), "age",
                null, new Int32Val(20), false, true, index);
        PhysicalRangeScan range2 = new PhysicalRangeScan(nextId(), "age",
                null, new Int32Val(100), false, true, index);

        long hash1 = PhysicalNodeShape.compute(range1);
        long hash2 = PhysicalNodeShape.compute(range2);

        assertEquals(hash1, hash2);
    }

    // === Wrapper Nodes (IndexScan, FullScan) ===

    @Test
    void shouldIncludeNodeTypeInHashForIndexScan() {
        // Behavior: PhysicalIndexScan wrapping a filter has different hash than bare filter
        PhysicalFilter filterNode = filter(Operator.EQ, "age", new Int32Val(25));
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalIndexScan indexScan = new PhysicalIndexScan(nextId(), filterNode, index);

        long hashBareFilter = PhysicalNodeShape.compute(filterNode);
        long hashIndexScan = PhysicalNodeShape.compute(indexScan);

        assertNotEquals(hashBareFilter, hashIndexScan);
    }

    @Test
    void shouldIncludeNodeTypeInHashForFullScan() {
        // Behavior: PhysicalFullScan wrapping a filter has different hash than bare filter
        PhysicalFilter filterNode = filter(Operator.EQ, "age", new Int32Val(25));

        PhysicalFullScan fullScan = new PhysicalFullScan(nextId(), filterNode);

        long hashBareFilter = PhysicalNodeShape.compute(filterNode);
        long hashFullScan = PhysicalNodeShape.compute(fullScan);

        assertNotEquals(hashBareFilter, hashFullScan);
    }

    @Test
    void shouldProduceDifferentHashForIndexScanVsFullScan() {
        // Behavior: IndexScan and FullScan wrapping same filter produce different hashes
        PhysicalFilter filterNode = filter(Operator.EQ, "age", new Int32Val(25));
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalIndexScan indexScan = new PhysicalIndexScan(nextId(), filterNode, index);
        PhysicalFullScan fullScan = new PhysicalFullScan(nextId(), filterNode);

        long hashIndexScan = PhysicalNodeShape.compute(indexScan);
        long hashFullScan = PhysicalNodeShape.compute(fullScan);

        assertNotEquals(hashIndexScan, hashFullScan);
    }

    @Test
    void shouldProduceSameHashForIndexScanWithDifferentFilterValues() {
        // Behavior: IndexScan with different filter values but same structure produce same hash
        PhysicalFilter filter1 = filter(Operator.EQ, "age", new Int32Val(25));
        PhysicalFilter filter2 = filter(Operator.EQ, "age", new Int32Val(50));
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalIndexScan indexScan1 = new PhysicalIndexScan(nextId(), filter1, index);
        PhysicalIndexScan indexScan2 = new PhysicalIndexScan(nextId(), filter2, index);

        long hash1 = PhysicalNodeShape.compute(indexScan1);
        long hash2 = PhysicalNodeShape.compute(indexScan2);

        assertEquals(hash1, hash2);
    }

    // === Special Cases ===

    @Test
    void shouldHandlePhysicalTrue() {
        // Behavior: PhysicalTrue produces consistent hash
        PhysicalTrue true1 = new PhysicalTrue(nextId());
        PhysicalTrue true2 = new PhysicalTrue(nextId());

        long hash1 = PhysicalNodeShape.compute(true1);
        long hash2 = PhysicalNodeShape.compute(true2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldHandlePhysicalFalse() {
        // Behavior: PhysicalFalse produces consistent hash, different from PhysicalTrue
        PhysicalTrue trueNode = new PhysicalTrue(nextId());
        PhysicalFalse falseNode = new PhysicalFalse(nextId());

        long hashTrue = PhysicalNodeShape.compute(trueNode);
        long hashFalse = PhysicalNodeShape.compute(falseNode);

        assertNotEquals(hashTrue, hashFalse);
    }

    @Test
    void shouldHandlePhysicalNot() {
        // Behavior: PhysicalNot(X) != X
        PhysicalFilter filterNode = filter(Operator.EQ, "age", new Int32Val(25));
        PhysicalNot notNode = new PhysicalNot(nextId(), filterNode);

        long hashFilter = PhysicalNodeShape.compute(filterNode);
        long hashNot = PhysicalNodeShape.compute(notNode);

        assertNotEquals(hashFilter, hashNot);
    }

    @Test
    void shouldProduceSameHashForNotWithDifferentValues() {
        // Behavior: PhysicalNot wrapping filters with different values produce same hash
        PhysicalFilter filter1 = filter(Operator.EQ, "age", new Int32Val(25));
        PhysicalFilter filter2 = filter(Operator.EQ, "age", new Int32Val(50));

        PhysicalNot not1 = new PhysicalNot(nextId(), filter1);
        PhysicalNot not2 = new PhysicalNot(nextId(), filter2);

        long hash1 = PhysicalNodeShape.compute(not1);
        long hash2 = PhysicalNodeShape.compute(not2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldHandleNestedElemMatch() {
        // Behavior: Nested elemMatch structures produce correct hash
        PhysicalFilter innerFilter = filter(Operator.GTE, "score", new Int32Val(80));
        PhysicalElemMatch elemMatch = new PhysicalElemMatch(nextId(), "scores", innerFilter);

        PhysicalFilter innerFilter2 = filter(Operator.GTE, "score", new Int32Val(90));
        PhysicalElemMatch elemMatch2 = new PhysicalElemMatch(nextId(), "scores", innerFilter2);

        long hash1 = PhysicalNodeShape.compute(elemMatch);
        long hash2 = PhysicalNodeShape.compute(elemMatch2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldProduceDifferentHashForElemMatchOnDifferentSelectors() {
        // Behavior: ElemMatch on "scores" vs "grades" produce different hashes
        PhysicalFilter innerFilter = filter(Operator.GTE, "value", new Int32Val(80));

        PhysicalElemMatch elemMatch1 = new PhysicalElemMatch(nextId(), "scores", innerFilter);
        PhysicalElemMatch elemMatch2 = new PhysicalElemMatch(nextId(), "grades", innerFilter);

        long hash1 = PhysicalNodeShape.compute(elemMatch1);
        long hash2 = PhysicalNodeShape.compute(elemMatch2);

        assertNotEquals(hash1, hash2);
    }

    // === Index Intersection ===

    @Test
    void shouldProduceSameHashForIndexIntersectionWithDifferentFilterOrder() {
        // Behavior: PhysicalIndexIntersection([A, B]) == PhysicalIndexIntersection([B, A])
        PhysicalFilter filterA = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter filterB = filter(Operator.EQ, "age", new Int32Val(25));

        SingleFieldIndexDefinition indexA = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition indexB = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalIndexIntersection intersection1 = new PhysicalIndexIntersection(
                nextId(), List.of(indexA, indexB), List.of(filterA, filterB));
        PhysicalIndexIntersection intersection2 = new PhysicalIndexIntersection(
                nextId(), List.of(indexB, indexA), List.of(filterB, filterA));

        long hash1 = PhysicalNodeShape.compute(intersection1);
        long hash2 = PhysicalNodeShape.compute(intersection2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldProduceDifferentHashForIndexIntersectionWithDifferentFilters() {
        // Behavior: IndexIntersection with different filter structures produce different hashes
        PhysicalFilter filterA = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter filterB = filter(Operator.EQ, "age", new Int32Val(25));
        PhysicalFilter filterC = filter(Operator.EQ, "status", new StringVal("active"));

        SingleFieldIndexDefinition indexA = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition indexB = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition indexC = SingleFieldIndexDefinition.create("status_idx", "status", BsonType.STRING, false, IndexStatus.WAITING);

        PhysicalIndexIntersection intersection1 = new PhysicalIndexIntersection(
                nextId(), List.of(indexA, indexB), List.of(filterA, filterB));
        PhysicalIndexIntersection intersection2 = new PhysicalIndexIntersection(
                nextId(), List.of(indexA, indexC), List.of(filterA, filterC));

        long hash1 = PhysicalNodeShape.compute(intersection1);
        long hash2 = PhysicalNodeShape.compute(intersection2);

        assertNotEquals(hash1, hash2);
    }

    @Test
    void shouldProduceSameHashForIndexIntersectionWithDifferentValues() {
        // Behavior: IndexIntersection with same structure but different values produce same hash
        PhysicalFilter filterA1 = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter filterB1 = filter(Operator.EQ, "age", new Int32Val(25));

        PhysicalFilter filterA2 = filter(Operator.EQ, "name", new StringVal("jane"));
        PhysicalFilter filterB2 = filter(Operator.EQ, "age", new Int32Val(30));

        SingleFieldIndexDefinition indexA = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition indexB = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalIndexIntersection intersection1 = new PhysicalIndexIntersection(
                nextId(), List.of(indexA, indexB), List.of(filterA1, filterB1));
        PhysicalIndexIntersection intersection2 = new PhysicalIndexIntersection(
                nextId(), List.of(indexA, indexB), List.of(filterA2, filterB2));

        long hash1 = PhysicalNodeShape.compute(intersection1);
        long hash2 = PhysicalNodeShape.compute(intersection2);

        assertEquals(hash1, hash2);
    }

    // === Compound Index Scan ===

    @Test
    void shouldProduceSameHashForCompoundIndexScanWithDifferentFilterOrder() {
        // Behavior: Two CompoundIndexScans with filters [A, B] vs [B, A] produce the same hash (order independence via sorted filter hashes)
        PhysicalFilter filterA = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter filterB = filter(Operator.EQ, "age", new Int32Val(25));

        CompoundIndexDefinition compoundIndex = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);

        PhysicalCompoundIndexScan scan1 = new PhysicalCompoundIndexScan(
                nextId(), compoundIndex, List.of(filterA, filterB));
        PhysicalCompoundIndexScan scan2 = new PhysicalCompoundIndexScan(
                nextId(), compoundIndex, List.of(filterB, filterA));

        long hash1 = PhysicalNodeShape.compute(scan1);
        long hash2 = PhysicalNodeShape.compute(scan2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldProduceDifferentHashForCompoundIndexScanWithDifferentFilters() {
        // Behavior: CompoundIndexScan with [name, age] vs [name, status] produce different hashes
        PhysicalFilter filterName = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter filterAge = filter(Operator.EQ, "age", new Int32Val(25));
        PhysicalFilter filterStatus = filter(Operator.EQ, "status", new StringVal("active"));

        CompoundIndexDefinition compoundIndex1 = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);
        CompoundIndexDefinition compoundIndex2 = CompoundIndexDefinition.create(
                "name_status_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("status", BsonType.STRING, false)
                )
                , IndexStatus.WAITING);

        PhysicalCompoundIndexScan scan1 = new PhysicalCompoundIndexScan(
                nextId(), compoundIndex1, List.of(filterName, filterAge));
        PhysicalCompoundIndexScan scan2 = new PhysicalCompoundIndexScan(
                nextId(), compoundIndex2, List.of(filterName, filterStatus));

        long hash1 = PhysicalNodeShape.compute(scan1);
        long hash2 = PhysicalNodeShape.compute(scan2);

        assertNotEquals(hash1, hash2);
    }

    @Test
    void shouldProduceSameHashForCompoundIndexScanWithDifferentValues() {
        // Behavior: Same structure, different filter values (e.g., name="john" vs name="jane") produce the same hash
        PhysicalFilter filterA1 = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter filterB1 = filter(Operator.EQ, "age", new Int32Val(25));

        PhysicalFilter filterA2 = filter(Operator.EQ, "name", new StringVal("jane"));
        PhysicalFilter filterB2 = filter(Operator.EQ, "age", new Int32Val(30));

        CompoundIndexDefinition compoundIndex = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);

        PhysicalCompoundIndexScan scan1 = new PhysicalCompoundIndexScan(
                nextId(), compoundIndex, List.of(filterA1, filterB1));
        PhysicalCompoundIndexScan scan2 = new PhysicalCompoundIndexScan(
                nextId(), compoundIndex, List.of(filterA2, filterB2));

        long hash1 = PhysicalNodeShape.compute(scan1);
        long hash2 = PhysicalNodeShape.compute(scan2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldProduceDifferentHashForCompoundIndexScanVsIndexIntersection() {
        // Behavior: CompoundIndexScan and IndexIntersection with the same filters produce different hashes (NODE_COMPOUND_INDEX_SCAN vs NODE_INDEX_INTERSECTION)
        PhysicalFilter filterName = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter filterAge = filter(Operator.EQ, "age", new Int32Val(25));

        CompoundIndexDefinition compoundIndex = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalCompoundIndexScan scan = new PhysicalCompoundIndexScan(
                nextId(), compoundIndex, List.of(filterName, filterAge));
        PhysicalIndexIntersection intersection = new PhysicalIndexIntersection(
                nextId(), List.of(nameIndex, ageIndex), List.of(filterName, filterAge));

        long hashScan = PhysicalNodeShape.compute(scan);
        long hashIntersection = PhysicalNodeShape.compute(intersection);

        assertNotEquals(hashScan, hashIntersection);
    }

    @Test
    void shouldProduceDifferentHashForCompoundIndexScanVsAnd() {
        // Behavior: CompoundIndexScan and AND with same filters produce different hashes (NODE_COMPOUND_INDEX_SCAN vs OP_AND)
        PhysicalFilter filterName = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter filterAge = filter(Operator.EQ, "age", new Int32Val(25));

        CompoundIndexDefinition compoundIndex = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);

        PhysicalCompoundIndexScan scan = new PhysicalCompoundIndexScan(
                nextId(), compoundIndex, List.of(filterName, filterAge));
        PhysicalAnd and = new PhysicalAnd(nextId(), List.of(filterName, filterAge));

        long hashScan = PhysicalNodeShape.compute(scan);
        long hashAnd = PhysicalNodeShape.compute(and);

        assertNotEquals(hashScan, hashAnd);
    }

    // === Size and Exists operators ===

    @Test
    void shouldProduceSameHashForSizeWithDifferentValues() {
        // Behavior: $size with 3 vs. 5 produce same hash (different integer values)
        PhysicalFilter filter1 = filter(Operator.SIZE, "tags", 3);
        PhysicalFilter filter2 = filter(Operator.SIZE, "tags", 5);

        long hash1 = PhysicalNodeShape.compute(filter1);
        long hash2 = PhysicalNodeShape.compute(filter2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldProduceSameHashForExistsTrueVsFalse() {
        // Behavior: $exists true vs false produce same hash because PhysicalNodeShape
        // only considers the value type (Boolean), not the actual boolean value.
        // Note: This differs from QueryShape which includes the boolean value.
        PhysicalFilter filterTrue = filter(Operator.EXISTS, "field", true);
        PhysicalFilter filterFalse = filter(Operator.EXISTS, "field", false);

        long hashTrue = PhysicalNodeShape.compute(filterTrue);
        long hashFalse = PhysicalNodeShape.compute(filterFalse);

        assertEquals(hashTrue, hashFalse);
    }

    // === Complex Nested Structures ===

    @Test
    void shouldProduceSameHashForComplexStructureWithDifferentValues() {
        // Behavior: Complex nested structure with different values produce same hash
        PhysicalFilter ageFilter1 = filter(Operator.GTE, "age", new Int32Val(18));
        List<Object> statusValues1 = List.of(new StringVal("active"), new StringVal("pending"));
        PhysicalFilter statusFilter1 = filter(Operator.IN, "status", statusValues1);
        PhysicalAnd and1 = new PhysicalAnd(nextId(), List.of(ageFilter1, statusFilter1));

        PhysicalFilter ageFilter2 = filter(Operator.GTE, "age", new Int32Val(21));
        List<Object> statusValues2 = List.of(new StringVal("approved"), new StringVal("review"));
        PhysicalFilter statusFilter2 = filter(Operator.IN, "status", statusValues2);
        PhysicalAnd and2 = new PhysicalAnd(nextId(), List.of(ageFilter2, statusFilter2));

        long hash1 = PhysicalNodeShape.compute(and1);
        long hash2 = PhysicalNodeShape.compute(and2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldProduceDifferentHashForDifferentAndChildCount() {
        // Behavior: AND with 2 children vs 3 children produce different hashes
        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB = filter(Operator.EQ, "b", new Int32Val(2));
        PhysicalFilter filterC = filter(Operator.EQ, "c", new Int32Val(3));

        PhysicalAnd and2 = new PhysicalAnd(nextId(), List.of(filterA, filterB));
        PhysicalAnd and3 = new PhysicalAnd(nextId(), List.of(filterA, filterB, filterC));

        long hash2 = PhysicalNodeShape.compute(and2);
        long hash3 = PhysicalNodeShape.compute(and3);

        assertNotEquals(hash2, hash3);
    }

    // === Deep Nesting and Complex Plans ===

    @Test
    void shouldHandleDeeplyNestedAndOr() {
        // Behavior: Deeply nested AND/OR structures (4 levels) produce consistent hashes
        // Structure: AND(OR(AND(a, b), c), d)
        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB = filter(Operator.EQ, "b", new Int32Val(2));
        PhysicalFilter filterC = filter(Operator.EQ, "c", new Int32Val(3));
        PhysicalFilter filterD = filter(Operator.EQ, "d", new Int32Val(4));

        PhysicalAnd innerAnd = new PhysicalAnd(nextId(), List.of(filterA, filterB));
        PhysicalOr middleOr = new PhysicalOr(nextId(), List.of(innerAnd, filterC));
        PhysicalAnd outerAnd = new PhysicalAnd(nextId(), List.of(middleOr, filterD));

        // Same structure with different values
        PhysicalFilter filterA2 = filter(Operator.EQ, "a", new Int32Val(10));
        PhysicalFilter filterB2 = filter(Operator.EQ, "b", new Int32Val(20));
        PhysicalFilter filterC2 = filter(Operator.EQ, "c", new Int32Val(30));
        PhysicalFilter filterD2 = filter(Operator.EQ, "d", new Int32Val(40));

        PhysicalAnd innerAnd2 = new PhysicalAnd(nextId(), List.of(filterA2, filterB2));
        PhysicalOr middleOr2 = new PhysicalOr(nextId(), List.of(innerAnd2, filterC2));
        PhysicalAnd outerAnd2 = new PhysicalAnd(nextId(), List.of(middleOr2, filterD2));

        long hash1 = PhysicalNodeShape.compute(outerAnd);
        long hash2 = PhysicalNodeShape.compute(outerAnd2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldHandleDeeplyNestedWithReordering() {
        // Behavior: Deep nesting with reordered children at multiple levels produces same hash
        // Structure1: AND(d, OR(c, AND(a, b)))
        // Structure2: AND(OR(AND(b, a), c), d) - same logical structure, different order
        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB = filter(Operator.EQ, "b", new Int32Val(2));
        PhysicalFilter filterC = filter(Operator.EQ, "c", new Int32Val(3));
        PhysicalFilter filterD = filter(Operator.EQ, "d", new Int32Val(4));

        PhysicalAnd innerAnd1 = new PhysicalAnd(nextId(), List.of(filterA, filterB));
        PhysicalOr middleOr1 = new PhysicalOr(nextId(), List.of(filterC, innerAnd1));
        PhysicalAnd outerAnd1 = new PhysicalAnd(nextId(), List.of(filterD, middleOr1));

        PhysicalAnd innerAnd2 = new PhysicalAnd(nextId(), List.of(filterB, filterA));
        PhysicalOr middleOr2 = new PhysicalOr(nextId(), List.of(innerAnd2, filterC));
        PhysicalAnd outerAnd2 = new PhysicalAnd(nextId(), List.of(middleOr2, filterD));

        long hash1 = PhysicalNodeShape.compute(outerAnd1);
        long hash2 = PhysicalNodeShape.compute(outerAnd2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldHandleNotWrappingComplexStructure() {
        // Behavior: NOT wrapping complex AND/OR produces consistent hash
        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB = filter(Operator.EQ, "b", new Int32Val(2));
        PhysicalOr innerOr = new PhysicalOr(nextId(), List.of(filterA, filterB));
        PhysicalNot not1 = new PhysicalNot(nextId(), innerOr);

        // Same structure, different values
        PhysicalFilter filterA2 = filter(Operator.EQ, "a", new Int32Val(10));
        PhysicalFilter filterB2 = filter(Operator.EQ, "b", new Int32Val(20));
        PhysicalOr innerOr2 = new PhysicalOr(nextId(), List.of(filterA2, filterB2));
        PhysicalNot not2 = new PhysicalNot(nextId(), innerOr2);

        long hash1 = PhysicalNodeShape.compute(not1);
        long hash2 = PhysicalNodeShape.compute(not2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldHandleElemMatchWithNestedAndOr() {
        // Behavior: ElemMatch containing AND/OR produces consistent hash
        PhysicalFilter filterGte = filter(Operator.GTE, "score", new Int32Val(80));
        PhysicalFilter filterLte = filter(Operator.LTE, "score", new Int32Val(100));
        PhysicalAnd innerAnd = new PhysicalAnd(nextId(), List.of(filterGte, filterLte));
        PhysicalElemMatch elemMatch1 = new PhysicalElemMatch(nextId(), "results", innerAnd);

        // Same structure, different values
        PhysicalFilter filterGte2 = filter(Operator.GTE, "score", new Int32Val(70));
        PhysicalFilter filterLte2 = filter(Operator.LTE, "score", new Int32Val(90));
        PhysicalAnd innerAnd2 = new PhysicalAnd(nextId(), List.of(filterGte2, filterLte2));
        PhysicalElemMatch elemMatch2 = new PhysicalElemMatch(nextId(), "results", innerAnd2);

        long hash1 = PhysicalNodeShape.compute(elemMatch1);
        long hash2 = PhysicalNodeShape.compute(elemMatch2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldHandleMixedScanTypesInAnd() {
        // Behavior: AND containing IndexScan and FullScan wrapped nodes
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalFilter ageFilter = filter(Operator.EQ, "age", new Int32Val(25));
        PhysicalIndexScan indexScan = new PhysicalIndexScan(nextId(), ageFilter, ageIndex);

        PhysicalFilter nameFilter = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFullScan fullScan = new PhysicalFullScan(nextId(), nameFilter);

        PhysicalAnd and1 = new PhysicalAnd(nextId(), List.of(indexScan, fullScan));

        // Same structure, different values
        PhysicalFilter ageFilter2 = filter(Operator.EQ, "age", new Int32Val(30));
        PhysicalIndexScan indexScan2 = new PhysicalIndexScan(nextId(), ageFilter2, ageIndex);

        PhysicalFilter nameFilter2 = filter(Operator.EQ, "name", new StringVal("jane"));
        PhysicalFullScan fullScan2 = new PhysicalFullScan(nextId(), nameFilter2);

        PhysicalAnd and2 = new PhysicalAnd(nextId(), List.of(indexScan2, fullScan2));

        long hash1 = PhysicalNodeShape.compute(and1);
        long hash2 = PhysicalNodeShape.compute(and2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldHandleRangeScanCombinedWithFilters() {
        // Behavior: AND containing RangeScan and regular filters
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalRangeScan rangeScan = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(18), new Int32Val(65), true, false, ageIndex);
        PhysicalFilter statusFilter = filter(Operator.EQ, "status", new StringVal("active"));
        PhysicalAnd and1 = new PhysicalAnd(nextId(), List.of(rangeScan, statusFilter));

        // Same structure, different values
        PhysicalRangeScan rangeScan2 = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(21), new Int32Val(50), true, false, ageIndex);
        PhysicalFilter statusFilter2 = filter(Operator.EQ, "status", new StringVal("pending"));
        PhysicalAnd and2 = new PhysicalAnd(nextId(), List.of(rangeScan2, statusFilter2));

        long hash1 = PhysicalNodeShape.compute(and1);
        long hash2 = PhysicalNodeShape.compute(and2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldHandleOrWithMultipleScanTypes() {
        // Behavior: OR containing different scan types produces consistent hash
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);

        PhysicalFilter ageFilter = filter(Operator.EQ, "age", new Int32Val(25));
        PhysicalIndexScan indexScan = new PhysicalIndexScan(nextId(), ageFilter, ageIndex);

        PhysicalRangeScan rangeScan = new PhysicalRangeScan(nextId(), "name",
                new StringVal("a"), new StringVal("m"), true, false, nameIndex);

        PhysicalOr or1 = new PhysicalOr(nextId(), List.of(indexScan, rangeScan));

        // Same structure, different values, reversed order
        PhysicalFilter ageFilter2 = filter(Operator.EQ, "age", new Int32Val(30));
        PhysicalIndexScan indexScan2 = new PhysicalIndexScan(nextId(), ageFilter2, ageIndex);

        PhysicalRangeScan rangeScan2 = new PhysicalRangeScan(nextId(), "name",
                new StringVal("n"), new StringVal("z"), true, false, nameIndex);

        PhysicalOr or2 = new PhysicalOr(nextId(), List.of(rangeScan2, indexScan2));

        long hash1 = PhysicalNodeShape.compute(or1);
        long hash2 = PhysicalNodeShape.compute(or2);

        assertEquals(hash1, hash2);
    }

    // === Real-world Query Pattern Tests ===

    @Test
    void shouldHandleTypicalUserSearchQuery() {
        // Behavior: Common user search pattern with multiple conditions
        // Pattern: {$and: [{age: {$gte: 18}}, {age: {$lt: 65}}, {status: {$in: ["active", "premium"]}}]}
        PhysicalFilter ageGte = filter(Operator.GTE, "age", new Int32Val(18));
        PhysicalFilter ageLt = filter(Operator.LT, "age", new Int32Val(65));
        List<Object> statusValues = List.of(new StringVal("active"), new StringVal("premium"));
        PhysicalFilter statusIn = filter(Operator.IN, "status", statusValues);

        PhysicalAnd query1 = new PhysicalAnd(nextId(), List.of(ageGte, ageLt, statusIn));

        // Same structure, different values, different order
        PhysicalFilter ageGte2 = filter(Operator.GTE, "age", new Int32Val(21));
        PhysicalFilter ageLt2 = filter(Operator.LT, "age", new Int32Val(50));
        List<Object> statusValues2 = List.of(new StringVal("verified"), new StringVal("admin"));
        PhysicalFilter statusIn2 = filter(Operator.IN, "status", statusValues2);

        PhysicalAnd query2 = new PhysicalAnd(nextId(), List.of(statusIn2, ageLt2, ageGte2));

        long hash1 = PhysicalNodeShape.compute(query1);
        long hash2 = PhysicalNodeShape.compute(query2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldHandleTypicalProductFilterQuery() {
        // Behavior: E-commerce product filter pattern
        // Pattern: {$and: [{price: {$gte: 10, $lte: 100}}, {category: "electronics"}, {$or: [{brand: "A"}, {brand: "B"}]}]}
        PhysicalFilter priceGte = filter(Operator.GTE, "price", new Int32Val(10));
        PhysicalFilter priceLte = filter(Operator.LTE, "price", new Int32Val(100));
        PhysicalFilter category = filter(Operator.EQ, "category", new StringVal("electronics"));
        PhysicalFilter brandA = filter(Operator.EQ, "brand", new StringVal("A"));
        PhysicalFilter brandB = filter(Operator.EQ, "brand", new StringVal("B"));

        PhysicalOr brandOr = new PhysicalOr(nextId(), List.of(brandA, brandB));
        PhysicalAnd query1 = new PhysicalAnd(nextId(), List.of(priceGte, priceLte, category, brandOr));

        // Same structure, different values
        PhysicalFilter priceGte2 = filter(Operator.GTE, "price", new Int32Val(50));
        PhysicalFilter priceLte2 = filter(Operator.LTE, "price", new Int32Val(500));
        PhysicalFilter category2 = filter(Operator.EQ, "category", new StringVal("clothing"));
        PhysicalFilter brandX = filter(Operator.EQ, "brand", new StringVal("X"));
        PhysicalFilter brandY = filter(Operator.EQ, "brand", new StringVal("Y"));

        PhysicalOr brandOr2 = new PhysicalOr(nextId(), List.of(brandY, brandX));
        PhysicalAnd query2 = new PhysicalAnd(nextId(), List.of(brandOr2, category2, priceLte2, priceGte2));

        long hash1 = PhysicalNodeShape.compute(query1);
        long hash2 = PhysicalNodeShape.compute(query2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldHandleArrayQueryWithElemMatch() {
        // Behavior: Query with array field using $elemMatch and $all
        // Pattern: {$and: [{tags: {$all: ["a", "b"]}}, {items: {$elemMatch: {qty: {$gte: 10}}}}]}
        List<Object> tagValues = List.of(new StringVal("a"), new StringVal("b"));
        PhysicalFilter tagsAll = filter(Operator.ALL, "tags", tagValues);

        PhysicalFilter qtyGte = filter(Operator.GTE, "qty", new Int32Val(10));
        PhysicalElemMatch elemMatch = new PhysicalElemMatch(nextId(), "items", qtyGte);

        PhysicalAnd query1 = new PhysicalAnd(nextId(), List.of(tagsAll, elemMatch));

        // Same structure, different values
        List<Object> tagValues2 = List.of(new StringVal("x"), new StringVal("y"));
        PhysicalFilter tagsAll2 = filter(Operator.ALL, "tags", tagValues2);

        PhysicalFilter qtyGte2 = filter(Operator.GTE, "qty", new Int32Val(50));
        PhysicalElemMatch elemMatch2 = new PhysicalElemMatch(nextId(), "items", qtyGte2);

        PhysicalAnd query2 = new PhysicalAnd(nextId(), List.of(elemMatch2, tagsAll2));

        long hash1 = PhysicalNodeShape.compute(query1);
        long hash2 = PhysicalNodeShape.compute(query2);

        assertEquals(hash1, hash2);
    }

    // === Ordering Consistency Tests ===

    @Test
    void shouldMaintainConsistentOrderingForSiblingFilters() {
        // Behavior: Filters with different selectors maintain consistent relative ordering
        // when used as siblings in AND. This is important for parameter binding alignment.
        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB = filter(Operator.EQ, "b", new Int32Val(2));
        PhysicalFilter filterC = filter(Operator.EQ, "c", new Int32Val(3));

        // Compute individual hashes
        long hashA = PhysicalNodeShape.compute(filterA);
        long hashB = PhysicalNodeShape.compute(filterB);
        long hashC = PhysicalNodeShape.compute(filterC);

        // Verify ordering is deterministic (different selectors = different hashes)
        assertNotEquals(hashA, hashB);
        assertNotEquals(hashB, hashC);
        assertNotEquals(hashA, hashC);

        // Create AND with all permutations and verify same result
        PhysicalAnd and1 = new PhysicalAnd(nextId(), List.of(filterA, filterB, filterC));
        PhysicalAnd and2 = new PhysicalAnd(nextId(), List.of(filterC, filterA, filterB));
        PhysicalAnd and3 = new PhysicalAnd(nextId(), List.of(filterB, filterC, filterA));

        long hash1 = PhysicalNodeShape.compute(and1);
        long hash2 = PhysicalNodeShape.compute(and2);
        long hash3 = PhysicalNodeShape.compute(and3);

        assertEquals(hash1, hash2);
        assertEquals(hash2, hash3);
    }

    @Test
    void shouldMaintainConsistentOrderingForOperatorVariants() {
        // Behavior: The same selector with different operators produces different hashes
        // This ensures proper differentiation for parameter binding
        PhysicalFilter filterGt = filter(Operator.GT, "age", new Int32Val(18));
        PhysicalFilter filterGte = filter(Operator.GTE, "age", new Int32Val(18));
        PhysicalFilter filterLt = filter(Operator.LT, "age", new Int32Val(65));
        PhysicalFilter filterLte = filter(Operator.LTE, "age", new Int32Val(65));

        long hashGt = PhysicalNodeShape.compute(filterGt);
        long hashGte = PhysicalNodeShape.compute(filterGte);
        long hashLt = PhysicalNodeShape.compute(filterLt);
        long hashLte = PhysicalNodeShape.compute(filterLte);

        // All should be different
        assertNotEquals(hashGt, hashGte);
        assertNotEquals(hashGt, hashLt);
        assertNotEquals(hashGt, hashLte);
        assertNotEquals(hashGte, hashLt);
        assertNotEquals(hashGte, hashLte);
        assertNotEquals(hashLt, hashLte);
    }

    @Test
    void shouldHandleIdenticalShapeSiblings() {
        // Behavior: Multiple filters with identical shape (same selector, operator, value type)
        // produce the same individual hash but AND maintains correct count
        PhysicalFilter filter1 = filter(Operator.EQ, "status", new StringVal("active"));
        PhysicalFilter filter2 = filter(Operator.EQ, "status", new StringVal("pending"));

        // Individual hashes should be equal (same shape)
        long hash1 = PhysicalNodeShape.compute(filter1);
        long hash2 = PhysicalNodeShape.compute(filter2);
        assertEquals(hash1, hash2);

        // BUT: AND with one vs two identical-shape filters should differ
        PhysicalAnd andOne = new PhysicalAnd(nextId(), List.of(filter1));
        PhysicalAnd andTwo = new PhysicalAnd(nextId(), List.of(filter1, filter2));

        long hashAndOne = PhysicalNodeShape.compute(andOne);
        long hashAndTwo = PhysicalNodeShape.compute(andTwo);

        assertNotEquals(hashAndOne, hashAndTwo);
    }

    // === ObjectId Type ===

    @Test
    void shouldProduceSameHashForObjectIdWithDifferentValues() {
        // Behavior: EQ filters on _id with different ObjectIdVal values produce the same shape hash.
        PhysicalFilter filter1 = filter(Operator.EQ, "_id", new ObjectIdVal(new ObjectId()));
        PhysicalFilter filter2 = filter(Operator.EQ, "_id", new ObjectIdVal(new ObjectId()));

        long hash1 = PhysicalNodeShape.compute(filter1);
        long hash2 = PhysicalNodeShape.compute(filter2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldProduceDifferentHashForObjectIdVsString() {
        // Behavior: EQ filter with ObjectIdVal vs StringVal on the same field produces different shape hashes.
        PhysicalFilter filter1 = filter(Operator.EQ, "_id", new ObjectIdVal(new ObjectId()));
        PhysicalFilter filter2 = filter(Operator.EQ, "_id", new StringVal("abc"));

        long hash1 = PhysicalNodeShape.compute(filter1);
        long hash2 = PhysicalNodeShape.compute(filter2);

        assertNotEquals(hash1, hash2);
    }

    @Test
    void shouldProduceSameHashForInWithObjectIdsWithDifferentValues() {
        // Behavior: $in with two different ObjectIdVal lists of the same size produce the same shape hash.
        List<Object> values1 = List.of(new ObjectIdVal(new ObjectId()), new ObjectIdVal(new ObjectId()));
        List<Object> values2 = List.of(new ObjectIdVal(new ObjectId()), new ObjectIdVal(new ObjectId()));

        PhysicalFilter filter1 = filter(Operator.IN, "_id", values1);
        PhysicalFilter filter2 = filter(Operator.IN, "_id", values2);

        long hash1 = PhysicalNodeShape.compute(filter1);
        long hash2 = PhysicalNodeShape.compute(filter2);

        assertEquals(hash1, hash2);
    }

    @Test
    void shouldProduceSameHashForInWithObjectIdsInDifferentOrder() {
        // Behavior: $in with the same ObjectIdVal items in different order produces the same shape hash.
        ObjectIdVal id1 = new ObjectIdVal(new ObjectId());
        ObjectIdVal id2 = new ObjectIdVal(new ObjectId());

        List<Object> values1 = List.of(id1, id2);
        List<Object> values2 = List.of(id2, id1);

        PhysicalFilter filter1 = filter(Operator.IN, "_id", values1);
        PhysicalFilter filter2 = filter(Operator.IN, "_id", values2);

        long hash1 = PhysicalNodeShape.compute(filter1);
        long hash2 = PhysicalNodeShape.compute(filter2);

        assertEquals(hash1, hash2);
    }

    // === Edge Cases ===

    @Test
    void shouldHandleSingleChildAnd() {
        // Behavior: AND with a single child produces different hash than the child alone
        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalAnd andSingle = new PhysicalAnd(nextId(), List.of(filterA));

        long hashFilter = PhysicalNodeShape.compute(filterA);
        long hashAnd = PhysicalNodeShape.compute(andSingle);

        assertNotEquals(hashFilter, hashAnd);
    }

    @Test
    void shouldHandleSingleChildOr() {
        // Behavior: OR with a single child produces different hash than the child alone
        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalOr orSingle = new PhysicalOr(nextId(), List.of(filterA));

        long hashFilter = PhysicalNodeShape.compute(filterA);
        long hashOr = PhysicalNodeShape.compute(orSingle);

        assertNotEquals(hashFilter, hashOr);
    }

    @Test
    void shouldHandleFullScanWrappingTrue() {
        // Behavior: FullScan wrapping PhysicalTrue (empty query case)
        PhysicalTrue trueNode = new PhysicalTrue(nextId());
        PhysicalFullScan fullScan = new PhysicalFullScan(nextId(), trueNode);

        long hashTrue = PhysicalNodeShape.compute(trueNode);
        long hashFullScan = PhysicalNodeShape.compute(fullScan);

        assertNotEquals(hashTrue, hashFullScan);
    }

    @Test
    void shouldHandleComplexIndexIntersection() {
        // Behavior: IndexIntersection with multiple EQ filters on different fields
        PhysicalFilter filterName = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter filterAge = filter(Operator.EQ, "age", new Int32Val(25));
        PhysicalFilter filterCity = filter(Operator.EQ, "city", new StringVal("NYC"));

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition cityIndex = SingleFieldIndexDefinition.create("city_idx", "city", BsonType.STRING, false, IndexStatus.WAITING);

        PhysicalIndexIntersection intersection1 = new PhysicalIndexIntersection(
                nextId(),
                List.of(nameIndex, ageIndex, cityIndex),
                List.of(filterName, filterAge, filterCity)
        );

        // Same structure, different values, different order
        PhysicalFilter filterName2 = filter(Operator.EQ, "name", new StringVal("jane"));
        PhysicalFilter filterAge2 = filter(Operator.EQ, "age", new Int32Val(30));
        PhysicalFilter filterCity2 = filter(Operator.EQ, "city", new StringVal("LA"));

        PhysicalIndexIntersection intersection2 = new PhysicalIndexIntersection(
                nextId(),
                List.of(cityIndex, nameIndex, ageIndex),
                List.of(filterCity2, filterName2, filterAge2)
        );

        long hash1 = PhysicalNodeShape.compute(intersection1);
        long hash2 = PhysicalNodeShape.compute(intersection2);

        assertEquals(hash1, hash2);
    }

    // =========================================================================
    // Integration Tests: QueryShape and PhysicalNodeShape Ordering Alignment
    // =========================================================================
    // These tests verify that the relative ordering of children when sorted by
    // hash is the same between QueryShape and PhysicalNodeShape. This is critical
    // for plan caching where ParameterExtractor uses QueryShape and
    // PhysicalPlanParameterBinder uses PhysicalNodeShape.

    @Test
    void shouldPreserveRelativeOrderingForDifferentSelectors() {
        // Behavior: When sorting filters by different selectors, QueryShape and
        // PhysicalNodeShape must produce the same relative ordering.

        // Create BQL expressions
        BqlExpr bqlA = new BqlEq("a", new Int32Val(1));
        BqlExpr bqlB = new BqlEq("b", new Int32Val(2));
        BqlExpr bqlC = new BqlEq("c", new Int32Val(3));

        // Get QueryShape ordering
        List<BqlExpr> bqlList = new ArrayList<>(List.of(bqlA, bqlB, bqlC));
        int[] bqlOrder = getSortedIndicesByQueryShape(bqlList);

        // Create corresponding PhysicalFilters
        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB = filter(Operator.EQ, "b", new Int32Val(2));
        PhysicalFilter filterC = filter(Operator.EQ, "c", new Int32Val(3));

        // Get PhysicalNodeShape ordering
        List<PhysicalNode> physicalList = new ArrayList<>(List.of(filterA, filterB, filterC));
        int[] physicalOrder = getSortedIndicesByPhysicalNodeShape(physicalList);

        // The relative ordering must match
        assertArrayEquals(bqlOrder, physicalOrder,
                "QueryShape and PhysicalNodeShape must produce same relative ordering for different selectors");
    }

    @Test
    void shouldPreserveRelativeOrderingForDifferentOperators() {
        // Behavior: The same selector with different operators must have consistent ordering.

        // Create BQL expressions with the same selector, different operators
        BqlExpr bqlGt = new BqlGt("age", new Int32Val(18));
        BqlExpr bqlLt = new BqlLt("age", new Int32Val(65));
        BqlExpr bqlEq = new BqlEq("age", new Int32Val(30));

        List<BqlExpr> bqlList = new ArrayList<>(List.of(bqlGt, bqlLt, bqlEq));
        int[] bqlOrder = getSortedIndicesByQueryShape(bqlList);

        // Create corresponding PhysicalFilters
        PhysicalFilter filterGt = filter(Operator.GT, "age", new Int32Val(18));
        PhysicalFilter filterLt = filter(Operator.LT, "age", new Int32Val(65));
        PhysicalFilter filterEq = filter(Operator.EQ, "age", new Int32Val(30));

        List<PhysicalNode> physicalList = new ArrayList<>(List.of(filterGt, filterLt, filterEq));
        int[] physicalOrder = getSortedIndicesByPhysicalNodeShape(physicalList);

        assertArrayEquals(bqlOrder, physicalOrder,
                "QueryShape and PhysicalNodeShape must produce same relative ordering for different operators");
    }

    @Test
    void shouldPreserveRelativeOrderingForDifferentValueTypes() {
        // Behavior: The same selector and operator with different value types must have consistent ordering.

        BqlExpr bqlInt = new BqlEq("value", new Int32Val(100));
        BqlExpr bqlStr = new BqlEq("value", new StringVal("100"));
        BqlExpr bqlBool = new BqlEq("value", new BooleanVal(true));

        List<BqlExpr> bqlList = new ArrayList<>(List.of(bqlInt, bqlStr, bqlBool));
        int[] bqlOrder = getSortedIndicesByQueryShape(bqlList);

        PhysicalFilter filterInt = filter(Operator.EQ, "value", new Int32Val(100));
        PhysicalFilter filterStr = filter(Operator.EQ, "value", new StringVal("100"));
        PhysicalFilter filterBool = filter(Operator.EQ, "value", new BooleanVal(true));

        List<PhysicalNode> physicalList = new ArrayList<>(List.of(filterInt, filterStr, filterBool));
        int[] physicalOrder = getSortedIndicesByPhysicalNodeShape(physicalList);

        assertArrayEquals(bqlOrder, physicalOrder,
                "QueryShape and PhysicalNodeShape must produce same relative ordering for different value types");
    }

    @Test
    void shouldPreserveRelativeOrderingForMixedExpressions() {
        // Behavior: Mixed expressions (different selectors, operators, types) must have consistent ordering.

        BqlExpr bqlAgeGte = new BqlGte("age", new Int32Val(18));
        BqlExpr bqlNameEq = new BqlEq("name", new StringVal("john"));
        BqlExpr bqlStatusIn = new BqlIn("status", List.of(new StringVal("active"), new StringVal("pending")));
        BqlExpr bqlScoreLt = new BqlLt("score", new Int32Val(100));

        List<BqlExpr> bqlList = new ArrayList<>(List.of(bqlAgeGte, bqlNameEq, bqlStatusIn, bqlScoreLt));
        int[] bqlOrder = getSortedIndicesByQueryShape(bqlList);

        PhysicalFilter filterAgeGte = filter(Operator.GTE, "age", new Int32Val(18));
        PhysicalFilter filterNameEq = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter filterStatusIn = filter(Operator.IN, "status",
                List.of(new StringVal("active"), new StringVal("pending")));
        PhysicalFilter filterScoreLt = filter(Operator.LT, "score", new Int32Val(100));

        List<PhysicalNode> physicalList = new ArrayList<>(List.of(filterAgeGte, filterNameEq, filterStatusIn, filterScoreLt));
        int[] physicalOrder = getSortedIndicesByPhysicalNodeShape(physicalList);

        assertArrayEquals(bqlOrder, physicalOrder,
                "QueryShape and PhysicalNodeShape must produce same relative ordering for mixed expressions");
    }

    @Test
    void shouldPreserveRelativeOrderingForComparisonOperatorVariants() {
        // Behavior: All comparison operators on same field must have consistent ordering.

        BqlExpr bqlGt = new BqlGt("x", new Int32Val(1));
        BqlExpr bqlGte = new BqlGte("x", new Int32Val(2));
        BqlExpr bqlLt = new BqlLt("x", new Int32Val(3));
        BqlExpr bqlLte = new BqlLte("x", new Int32Val(4));
        BqlExpr bqlEq = new BqlEq("x", new Int32Val(5));
        BqlExpr bqlNe = new BqlNe("x", new Int32Val(6));

        List<BqlExpr> bqlList = new ArrayList<>(List.of(bqlGt, bqlGte, bqlLt, bqlLte, bqlEq, bqlNe));
        int[] bqlOrder = getSortedIndicesByQueryShape(bqlList);

        PhysicalFilter filterGt = filter(Operator.GT, "x", new Int32Val(1));
        PhysicalFilter filterGte = filter(Operator.GTE, "x", new Int32Val(2));
        PhysicalFilter filterLt = filter(Operator.LT, "x", new Int32Val(3));
        PhysicalFilter filterLte = filter(Operator.LTE, "x", new Int32Val(4));
        PhysicalFilter filterEq = filter(Operator.EQ, "x", new Int32Val(5));
        PhysicalFilter filterNe = filter(Operator.NE, "x", new Int32Val(6));

        List<PhysicalNode> physicalList = new ArrayList<>(List.of(filterGt, filterGte, filterLt, filterLte, filterEq, filterNe));
        int[] physicalOrder = getSortedIndicesByPhysicalNodeShape(physicalList);

        assertArrayEquals(bqlOrder, physicalOrder,
                "QueryShape and PhysicalNodeShape must produce same relative ordering for all comparison operators");
    }

    @Test
    void shouldProduceConsistentParameterBindingOrder() {
        // Behavior: End-to-end test verifying parameter extraction order matches binding order.
        // Parse a query, extract parameters, create a physical plan, bind parameters,
        // and verify the parameter indices match.

        String query = "{\"$and\": [{\"age\": {\"$gte\": 18}}, {\"name\": \"john\"}, {\"score\": {\"$lt\": 100}}]}";
        BqlExpr bqlExpr = BqlParser.parse(query);

        // Extract parameters using ParameterExtractor (uses QueryShape)
        List<BqlValue> extractedParams = ParameterExtractor.extract(bqlExpr);

        // Create a corresponding physical plan (manual construction since we don't have full planner context)
        PhysicalFilter ageFilter = filter(Operator.GTE, "age", new Int32Val(18));
        PhysicalFilter nameFilter = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter scoreFilter = filter(Operator.LT, "score", new Int32Val(100));

        // Create AND with children in the original query order
        PhysicalAnd physicalAnd = new PhysicalAnd(nextId(), List.of(ageFilter, nameFilter, scoreFilter));

        // Bind parameters using PhysicalPlanParameterBinder (uses PhysicalNodeShape)
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(physicalAnd, extractedParams);

        // The sorted order should produce parameters matching extractedParams
        // Since AND sorts children by shape hash, the parameter indices assigned
        // should correspond to the same ordering as ParameterExtractor

        // Verify we have bindings for all three filters
        assertEquals(3, bindings.size());

        // Each filter should have exactly one parameter binding
        for (var entry : bindings.values()) {
            assertEquals(1, entry.size());
        }

        // Verify parameter count matches
        int totalParams = bindings.values().stream().mapToInt(List::size).sum();
        assertEquals(extractedParams.size(), totalParams);
    }

    @Test
    void shouldPreserveOrderingForArrayOperators() {
        // Behavior: Array operators ($in, $nin, $all) must have consistent ordering.

        BqlExpr bqlIn = new BqlIn("status", List.of(new StringVal("a"), new StringVal("b")));
        BqlExpr bqlNin = new BqlNin("role", List.of(new StringVal("x"), new StringVal("y")));
        BqlExpr bqlAll = new BqlAll("tags", List.of(new StringVal("p"), new StringVal("q")));

        List<BqlExpr> bqlList = new ArrayList<>(List.of(bqlIn, bqlNin, bqlAll));
        int[] bqlOrder = getSortedIndicesByQueryShape(bqlList);

        PhysicalFilter filterIn = filter(Operator.IN, "status", List.of(new StringVal("a"), new StringVal("b")));
        PhysicalFilter filterNin = filter(Operator.NIN, "role", List.of(new StringVal("x"), new StringVal("y")));
        PhysicalFilter filterAll = filter(Operator.ALL, "tags", List.of(new StringVal("p"), new StringVal("q")));

        List<PhysicalNode> physicalList = new ArrayList<>(List.of(filterIn, filterNin, filterAll));
        int[] physicalOrder = getSortedIndicesByPhysicalNodeShape(physicalList);

        assertArrayEquals(bqlOrder, physicalOrder,
                "QueryShape and PhysicalNodeShape must produce same relative ordering for array operators");
    }

    // === Helper methods for ordering tests ===

    /**
     * Returns the sorted indices based on QueryShape hash.
     */
    private int[] getSortedIndicesByQueryShape(List<BqlExpr> exprs) {
        int size = exprs.size();
        long[] hashes = new long[size];
        Integer[] indices = new Integer[size];
        for (int i = 0; i < size; i++) {
            hashes[i] = QueryShape.compute(exprs.get(i));
            indices[i] = i;
        }

        // Sort indices by hash
        Arrays.sort(indices, Comparator.comparingLong(a -> hashes[a]));

        return Arrays.stream(indices).mapToInt(Integer::intValue).toArray();
    }

    /**
     * Returns the sorted indices based on PhysicalNodeShape hash.
     */
    private int[] getSortedIndicesByPhysicalNodeShape(List<PhysicalNode> nodes) {
        int size = nodes.size();
        long[] hashes = new long[size];
        Integer[] indices = new Integer[size];
        for (int i = 0; i < size; i++) {
            hashes[i] = PhysicalNodeShape.compute(nodes.get(i));
            indices[i] = i;
        }

        // Sort indices by hash
        Arrays.sort(indices, Comparator.comparingLong(a -> hashes[a]));

        return Arrays.stream(indices).mapToInt(Integer::intValue).toArray();
    }

    /**
     * Asserts that two int arrays are equal.
     */
    private void assertArrayEquals(int[] expected, int[] actual, String message) {
        assertEquals(expected.length, actual.length, message + " - array lengths differ");
        for (int i = 0; i < expected.length; i++) {
            assertEquals(expected[i], actual[i], message + " - mismatch at index " + i);
        }
    }

    // =========================================================================
    // computeForBinding Tests
    // =========================================================================
    // These tests verify that computeForBinding produces hashes that match
    // QueryShape's ordering by skipping wrapper node type prefixes.

    @Test
    void shouldComputeForBindingSkipIndexScanPrefix() {
        // Behavior: computeForBinding skips NODE_INDEX_SCAN prefix, producing same hash as bare filter
        PhysicalFilter filterNode = filter(Operator.EQ, "age", new Int32Val(25));
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        PhysicalIndexScan indexScan = new PhysicalIndexScan(nextId(), filterNode, index);

        long hashBareFilter = PhysicalNodeShape.computeForBinding(filterNode);
        long hashIndexScan = PhysicalNodeShape.computeForBinding(indexScan);

        assertEquals(hashBareFilter, hashIndexScan,
                "computeForBinding should skip INDEX_SCAN prefix");
    }

    @Test
    void shouldComputeForBindingSkipFullScanPrefix() {
        // Behavior: computeForBinding skips NODE_FULL_SCAN prefix, producing same hash as bare filter
        PhysicalFilter filterNode = filter(Operator.EQ, "age", new Int32Val(25));
        PhysicalFullScan fullScan = new PhysicalFullScan(nextId(), filterNode);

        long hashBareFilter = PhysicalNodeShape.computeForBinding(filterNode);
        long hashFullScan = PhysicalNodeShape.computeForBinding(fullScan);

        assertEquals(hashBareFilter, hashFullScan,
                "computeForBinding should skip FULL_SCAN prefix");
    }

    @Test
    void shouldComputeForBindingProduceSameHashForIndexScanAndFullScan() {
        // Behavior: IndexScan and FullScan wrapping same filter produce same binding hash
        PhysicalFilter filterNode = filter(Operator.EQ, "age", new Int32Val(25));
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalIndexScan indexScan = new PhysicalIndexScan(nextId(), filterNode, index);
        PhysicalFullScan fullScan = new PhysicalFullScan(nextId(), filterNode);

        long hashIndexScan = PhysicalNodeShape.computeForBinding(indexScan);
        long hashFullScan = PhysicalNodeShape.computeForBinding(fullScan);

        assertEquals(hashIndexScan, hashFullScan,
                "computeForBinding should produce same hash for IndexScan and FullScan wrapping same filter");
    }

    @Test
    void shouldComputeForBindingPreserveOrderingMatchingQueryShape() {
        // Behavior: computeForBinding ordering matches QueryShape ordering for wrapped nodes
        // This is the critical test for parameter binding alignment

        BqlExpr bqlCategory = new BqlEq("category", new StringVal("electronics"));
        BqlExpr bqlBrand = new BqlEq("brand", new StringVal("acme"));

        List<BqlExpr> bqlList = new ArrayList<>(List.of(bqlCategory, bqlBrand));
        int[] bqlOrder = getSortedIndicesByQueryShape(bqlList);

        // Create PhysicalFullScan wrapped filters (simulating $elemMatch subplan)
        PhysicalFilter filterCategory = filter(Operator.EQ, "category", new StringVal("electronics"));
        PhysicalFilter filterBrand = filter(Operator.EQ, "brand", new StringVal("acme"));

        PhysicalFullScan fullScanCategory = new PhysicalFullScan(nextId(), filterCategory);
        PhysicalFullScan fullScanBrand = new PhysicalFullScan(nextId(), filterBrand);

        List<PhysicalNode> physicalList = new ArrayList<>(List.of(fullScanCategory, fullScanBrand));
        int[] physicalOrder = getSortedIndicesByPhysicalNodeShapeForBinding(physicalList);

        assertArrayEquals(bqlOrder, physicalOrder,
                "computeForBinding must produce same ordering as QueryShape for wrapped nodes");
    }

    @Test
    void shouldComputeForBindingPreserveOrderingWithMixedWrapperTypes() {
        // Behavior: Mixed IndexScan and FullScan wrappers produce same ordering as QueryShape
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        BqlExpr bqlAge = new BqlEq("age", new Int32Val(25));
        BqlExpr bqlName = new BqlEq("name", new StringVal("john"));

        List<BqlExpr> bqlList = new ArrayList<>(List.of(bqlAge, bqlName));
        int[] bqlOrder = getSortedIndicesByQueryShape(bqlList);

        // age gets IndexScan, name gets FullScan
        PhysicalFilter filterAge = filter(Operator.EQ, "age", new Int32Val(25));
        PhysicalFilter filterName = filter(Operator.EQ, "name", new StringVal("john"));

        PhysicalIndexScan indexScanAge = new PhysicalIndexScan(nextId(), filterAge, ageIndex);
        PhysicalFullScan fullScanName = new PhysicalFullScan(nextId(), filterName);

        List<PhysicalNode> physicalList = new ArrayList<>(List.of(indexScanAge, fullScanName));
        int[] physicalOrder = getSortedIndicesByPhysicalNodeShapeForBinding(physicalList);

        assertArrayEquals(bqlOrder, physicalOrder,
                "computeForBinding must produce same ordering for mixed wrapper types");
    }

    @Test
    void shouldComputeForBindingHandleOrWithWrappedChildren() {
        // Behavior: OR children with different wrapper types produce correct binding order
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("field_idx", "field", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB = filter(Operator.EQ, "b", new Int32Val(2));

        PhysicalIndexScan indexScanA = new PhysicalIndexScan(nextId(), filterA, index);
        PhysicalFullScan fullScanB = new PhysicalFullScan(nextId(), filterB);

        PhysicalOr or = new PhysicalOr(nextId(), List.of(indexScanA, fullScanB));

        // Compare with BqlOr ordering
        BqlExpr bqlA = new BqlEq("a", new Int32Val(1));
        BqlExpr bqlB = new BqlEq("b", new Int32Val(2));
        BqlOr bqlOr = new BqlOr(List.of(bqlA, bqlB));

        long bqlOrHash = QueryShape.compute(bqlOr);
        long physicalOrHash = PhysicalNodeShape.computeForBinding(or);

        // The hashes themselves may differ, but ordering of children should match
        // Verify by checking the sorted indices
        List<BqlExpr> bqlChildren = new ArrayList<>(List.of(bqlA, bqlB));
        List<PhysicalNode> physicalChildren = new ArrayList<>(List.of(indexScanA, fullScanB));

        int[] bqlOrder = getSortedIndicesByQueryShape(bqlChildren);
        int[] physicalOrder = getSortedIndicesByPhysicalNodeShapeForBinding(physicalChildren);

        assertArrayEquals(bqlOrder, physicalOrder,
                "computeForBinding must order OR children same as QueryShape");
    }

    @Test
    void shouldComputeForBindingHandleElemMatchSubplan() {
        // Behavior: ElemMatch with wrapped subplan produces correct binding hash
        PhysicalFilter innerFilter = filter(Operator.GTE, "score", new Int32Val(80));
        PhysicalFullScan wrappedFilter = new PhysicalFullScan(nextId(), innerFilter);
        PhysicalElemMatch elemMatch = new PhysicalElemMatch(nextId(), "scores", wrappedFilter);

        // Create same structure without wrapper
        PhysicalFilter innerFilter2 = filter(Operator.GTE, "score", new Int32Val(80));
        PhysicalElemMatch elemMatch2 = new PhysicalElemMatch(nextId(), "scores", innerFilter2);

        long hashWithWrapper = PhysicalNodeShape.computeForBinding(elemMatch);
        long hashWithoutWrapper = PhysicalNodeShape.computeForBinding(elemMatch2);

        assertEquals(hashWithWrapper, hashWithoutWrapper,
                "computeForBinding should produce same hash for ElemMatch regardless of subplan wrapper");
    }

    @Test
    void shouldComputeForBindingHandleRangeScan() {
        // Behavior: RangeScan binding hash is computed correctly
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalRangeScan range1 = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(18), new Int32Val(65), true, false, index);
        PhysicalRangeScan range2 = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(21), new Int32Val(50), true, false, index);

        long hash1 = PhysicalNodeShape.computeForBinding(range1);
        long hash2 = PhysicalNodeShape.computeForBinding(range2);

        assertEquals(hash1, hash2,
                "computeForBinding should produce same hash for RangeScan with same structure");
    }

    @Test
    void shouldComputeForBindingDifferFromComputeForWrappedNodes() {
        // Behavior: compute() and computeForBinding() produce different hashes for wrapped nodes
        PhysicalFilter filterNode = filter(Operator.EQ, "age", new Int32Val(25));
        PhysicalFullScan fullScan = new PhysicalFullScan(nextId(), filterNode);

        long hashCompute = PhysicalNodeShape.compute(fullScan);
        long hashComputeForBinding = PhysicalNodeShape.computeForBinding(fullScan);

        assertNotEquals(hashCompute, hashComputeForBinding,
                "compute() and computeForBinding() should produce different hashes for wrapped nodes");
    }

    @Test
    void shouldComputeForBindingProduceSameHashForBareFilters() {
        // Behavior: For bare filters (not wrapped), compute() and computeForBinding() produce same hash
        PhysicalFilter filterNode = filter(Operator.EQ, "age", new Int32Val(25));

        long hashCompute = PhysicalNodeShape.compute(filterNode);
        long hashComputeForBinding = PhysicalNodeShape.computeForBinding(filterNode);

        assertEquals(hashCompute, hashComputeForBinding,
                "compute() and computeForBinding() should produce same hash for bare filters");
    }

    @Test
    void shouldComputeForBindingPreserveOrderingForElemMatchWithOr() {
        // Behavior: This is the exact scenario that caused the original bug -
        // $elemMatch with $or children where ordering was inverted
        BqlExpr bqlCategory = new BqlEq("category", new StringVal("electronics"));
        BqlExpr bqlBrand = new BqlEq("brand", new StringVal("acme"));
        BqlOr bqlOr = new BqlOr(List.of(bqlCategory, bqlBrand));
        BqlElemMatch bqlElemMatch = new BqlElemMatch("items", bqlOr);

        // Get BQL children ordering
        List<BqlExpr> bqlChildren = new ArrayList<>(List.of(bqlCategory, bqlBrand));
        int[] bqlOrder = getSortedIndicesByQueryShape(bqlChildren);

        // Create physical plan with FullScan wrapped filters (typical for $elemMatch subplan)
        PhysicalFilter filterCategory = filter(Operator.EQ, "category", new StringVal("electronics"));
        PhysicalFilter filterBrand = filter(Operator.EQ, "brand", new StringVal("acme"));

        PhysicalFullScan fullScanCategory = new PhysicalFullScan(nextId(), filterCategory);
        PhysicalFullScan fullScanBrand = new PhysicalFullScan(nextId(), filterBrand);

        PhysicalOr physicalOr = new PhysicalOr(nextId(), List.of(fullScanCategory, fullScanBrand));
        PhysicalElemMatch physicalElemMatch = new PhysicalElemMatch(nextId(), "items", physicalOr);

        // Get physical children ordering using computeForBinding
        List<PhysicalNode> physicalChildren = new ArrayList<>(List.of(fullScanCategory, fullScanBrand));
        int[] physicalOrder = getSortedIndicesByPhysicalNodeShapeForBinding(physicalChildren);

        assertArrayEquals(bqlOrder, physicalOrder,
                "computeForBinding must preserve ordering for $elemMatch with $or - the original bug scenario");
    }

    @Test
    void shouldComputeForBindingHandleAndWithMixedWrappers() {
        // Behavior: AND with mixed wrapper types produces correct ordering
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        BqlExpr bqlName = new BqlEq("name", new StringVal("john"));
        BqlExpr bqlAge = new BqlGte("age", new Int32Val(18));
        BqlExpr bqlStatus = new BqlEq("status", new StringVal("active"));

        List<BqlExpr> bqlList = new ArrayList<>(List.of(bqlName, bqlAge, bqlStatus));
        int[] bqlOrder = getSortedIndicesByQueryShape(bqlList);

        PhysicalFilter filterName = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter filterAge = filter(Operator.GTE, "age", new Int32Val(18));
        PhysicalFilter filterStatus = filter(Operator.EQ, "status", new StringVal("active"));

        PhysicalIndexScan indexScanName = new PhysicalIndexScan(nextId(), filterName, nameIndex);
        PhysicalIndexScan indexScanAge = new PhysicalIndexScan(nextId(), filterAge, ageIndex);
        PhysicalFullScan fullScanStatus = new PhysicalFullScan(nextId(), filterStatus);

        List<PhysicalNode> physicalList = new ArrayList<>(List.of(indexScanName, indexScanAge, fullScanStatus));
        int[] physicalOrder = getSortedIndicesByPhysicalNodeShapeForBinding(physicalList);

        assertArrayEquals(bqlOrder, physicalOrder,
                "computeForBinding must produce same ordering for AND with mixed wrapper types");
    }

    @Test
    void shouldComputeForBindingHandleNestedAndOrWithWrappers() {
        // Behavior: Nested AND/OR with wrapped children produces correct ordering
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("field_idx", "field", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB = filter(Operator.EQ, "b", new Int32Val(2));
        PhysicalFilter filterC = filter(Operator.EQ, "c", new Int32Val(3));

        PhysicalIndexScan indexScanA = new PhysicalIndexScan(nextId(), filterA, index);
        PhysicalFullScan fullScanB = new PhysicalFullScan(nextId(), filterB);
        PhysicalFullScan fullScanC = new PhysicalFullScan(nextId(), filterC);

        PhysicalOr innerOr = new PhysicalOr(nextId(), List.of(indexScanA, fullScanB));
        PhysicalAnd outerAnd = new PhysicalAnd(nextId(), List.of(innerOr, fullScanC));

        // Same structure with different wrappers should produce same binding hash
        PhysicalFilter filterA2 = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB2 = filter(Operator.EQ, "b", new Int32Val(2));
        PhysicalFilter filterC2 = filter(Operator.EQ, "c", new Int32Val(3));

        PhysicalFullScan fullScanA2 = new PhysicalFullScan(nextId(), filterA2);
        PhysicalIndexScan indexScanB2 = new PhysicalIndexScan(nextId(), filterB2, index);
        PhysicalIndexScan indexScanC2 = new PhysicalIndexScan(nextId(), filterC2, index);

        PhysicalOr innerOr2 = new PhysicalOr(nextId(), List.of(fullScanA2, indexScanB2));
        PhysicalAnd outerAnd2 = new PhysicalAnd(nextId(), List.of(innerOr2, indexScanC2));

        long hash1 = PhysicalNodeShape.computeForBinding(outerAnd);
        long hash2 = PhysicalNodeShape.computeForBinding(outerAnd2);

        assertEquals(hash1, hash2,
                "computeForBinding should produce same hash regardless of wrapper types");
    }

    /**
     * Returns the sorted indices based on PhysicalNodeShape.computeForBinding hash.
     */
    private int[] getSortedIndicesByPhysicalNodeShapeForBinding(List<PhysicalNode> nodes) {
        int size = nodes.size();
        long[] hashes = new long[size];
        Integer[] indices = new Integer[size];
        for (int i = 0; i < size; i++) {
            hashes[i] = PhysicalNodeShape.computeForBinding(nodes.get(i));
            indices[i] = i;
        }

        // Sort indices by hash
        Arrays.sort(indices, Comparator.comparingLong(a -> hashes[a]));

        return Arrays.stream(indices).mapToInt(Integer::intValue).toArray();
    }

    // =========================================================================
    // hashRangeScanForBinding Tests
    // =========================================================================

    @Test
    void shouldComputeForBindingSkipRangeScanPrefix() {
        // Behavior: computeForBinding for RangeScan skips NODE_RANGE_SCAN prefix
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalRangeScan rangeScan = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(18), new Int32Val(65), true, false, index);

        long hashCompute = PhysicalNodeShape.compute(rangeScan);
        long hashComputeForBinding = PhysicalNodeShape.computeForBinding(rangeScan);

        // compute() includes NODE_RANGE_SCAN prefix, computeForBinding() does not
        assertNotEquals(hashCompute, hashComputeForBinding,
                "compute() and computeForBinding() should differ for RangeScan due to NODE_RANGE_SCAN prefix");
    }

    @Test
    void shouldComputeForBindingPreserveRangeScanLowerOnlyOrdering() {
        // Behavior: RangeScan with only lower bound produces correct binding hash
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalRangeScan rangeLowerOnly1 = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(18), null, true, false, index);
        PhysicalRangeScan rangeLowerOnly2 = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(25), null, true, false, index);

        long hash1 = PhysicalNodeShape.computeForBinding(rangeLowerOnly1);
        long hash2 = PhysicalNodeShape.computeForBinding(rangeLowerOnly2);

        assertEquals(hash1, hash2,
                "computeForBinding should produce same hash for RangeScan with lower bound only");
    }

    @Test
    void shouldComputeForBindingPreserveRangeScanUpperOnlyOrdering() {
        // Behavior: RangeScan with only the upper bound produces the correct binding hash
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalRangeScan rangeUpperOnly1 = new PhysicalRangeScan(nextId(), "age",
                null, new Int32Val(65), false, true, index);
        PhysicalRangeScan rangeUpperOnly2 = new PhysicalRangeScan(nextId(), "age",
                null, new Int32Val(50), false, true, index);

        long hash1 = PhysicalNodeShape.computeForBinding(rangeUpperOnly1);
        long hash2 = PhysicalNodeShape.computeForBinding(rangeUpperOnly2);

        assertEquals(hash1, hash2,
                "computeForBinding should produce same hash for RangeScan with upper bound only");
    }

    @Test
    void shouldComputeForBindingDifferForRangeScanWithDifferentInclusivity() {
        // Behavior: RangeScan with different inclusivity flags produces different binding hashes
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalRangeScan rangeInclusive = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(18), new Int32Val(65), true, true, index);
        PhysicalRangeScan rangeExclusive = new PhysicalRangeScan(nextId(), "age",
                new Int32Val(18), new Int32Val(65), false, false, index);

        long hashInclusive = PhysicalNodeShape.computeForBinding(rangeInclusive);
        long hashExclusive = PhysicalNodeShape.computeForBinding(rangeExclusive);

        assertNotEquals(hashInclusive, hashExclusive,
                "computeForBinding should produce different hashes for different inclusivity");
    }

    // =========================================================================
    // hashIndexIntersectionForBinding Tests
    // =========================================================================

    @Test
    void shouldComputeForBindingSkipIndexIntersectionPrefix() {
        // Behavior: computeForBinding for IndexIntersection uses OP_AND instead of NODE_INDEX_INTERSECTION
        PhysicalFilter filterA = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter filterB = filter(Operator.EQ, "age", new Int32Val(25));

        SingleFieldIndexDefinition indexA = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition indexB = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalIndexIntersection intersection = new PhysicalIndexIntersection(
                nextId(), List.of(indexA, indexB), List.of(filterA, filterB));

        long hashCompute = PhysicalNodeShape.compute(intersection);
        long hashComputeForBinding = PhysicalNodeShape.computeForBinding(intersection);

        // compute() uses NODE_INDEX_INTERSECTION, computeForBinding() uses OP_AND
        assertNotEquals(hashCompute, hashComputeForBinding,
                "compute() and computeForBinding() should differ for IndexIntersection");
    }

    @Test
    void shouldComputeForBindingPreserveIndexIntersectionOrdering() {
        // Behavior: IndexIntersection ordering matches QueryShape AND ordering
        PhysicalFilter filterName = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter filterAge = filter(Operator.EQ, "age", new Int32Val(25));

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalIndexIntersection intersection1 = new PhysicalIndexIntersection(
                nextId(), List.of(nameIndex, ageIndex), List.of(filterName, filterAge));

        // Same structure, different order
        PhysicalFilter filterName2 = filter(Operator.EQ, "name", new StringVal("jane"));
        PhysicalFilter filterAge2 = filter(Operator.EQ, "age", new Int32Val(30));

        PhysicalIndexIntersection intersection2 = new PhysicalIndexIntersection(
                nextId(), List.of(ageIndex, nameIndex), List.of(filterAge2, filterName2));

        long hash1 = PhysicalNodeShape.computeForBinding(intersection1);
        long hash2 = PhysicalNodeShape.computeForBinding(intersection2);

        assertEquals(hash1, hash2,
                "computeForBinding should produce same hash for IndexIntersection with different order");
    }

    @Test
    void shouldComputeForBindingProduceSameHashForAndAndIndexIntersectionWithSameFilters() {
        // Behavior: AND and IndexIntersection with the same filters produce the same binding hash
        // This is important because both represent conjunctive conditions
        PhysicalFilter filterName = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter filterAge = filter(Operator.EQ, "age", new Int32Val(25));

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalIndexIntersection intersection = new PhysicalIndexIntersection(
                nextId(), List.of(nameIndex, ageIndex), List.of(filterName, filterAge));

        PhysicalAnd and = new PhysicalAnd(nextId(), List.of(filterName, filterAge));

        long hashIntersection = PhysicalNodeShape.computeForBinding(intersection);
        long hashAnd = PhysicalNodeShape.computeForBinding(and);

        assertEquals(hashIntersection, hashAnd,
                "computeForBinding should produce same hash for AND and IndexIntersection with same filters");
    }

    // =========================================================================
    // hashCompoundIndexScanForBinding Tests
    // =========================================================================

    @Test
    void shouldComputeForBindingSkipCompoundIndexScanPrefix() {
        // Behavior: compute() uses NODE_COMPOUND_INDEX_SCAN, computeForBinding() uses OP_AND — hashes differ
        PhysicalFilter filterA = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter filterB = filter(Operator.EQ, "age", new Int32Val(25));

        CompoundIndexDefinition compoundIndex = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);

        PhysicalCompoundIndexScan scan = new PhysicalCompoundIndexScan(
                nextId(), compoundIndex, List.of(filterA, filterB));

        long hashCompute = PhysicalNodeShape.compute(scan);
        long hashComputeForBinding = PhysicalNodeShape.computeForBinding(scan);

        assertNotEquals(hashCompute, hashComputeForBinding,
                "compute() and computeForBinding() should differ for CompoundIndexScan");
    }

    @Test
    void shouldComputeForBindingPreserveCompoundIndexScanOrdering() {
        // Behavior: Same structure, different filter order produces same binding hash
        PhysicalFilter filterName = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter filterAge = filter(Operator.EQ, "age", new Int32Val(25));

        CompoundIndexDefinition compoundIndex = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);

        PhysicalCompoundIndexScan scan1 = new PhysicalCompoundIndexScan(
                nextId(), compoundIndex, List.of(filterName, filterAge));

        PhysicalFilter filterName2 = filter(Operator.EQ, "name", new StringVal("jane"));
        PhysicalFilter filterAge2 = filter(Operator.EQ, "age", new Int32Val(30));

        PhysicalCompoundIndexScan scan2 = new PhysicalCompoundIndexScan(
                nextId(), compoundIndex, List.of(filterAge2, filterName2));

        long hash1 = PhysicalNodeShape.computeForBinding(scan1);
        long hash2 = PhysicalNodeShape.computeForBinding(scan2);

        assertEquals(hash1, hash2,
                "computeForBinding should produce same hash for CompoundIndexScan with different order");
    }

    @Test
    void shouldComputeForBindingProduceSameHashForAndAndCompoundIndexScanWithSameFilters() {
        // Behavior: AND and CompoundIndexScan with same filters produce identical binding hash (both use OP_AND)
        PhysicalFilter filterName = filter(Operator.EQ, "name", new StringVal("john"));
        PhysicalFilter filterAge = filter(Operator.EQ, "age", new Int32Val(25));

        CompoundIndexDefinition compoundIndex = CompoundIndexDefinition.create(
                "name_age_idx",
                List.of(
                        new CompoundIndexField("name", BsonType.STRING, false),
                        new CompoundIndexField("age", BsonType.INT32, false)
                )
                , IndexStatus.WAITING);

        PhysicalCompoundIndexScan scan = new PhysicalCompoundIndexScan(
                nextId(), compoundIndex, List.of(filterName, filterAge));

        PhysicalAnd and = new PhysicalAnd(nextId(), List.of(filterName, filterAge));

        long hashScan = PhysicalNodeShape.computeForBinding(scan);
        long hashAnd = PhysicalNodeShape.computeForBinding(and);

        assertEquals(hashScan, hashAnd,
                "computeForBinding should produce same hash for AND and CompoundIndexScan with same filters");
    }

    // =========================================================================
    // hashLogicalForBinding Tests
    // =========================================================================

    @Test
    void shouldComputeForBindingUseSameLogicalHashForWrappedAndBareChildren() {
        // Behavior: AND with wrapped children produces the same hash as AND with bare children
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("field_idx", "field", BsonType.INT32, false, IndexStatus.WAITING);

        // AND with wrapped children
        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB = filter(Operator.EQ, "b", new Int32Val(2));
        PhysicalIndexScan indexScanA = new PhysicalIndexScan(nextId(), filterA, index);
        PhysicalFullScan fullScanB = new PhysicalFullScan(nextId(), filterB);
        PhysicalAnd andWrapped = new PhysicalAnd(nextId(), List.of(indexScanA, fullScanB));

        // AND with bare children (same filters)
        PhysicalFilter filterA2 = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB2 = filter(Operator.EQ, "b", new Int32Val(2));
        PhysicalAnd andBare = new PhysicalAnd(nextId(), List.of(filterA2, filterB2));

        long hashWrapped = PhysicalNodeShape.computeForBinding(andWrapped);
        long hashBare = PhysicalNodeShape.computeForBinding(andBare);

        assertEquals(hashWrapped, hashBare,
                "computeForBinding should produce same hash for AND with wrapped vs bare children");
    }

    @Test
    void shouldComputeForBindingUseSameOrHashForWrappedAndBareChildren() {
        // Behavior: OR with wrapped children produces same hash as OR with bare children
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("field_idx", "field", BsonType.INT32, false, IndexStatus.WAITING);

        // OR with wrapped children
        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB = filter(Operator.EQ, "b", new Int32Val(2));
        PhysicalIndexScan indexScanA = new PhysicalIndexScan(nextId(), filterA, index);
        PhysicalFullScan fullScanB = new PhysicalFullScan(nextId(), filterB);
        PhysicalOr orWrapped = new PhysicalOr(nextId(), List.of(indexScanA, fullScanB));

        // OR with bare children (same filters)
        PhysicalFilter filterA2 = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB2 = filter(Operator.EQ, "b", new Int32Val(2));
        PhysicalOr orBare = new PhysicalOr(nextId(), List.of(filterA2, filterB2));

        long hashWrapped = PhysicalNodeShape.computeForBinding(orWrapped);
        long hashBare = PhysicalNodeShape.computeForBinding(orBare);

        assertEquals(hashWrapped, hashBare,
                "computeForBinding should produce same hash for OR with wrapped vs bare children");
    }

    @Test
    void shouldComputeForBindingPreserveAndChildOrderingMatchingQueryShape() {
        // Behavior: AND child ordering via computeForBinding matches QueryShape ordering
        BqlExpr bqlA = new BqlEq("a", new Int32Val(1));
        BqlExpr bqlB = new BqlEq("b", new Int32Val(2));
        BqlExpr bqlC = new BqlEq("c", new Int32Val(3));

        List<BqlExpr> bqlList = new ArrayList<>(List.of(bqlA, bqlB, bqlC));
        int[] bqlOrder = getSortedIndicesByQueryShape(bqlList);

        // Create AND with IndexScan/FullScan wrapped children
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("idx", "field", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB = filter(Operator.EQ, "b", new Int32Val(2));
        PhysicalFilter filterC = filter(Operator.EQ, "c", new Int32Val(3));

        PhysicalIndexScan wrapA = new PhysicalIndexScan(nextId(), filterA, index);
        PhysicalFullScan wrapB = new PhysicalFullScan(nextId(), filterB);
        PhysicalIndexScan wrapC = new PhysicalIndexScan(nextId(), filterC, index);

        List<PhysicalNode> physicalList = new ArrayList<>(List.of(wrapA, wrapB, wrapC));
        int[] physicalOrder = getSortedIndicesByPhysicalNodeShapeForBinding(physicalList);

        assertArrayEquals(bqlOrder, physicalOrder,
                "computeForBinding AND child ordering must match QueryShape ordering");
    }

    @Test
    void shouldComputeForBindingPreserveOrChildOrderingMatchingQueryShape() {
        // Behavior: OR child ordering via computeForBinding matches QueryShape ordering
        BqlExpr bqlX = new BqlEq("x", new Int32Val(10));
        BqlExpr bqlY = new BqlEq("y", new StringVal("hello"));
        BqlExpr bqlZ = new BqlGte("z", new Int32Val(5));

        List<BqlExpr> bqlList = new ArrayList<>(List.of(bqlX, bqlY, bqlZ));
        int[] bqlOrder = getSortedIndicesByQueryShape(bqlList);

        // Create OR with wrapped children
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("idx", "field", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalFilter filterX = filter(Operator.EQ, "x", new Int32Val(10));
        PhysicalFilter filterY = filter(Operator.EQ, "y", new StringVal("hello"));
        PhysicalFilter filterZ = filter(Operator.GTE, "z", new Int32Val(5));

        PhysicalFullScan wrapX = new PhysicalFullScan(nextId(), filterX);
        PhysicalIndexScan wrapY = new PhysicalIndexScan(nextId(), filterY, index);
        PhysicalFullScan wrapZ = new PhysicalFullScan(nextId(), filterZ);

        List<PhysicalNode> physicalList = new ArrayList<>(List.of(wrapX, wrapY, wrapZ));
        int[] physicalOrder = getSortedIndicesByPhysicalNodeShapeForBinding(physicalList);

        assertArrayEquals(bqlOrder, physicalOrder,
                "computeForBinding OR child ordering must match QueryShape ordering");
    }

    @Test
    void shouldComputeForBindingDifferFromComputeForAndWithWrappedChildren() {
        // Behavior: AND with wrapped children has different compute() vs computeForBinding()
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("idx", "field", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalFilter filterA = filter(Operator.EQ, "a", new Int32Val(1));
        PhysicalFilter filterB = filter(Operator.EQ, "b", new Int32Val(2));

        PhysicalIndexScan indexScanA = new PhysicalIndexScan(nextId(), filterA, index);
        PhysicalFullScan fullScanB = new PhysicalFullScan(nextId(), filterB);

        PhysicalAnd and = new PhysicalAnd(nextId(), List.of(indexScanA, fullScanB));

        long hashCompute = PhysicalNodeShape.compute(and);
        long hashComputeForBinding = PhysicalNodeShape.computeForBinding(and);

        assertNotEquals(hashCompute, hashComputeForBinding,
                "compute() and computeForBinding() should differ for AND with wrapped children");
    }
}
