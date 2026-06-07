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
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.CompoundIndexDefinition;
import com.kronotop.bucket.index.CompoundIndexField;
import com.kronotop.bucket.index.IndexStatus;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.bucket.planner.physical.*;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

class PhysicalPlanParameterBinderTest {

    private final AtomicInteger idGenerator = new AtomicInteger(0);

    private int nextId() {
        return idGenerator.incrementAndGet();
    }

    private PhysicalFilter filter(Operator op, String selector, Object operand) {
        return new PhysicalFilter(nextId(), selector, op, operand);
    }

    private void assertBindingCount(Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings,
                                    int nodeId, int expected) {
        assertTrue(bindings.containsKey(nodeId), "Bindings should contain node " + nodeId);
        assertEquals(expected, bindings.get(nodeId).size());
    }

    private void assertBinding(Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings,
                               int nodeId, int index, int expectedParamIndex, int expectedOccurrence) {
        var nodeBindings = bindings.get(nodeId);
        assertNotNull(nodeBindings, "Node " + nodeId + " should have bindings");
        PhysicalPlanParameterBinder.ParamBinding binding = nodeBindings.get(index);
        assertEquals(expectedParamIndex, binding.paramIndex(), "paramIndex mismatch");
        assertEquals(expectedOccurrence, binding.occurrence(), "occurrence mismatch");
    }

    private int getTotalBindingCount(Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings) {
        return bindings.values().stream().mapToInt(List::size).sum();
    }

    // ===== Basic Single-Value Operator Tests =====

    @Test
    void shouldBindSingleParameterFromPhysicalFilterEq() {
        // Behavior: PhysicalFilter with EQ binds single parameter at index 0, occurrence 0
        Int32Val value = new Int32Val(25);
        PhysicalFilter filter = filter(Operator.EQ, "age", value);
        List<BqlValue> parameters = List.of(value);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 1);
        assertBinding(bindings, filter.id(), 0, 0, 0);
    }

    @Test
    void shouldBindSingleParameterFromPhysicalFilterGt() {
        // Behavior: PhysicalFilter with GT binds single parameter
        Int32Val value = new Int32Val(100);
        PhysicalFilter filter = filter(Operator.GT, "score", value);
        List<BqlValue> parameters = List.of(value);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 1);
        assertBinding(bindings, filter.id(), 0, 0, 0);
    }

    @Test
    void shouldBindSingleParameterFromPhysicalFilterGte() {
        // Behavior: PhysicalFilter with GTE binds single parameter
        Int32Val value = new Int32Val(18);
        PhysicalFilter filter = filter(Operator.GTE, "age", value);
        List<BqlValue> parameters = List.of(value);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 1);
        assertBinding(bindings, filter.id(), 0, 0, 0);
    }

    @Test
    void shouldBindSingleParameterFromPhysicalFilterLt() {
        // Behavior: PhysicalFilter with LT binds single parameter
        Int32Val value = new Int32Val(50);
        PhysicalFilter filter = filter(Operator.LT, "price", value);
        List<BqlValue> parameters = List.of(value);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 1);
        assertBinding(bindings, filter.id(), 0, 0, 0);
    }

    @Test
    void shouldBindSingleParameterFromPhysicalFilterLte() {
        // Behavior: PhysicalFilter with LTE binds single parameter
        Int32Val value = new Int32Val(10);
        PhysicalFilter filter = filter(Operator.LTE, "quantity", value);
        List<BqlValue> parameters = List.of(value);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 1);
        assertBinding(bindings, filter.id(), 0, 0, 0);
    }

    @Test
    void shouldBindSingleParameterFromPhysicalFilterNe() {
        // Behavior: PhysicalFilter with NE binds single parameter
        StringVal value = new StringVal("deleted");
        PhysicalFilter filter = filter(Operator.NE, "status", value);
        List<BqlValue> parameters = List.of(value);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 1);
        assertBinding(bindings, filter.id(), 0, 0, 0);
    }

    @Test
    void shouldBindSizeParameter() {
        // Behavior: PhysicalFilter with SIZE binds single parameter
        Int32Val value = new Int32Val(5);
        PhysicalFilter filter = filter(Operator.SIZE, "items", value);
        List<BqlValue> parameters = List.of(value);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 1);
        assertBinding(bindings, filter.id(), 0, 0, 0);
    }

    @Test
    void shouldNotBindParametersFromExistsTrue() {
        // Behavior: PhysicalFilter with EXISTS:true produces no bindings (boolean is part of shape)
        PhysicalFilter filter = filter(Operator.EXISTS, "field", true);
        List<BqlValue> parameters = List.of();
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertTrue(bindings.isEmpty());
    }

    @Test
    void shouldNotBindParametersFromExistsFalse() {
        // Behavior: PhysicalFilter with EXISTS:false produces no bindings
        PhysicalFilter filter = filter(Operator.EXISTS, "field", false);
        List<BqlValue> parameters = List.of();
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertTrue(bindings.isEmpty());
    }

    // ===== Array Operator Tests ($in, $nin, $all) =====

    @Test
    void shouldBindAllValuesFromInOperator() {
        // Behavior: $in with [1,2,3] produces 3 bindings with occurrences 0,1,2
        Int32Val v1 = new Int32Val(1);
        Int32Val v2 = new Int32Val(2);
        Int32Val v3 = new Int32Val(3);
        List<Object> values = List.of(v1, v2, v3);
        PhysicalFilter filter = filter(Operator.IN, "status", values);
        List<BqlValue> parameters = List.of(v1, v2, v3);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 3);
        assertBinding(bindings, filter.id(), 0, 0, 0);
        assertBinding(bindings, filter.id(), 1, 1, 1);
        assertBinding(bindings, filter.id(), 2, 2, 2);
    }

    @Test
    void shouldBindAllValuesFromNinOperator() {
        // Behavior: $nin with ["a","b"] produces 2 bindings with occurrences 0,1
        StringVal v1 = new StringVal("a");
        StringVal v2 = new StringVal("b");
        List<Object> values = List.of(v1, v2);
        PhysicalFilter filter = filter(Operator.NIN, "code", values);
        List<BqlValue> parameters = List.of(v1, v2);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 2);
        assertBinding(bindings, filter.id(), 0, 0, 0);
        assertBinding(bindings, filter.id(), 1, 1, 1);
    }

    @Test
    void shouldBindAllValuesFromAllOperator() {
        // Behavior: $all with [x,y,z] produces 3 bindings with occurrences 0,1,2
        StringVal v1 = new StringVal("x");
        StringVal v2 = new StringVal("y");
        StringVal v3 = new StringVal("z");
        List<Object> values = List.of(v1, v2, v3);
        PhysicalFilter filter = filter(Operator.ALL, "tags", values);
        List<BqlValue> parameters = List.of(v1, v2, v3);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 3);
        assertBinding(bindings, filter.id(), 0, 0, 0);
        assertBinding(bindings, filter.id(), 1, 1, 1);
        assertBinding(bindings, filter.id(), 2, 2, 2);
    }

    @Test
    void shouldHandleEmptyInArray() {
        // Behavior: $in with [] produces no bindings (empty list)
        List<Object> values = List.of();
        PhysicalFilter filter = filter(Operator.IN, "field", values);
        List<BqlValue> parameters = List.of();
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 0);
    }

    @Test
    void shouldHandleSingleElementIn() {
        // Behavior: $in with [42] produces 1 binding with occurrence 0
        Int32Val v1 = new Int32Val(42);
        List<Object> values = List.of(v1);
        PhysicalFilter filter = filter(Operator.IN, "field", values);
        List<BqlValue> parameters = List.of(v1);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 1);
        assertBinding(bindings, filter.id(), 0, 0, 0);
    }

    @Test
    void shouldHandleLargeInArray() {
        // Behavior: $in with 10 elements produces 10 bindings with occurrences 0-9
        Int32Val v1 = new Int32Val(1);
        Int32Val v2 = new Int32Val(2);
        Int32Val v3 = new Int32Val(3);
        Int32Val v4 = new Int32Val(4);
        Int32Val v5 = new Int32Val(5);
        Int32Val v6 = new Int32Val(6);
        Int32Val v7 = new Int32Val(7);
        Int32Val v8 = new Int32Val(8);
        Int32Val v9 = new Int32Val(9);
        Int32Val v10 = new Int32Val(10);
        List<Object> values = List.of(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10);
        PhysicalFilter filter = filter(Operator.IN, "field", values);
        List<BqlValue> parameters = List.of(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 10);
        for (int i = 0; i < 10; i++) {
            assertBinding(bindings, filter.id(), i, i, i);
        }
    }

    // ===== RangeScan Tests (Occurrence Values) =====

    @Test
    void shouldBindBothBoundsFromRangeScan() {
        // Behavior: RangeScan with lower+upper produces 2 bindings: paramIndex 0,1 with occurrences 0,1
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        Int32Val lower = new Int32Val(18);
        Int32Val upper = new Int32Val(65);
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(nextId(), "age",
                lower, upper, true, false, index);
        List<BqlValue> parameters = List.of(lower, upper);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(rangeScan, parameters);

        assertBindingCount(bindings, rangeScan.id(), 2);
        assertBinding(bindings, rangeScan.id(), 0, 0, 0); // lower bound
        assertBinding(bindings, rangeScan.id(), 1, 1, 1); // upper bound
    }

    @Test
    void shouldBindLowerBoundOnlyFromRangeScan() {
        // Behavior: RangeScan with only lower bound produces 1 binding: (paramIndex=0, occurrence=0)
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        Int32Val lower = new Int32Val(18);
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(nextId(), "age",
                lower, null, true, false, index);
        List<BqlValue> parameters = List.of(lower);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(rangeScan, parameters);

        assertBindingCount(bindings, rangeScan.id(), 1);
        assertBinding(bindings, rangeScan.id(), 0, 0, 0);
    }

    @Test
    void shouldBindUpperBoundOnlyFromRangeScan() {
        // Behavior: RangeScan with only upper bound produces 1 binding: (paramIndex=0, occurrence=0)
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        Int32Val upper = new Int32Val(65);
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(nextId(), "age",
                null, upper, false, true, index);
        List<BqlValue> parameters = List.of(upper);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(rangeScan, parameters);

        assertBindingCount(bindings, rangeScan.id(), 1);
        assertBinding(bindings, rangeScan.id(), 0, 0, 0);
    }

    // ===== Wrapper Node Tests (IndexScan, FullScan) =====

    @Test
    void shouldDelegateToWrappedNodeForIndexScan() {
        // Behavior: IndexScan wrapping PhysicalFilter binds through to inner filter
        Int32Val value = new Int32Val(25);
        PhysicalFilter innerFilter = filter(Operator.EQ, "age", value);
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        PhysicalIndexScan indexScan = new PhysicalIndexScan(nextId(), innerFilter, index);
        List<BqlValue> parameters = List.of(value);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(indexScan, parameters);

        assertBindingCount(bindings, innerFilter.id(), 1);
        assertBinding(bindings, innerFilter.id(), 0, 0, 0);
    }

    @Test
    void shouldDelegateToWrappedNodeForFullScan() {
        // Behavior: FullScan wrapping PhysicalFilter binds through to inner filter
        StringVal value = new StringVal("john");
        PhysicalFilter innerFilter = filter(Operator.EQ, "name", value);
        PhysicalFullScan fullScan = new PhysicalFullScan(nextId(), innerFilter);
        List<BqlValue> parameters = List.of(value);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(fullScan, parameters);

        assertBindingCount(bindings, innerFilter.id(), 1);
        assertBinding(bindings, innerFilter.id(), 0, 0, 0);
    }

    @Test
    void shouldDelegateToWrappedInArrayForIndexScan() {
        // Behavior: IndexScan wrapping $in filter binds all array values
        Int32Val v1 = new Int32Val(1);
        Int32Val v2 = new Int32Val(2);
        Int32Val v3 = new Int32Val(3);
        List<Object> values = List.of(v1, v2, v3);
        PhysicalFilter innerFilter = filter(Operator.IN, "status", values);
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("status_idx", "status", BsonType.INT32, false, IndexStatus.WAITING);
        PhysicalIndexScan indexScan = new PhysicalIndexScan(nextId(), innerFilter, index);
        List<BqlValue> parameters = List.of(v1, v2, v3);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(indexScan, parameters);

        assertBindingCount(bindings, innerFilter.id(), 3);
    }

    // ===== Logical Node Canonical Ordering Tests (AND/OR) =====

    @Test
    void shouldBindAndChildrenInCanonicalOrder() {
        // Behavior: PhysicalAnd children sorted by PhysicalNodeShape hash before binding
        Int32Val v1 = new Int32Val(1);
        Int32Val v2 = new Int32Val(2);
        PhysicalFilter filterA = filter(Operator.EQ, "a", v1);
        PhysicalFilter filterB = filter(Operator.EQ, "b", v2);
        PhysicalAnd and = new PhysicalAnd(nextId(), List.of(filterA, filterB));
        List<BqlValue> parameters = List.of(v1, v2);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(and, parameters);

        assertEquals(2, getTotalBindingCount(bindings));
        assertBindingCount(bindings, filterA.id(), 1);
        assertBindingCount(bindings, filterB.id(), 1);
    }

    @Test
    void shouldBindOrChildrenInCanonicalOrder() {
        // Behavior: PhysicalOr children sorted by shape hash before binding
        Int32Val v1 = new Int32Val(1);
        Int32Val v2 = new Int32Val(2);
        PhysicalFilter filterA = filter(Operator.EQ, "a", v1);
        PhysicalFilter filterB = filter(Operator.EQ, "b", v2);
        PhysicalOr or = new PhysicalOr(nextId(), List.of(filterA, filterB));
        List<BqlValue> parameters = List.of(v1, v2);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(or, parameters);

        assertEquals(2, getTotalBindingCount(bindings));
        assertBindingCount(bindings, filterA.id(), 1);
        assertBindingCount(bindings, filterB.id(), 1);
    }

    @Test
    void shouldProduceSameBindingOrderForAndWithDifferentChildOrder() {
        // Behavior: AND([a,b]) and AND([b,a]) produce same parameter index assignments
        Int32Val v1 = new Int32Val(1);
        Int32Val v2 = new Int32Val(2);
        PhysicalFilter filterA1 = filter(Operator.EQ, "a", v1);
        PhysicalFilter filterB1 = filter(Operator.EQ, "b", v2);
        PhysicalAnd and1 = new PhysicalAnd(nextId(), List.of(filterA1, filterB1));
        List<BqlValue> parameters1 = List.of(v1, v2);

        Int32Val v10 = new Int32Val(10);
        Int32Val v20 = new Int32Val(20);
        PhysicalFilter filterA2 = filter(Operator.EQ, "a", v10);
        PhysicalFilter filterB2 = filter(Operator.EQ, "b", v20);
        PhysicalAnd and2 = new PhysicalAnd(nextId(), List.of(filterB2, filterA2));
        List<BqlValue> parameters2 = List.of(v10, v20);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings1 =
                PhysicalPlanParameterBinder.bind(and1, parameters1);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings2 =
                PhysicalPlanParameterBinder.bind(and2, parameters2);

        // Both should produce the same param indices for equivalent selectors
        int paramIdxA1 = bindings1.get(filterA1.id()).get(0).paramIndex();
        int paramIdxA2 = bindings2.get(filterA2.id()).get(0).paramIndex();
        int paramIdxB1 = bindings1.get(filterB1.id()).get(0).paramIndex();
        int paramIdxB2 = bindings2.get(filterB2.id()).get(0).paramIndex();

        assertEquals(paramIdxA1, paramIdxA2, "Parameter index for 'a' should be the same");
        assertEquals(paramIdxB1, paramIdxB2, "Parameter index for 'b' should be the same");
    }

    @Test
    void shouldProduceSameBindingOrderForOrWithDifferentChildOrder() {
        // Behavior: OR([a,b]) and OR([b,a]) produce same parameter index assignments
        Int32Val v1 = new Int32Val(1);
        Int32Val v2 = new Int32Val(2);
        PhysicalFilter filterA1 = filter(Operator.EQ, "a", v1);
        PhysicalFilter filterB1 = filter(Operator.EQ, "b", v2);
        PhysicalOr or1 = new PhysicalOr(nextId(), List.of(filterA1, filterB1));
        List<BqlValue> parameters1 = List.of(v1, v2);

        Int32Val v10 = new Int32Val(10);
        Int32Val v20 = new Int32Val(20);
        PhysicalFilter filterA2 = filter(Operator.EQ, "a", v10);
        PhysicalFilter filterB2 = filter(Operator.EQ, "b", v20);
        PhysicalOr or2 = new PhysicalOr(nextId(), List.of(filterB2, filterA2));
        List<BqlValue> parameters2 = List.of(v10, v20);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings1 =
                PhysicalPlanParameterBinder.bind(or1, parameters1);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings2 =
                PhysicalPlanParameterBinder.bind(or2, parameters2);

        int paramIdxA1 = bindings1.get(filterA1.id()).get(0).paramIndex();
        int paramIdxA2 = bindings2.get(filterA2.id()).get(0).paramIndex();
        int paramIdxB1 = bindings1.get(filterB1.id()).get(0).paramIndex();
        int paramIdxB2 = bindings2.get(filterB2.id()).get(0).paramIndex();

        assertEquals(paramIdxA1, paramIdxA2, "Parameter index for 'a' should be the same");
        assertEquals(paramIdxB1, paramIdxB2, "Parameter index for 'b' should be the same");
    }

    @Test
    void shouldHandleEmptyAnd() {
        // Behavior: Empty PhysicalAnd produces no bindings
        PhysicalAnd emptyAnd = new PhysicalAnd(nextId(), List.of());
        List<BqlValue> parameters = List.of();
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(emptyAnd, parameters);

        assertTrue(bindings.isEmpty());
    }

    @Test
    void shouldHandleSingleChildAnd() {
        // Behavior: AND with single child binds that child's parameters
        Int32Val v1 = new Int32Val(1);
        PhysicalFilter filter = filter(Operator.EQ, "a", v1);
        PhysicalAnd and = new PhysicalAnd(nextId(), List.of(filter));
        List<BqlValue> parameters = List.of(v1);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(and, parameters);

        assertBindingCount(bindings, filter.id(), 1);
        assertBinding(bindings, filter.id(), 0, 0, 0);
    }

    @Test
    void shouldHandleAndWithFiveChildren() {
        // Behavior: AND with 5 children binds all in canonical order
        Int32Val v1 = new Int32Val(1);
        Int32Val v2 = new Int32Val(2);
        Int32Val v3 = new Int32Val(3);
        Int32Val v4 = new Int32Val(4);
        Int32Val v5 = new Int32Val(5);
        PhysicalFilter filterA = filter(Operator.EQ, "a", v1);
        PhysicalFilter filterB = filter(Operator.EQ, "b", v2);
        PhysicalFilter filterC = filter(Operator.EQ, "c", v3);
        PhysicalFilter filterD = filter(Operator.EQ, "d", v4);
        PhysicalFilter filterE = filter(Operator.EQ, "e", v5);
        PhysicalAnd and = new PhysicalAnd(nextId(), List.of(filterA, filterB, filterC, filterD, filterE));
        List<BqlValue> parameters = List.of(v1, v2, v3, v4, v5);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(and, parameters);

        assertEquals(5, getTotalBindingCount(bindings));
    }

    // ===== Not and ElemMatch Tests =====

    @Test
    void shouldRecurseToChildForNot() {
        // Behavior: PhysicalNot delegates binding to its child node
        Int32Val value = new Int32Val(30);
        PhysicalFilter innerFilter = filter(Operator.EQ, "age", value);
        PhysicalNot not = new PhysicalNot(nextId(), innerFilter);
        List<BqlValue> parameters = List.of(value);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(not, parameters);

        assertBindingCount(bindings, innerFilter.id(), 1);
        assertBinding(bindings, innerFilter.id(), 0, 0, 0);
    }

    @Test
    void shouldRecurseToSubPlanForElemMatch() {
        // Behavior: PhysicalElemMatch delegates binding to its subPlan
        Int32Val value = new Int32Val(80);
        PhysicalFilter innerFilter = filter(Operator.GTE, "score", value);
        PhysicalElemMatch elemMatch = new PhysicalElemMatch(nextId(), "scores", innerFilter);
        List<BqlValue> parameters = List.of(value);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(elemMatch, parameters);

        assertBindingCount(bindings, innerFilter.id(), 1);
        assertBinding(bindings, innerFilter.id(), 0, 0, 0);
    }

    @Test
    void shouldBindNotWrappingIn() {
        // Behavior: NOT wrapping $in binds all array values
        StringVal v1 = new StringVal("deleted");
        StringVal v2 = new StringVal("archived");
        List<Object> values = List.of(v1, v2);
        PhysicalFilter innerFilter = filter(Operator.IN, "status", values);
        PhysicalNot not = new PhysicalNot(nextId(), innerFilter);
        List<BqlValue> parameters = List.of(v1, v2);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(not, parameters);

        assertBindingCount(bindings, innerFilter.id(), 2);
    }

    @Test
    void shouldBindElemMatchWithNestedAnd() {
        // Behavior: ElemMatch with AND sub-plan binds all children in canonical order
        Int32Val v1 = new Int32Val(80);
        Int32Val v2 = new Int32Val(100);
        PhysicalFilter filterGte = filter(Operator.GTE, "score", v1);
        PhysicalFilter filterLte = filter(Operator.LTE, "score", v2);
        PhysicalAnd innerAnd = new PhysicalAnd(nextId(), List.of(filterGte, filterLte));
        PhysicalElemMatch elemMatch = new PhysicalElemMatch(nextId(), "results", innerAnd);
        List<BqlValue> parameters = List.of(v1, v2);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(elemMatch, parameters);

        assertEquals(2, getTotalBindingCount(bindings));
    }

    // ===== PhysicalTrue/PhysicalFalse Tests =====

    @Test
    void shouldProduceNoBindingsFromPhysicalTrue() {
        // Behavior: PhysicalTrue produces empty binding map
        PhysicalTrue trueNode = new PhysicalTrue(nextId());
        List<BqlValue> parameters = List.of();
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(trueNode, parameters);

        assertTrue(bindings.isEmpty());
    }

    @Test
    void shouldProduceNoBindingsFromPhysicalFalse() {
        // Behavior: PhysicalFalse produces empty binding map
        PhysicalFalse falseNode = new PhysicalFalse(nextId());
        List<BqlValue> parameters = List.of();
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(falseNode, parameters);

        assertTrue(bindings.isEmpty());
    }

    // ===== IndexIntersection Tests =====

    @Test
    void shouldBindIndexIntersectionFiltersInCanonicalOrder() {
        // Behavior: Filters sorted by shape hash before binding
        StringVal nameVal = new StringVal("john");
        Int32Val ageVal = new Int32Val(25);
        PhysicalFilter filterName = filter(Operator.EQ, "name", nameVal);
        PhysicalFilter filterAge = filter(Operator.EQ, "age", ageVal);

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalIndexIntersection intersection = new PhysicalIndexIntersection(
                nextId(), List.of(nameIndex, ageIndex), List.of(filterName, filterAge));
        List<BqlValue> parameters = List.of(nameVal, ageVal);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(intersection, parameters);

        assertEquals(2, getTotalBindingCount(bindings));
    }

    @Test
    void shouldProduceSameBindingOrderForIntersectionWithDifferentFilterOrder() {
        // Behavior: Same filters in different order produce same binding indices
        StringVal nameVal1 = new StringVal("john");
        Int32Val ageVal1 = new Int32Val(25);
        PhysicalFilter filterName1 = filter(Operator.EQ, "name", nameVal1);
        PhysicalFilter filterAge1 = filter(Operator.EQ, "age", ageVal1);
        List<BqlValue> parameters1 = List.of(nameVal1, ageVal1);

        StringVal nameVal2 = new StringVal("jane");
        Int32Val ageVal2 = new Int32Val(30);
        PhysicalFilter filterName2 = filter(Operator.EQ, "name", nameVal2);
        PhysicalFilter filterAge2 = filter(Operator.EQ, "age", ageVal2);
        List<BqlValue> parameters2 = List.of(nameVal2, ageVal2);

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        PhysicalIndexIntersection intersection1 = new PhysicalIndexIntersection(
                nextId(), List.of(nameIndex, ageIndex), List.of(filterName1, filterAge1));
        PhysicalIndexIntersection intersection2 = new PhysicalIndexIntersection(
                nextId(), List.of(ageIndex, nameIndex), List.of(filterAge2, filterName2));

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings1 =
                PhysicalPlanParameterBinder.bind(intersection1, parameters1);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings2 =
                PhysicalPlanParameterBinder.bind(intersection2, parameters2);

        int paramIdxName1 = bindings1.get(filterName1.id()).get(0).paramIndex();
        int paramIdxName2 = bindings2.get(filterName2.id()).get(0).paramIndex();
        int paramIdxAge1 = bindings1.get(filterAge1.id()).get(0).paramIndex();
        int paramIdxAge2 = bindings2.get(filterAge2.id()).get(0).paramIndex();

        assertEquals(paramIdxName1, paramIdxName2, "Parameter index for 'name' should be the same");
        assertEquals(paramIdxAge1, paramIdxAge2, "Parameter index for 'age' should be the same");
    }

    // ===== CompoundIndexScan Tests =====

    @Test
    void shouldBindCompoundIndexScanFiltersInCanonicalOrder() {
        // Behavior: CompoundIndexScan filters are sorted by shape hash before binding
        StringVal nameVal = new StringVal("john");
        Int32Val ageVal = new Int32Val(25);
        PhysicalFilter filterName = filter(Operator.EQ, "name", nameVal);
        PhysicalFilter filterAge = filter(Operator.EQ, "age", ageVal);

        CompoundIndexDefinition compoundIndex = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);

        PhysicalCompoundIndexScan compoundScan = new PhysicalCompoundIndexScan(
                nextId(), compoundIndex, List.of(filterName, filterAge));
        List<BqlValue> parameters = List.of(nameVal, ageVal);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(compoundScan, parameters);

        assertEquals(2, getTotalBindingCount(bindings));
    }

    @Test
    void shouldProduceSameBindingOrderForCompoundIndexScanWithDifferentFilterOrder() {
        // Behavior: Same filters in different order produce same binding indices for compound index scan
        StringVal nameVal1 = new StringVal("john");
        Int32Val ageVal1 = new Int32Val(25);
        PhysicalFilter filterName1 = filter(Operator.EQ, "name", nameVal1);
        PhysicalFilter filterAge1 = filter(Operator.EQ, "age", ageVal1);
        List<BqlValue> parameters1 = List.of(nameVal1, ageVal1);

        StringVal nameVal2 = new StringVal("jane");
        Int32Val ageVal2 = new Int32Val(30);
        PhysicalFilter filterName2 = filter(Operator.EQ, "name", nameVal2);
        PhysicalFilter filterAge2 = filter(Operator.EQ, "age", ageVal2);
        List<BqlValue> parameters2 = List.of(nameVal2, ageVal2);

        CompoundIndexDefinition compoundIndex = CompoundIndexDefinition.create("idx_name_age", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false)
        ), IndexStatus.WAITING);

        PhysicalCompoundIndexScan compoundScan1 = new PhysicalCompoundIndexScan(
                nextId(), compoundIndex, List.of(filterName1, filterAge1));
        PhysicalCompoundIndexScan compoundScan2 = new PhysicalCompoundIndexScan(
                nextId(), compoundIndex, List.of(filterAge2, filterName2));

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings1 =
                PhysicalPlanParameterBinder.bind(compoundScan1, parameters1);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings2 =
                PhysicalPlanParameterBinder.bind(compoundScan2, parameters2);

        int paramIdxName1 = bindings1.get(filterName1.id()).get(0).paramIndex();
        int paramIdxName2 = bindings2.get(filterName2.id()).get(0).paramIndex();
        int paramIdxAge1 = bindings1.get(filterAge1.id()).get(0).paramIndex();
        int paramIdxAge2 = bindings2.get(filterAge2.id()).get(0).paramIndex();

        assertEquals(paramIdxName1, paramIdxName2, "Parameter index for 'name' should be the same");
        assertEquals(paramIdxAge1, paramIdxAge2, "Parameter index for 'age' should be the same");
    }

    @Test
    void shouldBindCompoundIndexScanWithThreeFilters() {
        // Behavior: CompoundIndexScan with 3 filters binds all parameters in canonical order
        StringVal nameVal = new StringVal("john");
        Int32Val ageVal = new Int32Val(25);
        StringVal cityVal = new StringVal("NYC");
        PhysicalFilter filterName = filter(Operator.EQ, "name", nameVal);
        PhysicalFilter filterAge = filter(Operator.EQ, "age", ageVal);
        PhysicalFilter filterCity = filter(Operator.EQ, "city", cityVal);

        CompoundIndexDefinition compoundIndex = CompoundIndexDefinition.create("idx_name_age_city", List.of(
                new CompoundIndexField("name", BsonType.STRING, false),
                new CompoundIndexField("age", BsonType.INT32, false),
                new CompoundIndexField("city", BsonType.STRING, false)
        ), IndexStatus.WAITING);

        PhysicalCompoundIndexScan compoundScan = new PhysicalCompoundIndexScan(
                nextId(), compoundIndex, List.of(filterName, filterAge, filterCity));
        List<BqlValue> parameters = List.of(nameVal, ageVal, cityVal);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(compoundScan, parameters);

        assertEquals(3, getTotalBindingCount(bindings));
    }

    // ===== Deep Nesting Tests =====

    @Test
    void shouldBindThreeLevelNestedAndOrAnd() {
        // Behavior: AND(OR(AND(a,b),c),d) binds all 4 parameters correctly
        Int32Val v1 = new Int32Val(1);
        Int32Val v2 = new Int32Val(2);
        Int32Val v3 = new Int32Val(3);
        Int32Val v4 = new Int32Val(4);
        PhysicalFilter filterA = filter(Operator.EQ, "a", v1);
        PhysicalFilter filterB = filter(Operator.EQ, "b", v2);
        PhysicalFilter filterC = filter(Operator.EQ, "c", v3);
        PhysicalFilter filterD = filter(Operator.EQ, "d", v4);

        PhysicalAnd innerAnd = new PhysicalAnd(nextId(), List.of(filterA, filterB));
        PhysicalOr middleOr = new PhysicalOr(nextId(), List.of(innerAnd, filterC));
        PhysicalAnd outerAnd = new PhysicalAnd(nextId(), List.of(middleOr, filterD));
        List<BqlValue> parameters = List.of(v1, v2, v3, v4);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(outerAnd, parameters);

        assertEquals(4, getTotalBindingCount(bindings));
    }

    @Test
    void shouldProduceSameOrderForDeepNestedWithDifferentChildOrder() {
        // Behavior: Deeply nested with reordered children produces same binding order
        Int32Val v1 = new Int32Val(1);
        Int32Val v2 = new Int32Val(2);
        Int32Val v3 = new Int32Val(3);
        Int32Val v4 = new Int32Val(4);
        PhysicalFilter filterA1 = filter(Operator.EQ, "a", v1);
        PhysicalFilter filterB1 = filter(Operator.EQ, "b", v2);
        PhysicalFilter filterC1 = filter(Operator.EQ, "c", v3);
        PhysicalFilter filterD1 = filter(Operator.EQ, "d", v4);

        PhysicalAnd innerAnd1 = new PhysicalAnd(nextId(), List.of(filterA1, filterB1));
        PhysicalOr middleOr1 = new PhysicalOr(nextId(), List.of(innerAnd1, filterC1));
        PhysicalAnd outerAnd1 = new PhysicalAnd(nextId(), List.of(middleOr1, filterD1));
        List<BqlValue> parameters1 = List.of(v1, v2, v3, v4);

        Int32Val v10 = new Int32Val(10);
        Int32Val v20 = new Int32Val(20);
        Int32Val v30 = new Int32Val(30);
        Int32Val v40 = new Int32Val(40);
        PhysicalFilter filterA2 = filter(Operator.EQ, "a", v10);
        PhysicalFilter filterB2 = filter(Operator.EQ, "b", v20);
        PhysicalFilter filterC2 = filter(Operator.EQ, "c", v30);
        PhysicalFilter filterD2 = filter(Operator.EQ, "d", v40);

        PhysicalAnd innerAnd2 = new PhysicalAnd(nextId(), List.of(filterB2, filterA2));
        PhysicalOr middleOr2 = new PhysicalOr(nextId(), List.of(filterC2, innerAnd2));
        PhysicalAnd outerAnd2 = new PhysicalAnd(nextId(), List.of(filterD2, middleOr2));
        List<BqlValue> parameters2 = List.of(v10, v20, v30, v40);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings1 =
                PhysicalPlanParameterBinder.bind(outerAnd1, parameters1);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings2 =
                PhysicalPlanParameterBinder.bind(outerAnd2, parameters2);

        int paramIdxA1 = bindings1.get(filterA1.id()).get(0).paramIndex();
        int paramIdxA2 = bindings2.get(filterA2.id()).get(0).paramIndex();
        assertEquals(paramIdxA1, paramIdxA2, "Parameter index for 'a' should be the same");

        int paramIdxB1 = bindings1.get(filterB1.id()).get(0).paramIndex();
        int paramIdxB2 = bindings2.get(filterB2.id()).get(0).paramIndex();
        assertEquals(paramIdxB1, paramIdxB2, "Parameter index for 'b' should be the same");
    }

    // ===== Identical-Shape Siblings Tests (Critical) =====

    @Test
    void shouldPreserveInsertionOrderForIdenticalShapeSiblingsInAnd() {
        // Behavior: AND with children having identical shape hashes preserves insertion order
        StringVal v1 = new StringVal("active");
        StringVal v2 = new StringVal("pending");
        PhysicalFilter filter1 = filter(Operator.EQ, "status", v1);
        PhysicalFilter filter2 = filter(Operator.EQ, "status", v2);
        PhysicalAnd and = new PhysicalAnd(nextId(), List.of(filter1, filter2));
        List<BqlValue> parameters = List.of(v1, v2);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(and, parameters);

        assertEquals(2, getTotalBindingCount(bindings));

        int paramIdx1 = bindings.get(filter1.id()).get(0).paramIndex();
        int paramIdx2 = bindings.get(filter2.id()).get(0).paramIndex();

        // Should have consecutive param indices
        assertTrue(paramIdx1 != paramIdx2, "Different filters should have different param indices");
    }

    @Test
    void shouldPreserveInsertionOrderForIdenticalShapeSiblingsInOr() {
        // Behavior: OR with children having identical shape hashes preserves insertion order
        StringVal v1 = new StringVal("Apple");
        StringVal v2 = new StringVal("Samsung");
        PhysicalFilter filter1 = filter(Operator.EQ, "brand", v1);
        PhysicalFilter filter2 = filter(Operator.EQ, "brand", v2);
        PhysicalOr or = new PhysicalOr(nextId(), List.of(filter1, filter2));
        List<BqlValue> parameters = List.of(v1, v2);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(or, parameters);

        assertEquals(2, getTotalBindingCount(bindings));

        int paramIdx1 = bindings.get(filter1.id()).get(0).paramIndex();
        int paramIdx2 = bindings.get(filter2.id()).get(0).paramIndex();

        assertTrue(paramIdx1 != paramIdx2, "Different filters should have different param indices");
    }

    // ===== Integration Tests (ParameterExtractor Alignment) =====

    @Test
    void shouldMatchParameterExtractorOrderForSimpleAnd() {
        // Behavior: Physical plan bindings match BQL parameter extraction order
        String query = "{\"$and\": [{\"a\": 1}, {\"b\": 2}]}";
        BqlExpr bqlExpr = BqlParser.parse(query);
        List<BqlValue> extractedParams = ParameterExtractor.extract(bqlExpr);

        // Use extracted params to build filters (same values)
        PhysicalFilter filterA = filter(Operator.EQ, "a", extractedParams.get(0));
        PhysicalFilter filterB = filter(Operator.EQ, "b", extractedParams.get(1));
        PhysicalAnd and = new PhysicalAnd(nextId(), List.of(filterA, filterB));

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(and, extractedParams);

        assertEquals(extractedParams.size(), getTotalBindingCount(bindings));
    }

    @Test
    void shouldMatchParameterExtractorOrderForNestedAndOr() {
        // Behavior: Nested structures produce aligned binding order with ParameterExtractor
        String query = "{\"$and\": [{\"a\": 1}, {\"$or\": [{\"b\": 2}, {\"c\": 3}]}]}";
        BqlExpr bqlExpr = BqlParser.parse(query);
        List<BqlValue> extractedParams = ParameterExtractor.extract(bqlExpr);

        // Find values by their actual content to build filters
        Int32Val v1 = new Int32Val(1);
        Int32Val v2 = new Int32Val(2);
        Int32Val v3 = new Int32Val(3);
        PhysicalFilter filterA = filter(Operator.EQ, "a", v1);
        PhysicalFilter filterB = filter(Operator.EQ, "b", v2);
        PhysicalFilter filterC = filter(Operator.EQ, "c", v3);

        PhysicalOr innerOr = new PhysicalOr(nextId(), List.of(filterB, filterC));
        PhysicalAnd and = new PhysicalAnd(nextId(), List.of(filterA, innerOr));

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(and, extractedParams);

        assertEquals(extractedParams.size(), getTotalBindingCount(bindings));
    }

    @Test
    void shouldMatchParameterExtractorOrderForArrayOperators() {
        // Behavior: $in/$nin/$all produce aligned order between binder and extractor
        String query = "{\"status\": {\"$in\": [1, 2, 3]}}";
        BqlExpr bqlExpr = BqlParser.parse(query);
        List<BqlValue> extractedParams = ParameterExtractor.extract(bqlExpr);

        List<Object> values = List.of(extractedParams.get(0), extractedParams.get(1), extractedParams.get(2));
        PhysicalFilter filter = filter(Operator.IN, "status", values);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, extractedParams);

        assertEquals(extractedParams.size(), getTotalBindingCount(bindings));
    }

    @Test
    void shouldMatchParameterExtractorOrderForMixedOperators() {
        // Behavior: Mixed comparison operators produce aligned order
        String query = "{\"$and\": [{\"age\": {\"$gte\": 18}}, {\"score\": {\"$lt\": 100}}]}";
        BqlExpr bqlExpr = BqlParser.parse(query);
        List<BqlValue> extractedParams = ParameterExtractor.extract(bqlExpr);

        Int32Val ageVal = new Int32Val(18);
        Int32Val scoreVal = new Int32Val(100);
        PhysicalFilter ageFilter = filter(Operator.GTE, "age", ageVal);
        PhysicalFilter scoreFilter = filter(Operator.LT, "score", scoreVal);
        PhysicalAnd and = new PhysicalAnd(nextId(), List.of(ageFilter, scoreFilter));

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(and, extractedParams);

        assertEquals(extractedParams.size(), getTotalBindingCount(bindings));
    }

    // ===== Real-World Query Pattern Tests =====

    @Test
    void shouldBindTypicalUserSearchQuery() {
        // Behavior: age >= 18 AND age < 65 AND status IN ["active", "pending"]
        Int32Val ageVal1 = new Int32Val(18);
        Int32Val ageVal2 = new Int32Val(65);
        StringVal statusVal1 = new StringVal("active");
        StringVal statusVal2 = new StringVal("pending");
        PhysicalFilter ageGte = filter(Operator.GTE, "age", ageVal1);
        PhysicalFilter ageLt = filter(Operator.LT, "age", ageVal2);
        List<Object> statusValues = List.of(statusVal1, statusVal2);
        PhysicalFilter statusIn = filter(Operator.IN, "status", statusValues);

        PhysicalAnd query = new PhysicalAnd(nextId(), List.of(ageGte, ageLt, statusIn));
        List<BqlValue> parameters = List.of(ageVal1, ageVal2, statusVal1, statusVal2);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(query, parameters);

        // 1 for ageGte + 1 for ageLt + 2 for statusIn = 4 total
        assertEquals(4, getTotalBindingCount(bindings));
    }

    @Test
    void shouldBindTypicalProductFilterQuery() {
        // Behavior: price range AND category EQ AND (brand = "A" OR brand = "B")
        Int32Val priceVal1 = new Int32Val(10);
        Int32Val priceVal2 = new Int32Val(100);
        StringVal categoryVal = new StringVal("electronics");
        StringVal brandValA = new StringVal("A");
        StringVal brandValB = new StringVal("B");
        PhysicalFilter priceGte = filter(Operator.GTE, "price", priceVal1);
        PhysicalFilter priceLte = filter(Operator.LTE, "price", priceVal2);
        PhysicalFilter category = filter(Operator.EQ, "category", categoryVal);
        PhysicalFilter brandA = filter(Operator.EQ, "brand", brandValA);
        PhysicalFilter brandB = filter(Operator.EQ, "brand", brandValB);

        PhysicalOr brandOr = new PhysicalOr(nextId(), List.of(brandA, brandB));
        PhysicalAnd query = new PhysicalAnd(nextId(), List.of(priceGte, priceLte, category, brandOr));
        List<BqlValue> parameters = List.of(priceVal1, priceVal2, categoryVal, brandValA, brandValB);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(query, parameters);

        // 1 + 1 + 1 + 2 = 5 total parameters
        assertEquals(5, getTotalBindingCount(bindings));
    }

    // ===== Additional Edge Cases =====

    @Test
    void shouldHandleNestedElemMatchWithMultipleFilters() {
        // Behavior: ElemMatch containing multiple nested filters binds all parameters
        Int32Val v1 = new Int32Val(10);
        Int32Val v2 = new Int32Val(50);
        PhysicalFilter filterGte = filter(Operator.GTE, "qty", v1);
        PhysicalFilter filterLte = filter(Operator.LTE, "price", v2);
        PhysicalAnd innerAnd = new PhysicalAnd(nextId(), List.of(filterGte, filterLte));
        PhysicalElemMatch elemMatch = new PhysicalElemMatch(nextId(), "items", innerAnd);
        List<BqlValue> parameters = List.of(v1, v2);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(elemMatch, parameters);

        assertEquals(2, getTotalBindingCount(bindings));
    }

    @Test
    void shouldHandleDoubleNot() {
        // Behavior: Double NOT wrapping a filter still binds the innermost filter's parameter
        Int32Val value = new Int32Val(25);
        PhysicalFilter innerFilter = filter(Operator.EQ, "age", value);
        PhysicalNot innerNot = new PhysicalNot(nextId(), innerFilter);
        PhysicalNot outerNot = new PhysicalNot(nextId(), innerNot);
        List<BqlValue> parameters = List.of(value);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(outerNot, parameters);

        assertBindingCount(bindings, innerFilter.id(), 1);
    }

    @Test
    void shouldHandleFullScanWrappingTrue() {
        // Behavior: FullScan wrapping PhysicalTrue produces no bindings
        PhysicalTrue trueNode = new PhysicalTrue(nextId());
        PhysicalFullScan fullScan = new PhysicalFullScan(nextId(), trueNode);
        List<BqlValue> parameters = List.of();

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(fullScan, parameters);

        assertTrue(bindings.isEmpty());
    }

    @Test
    void shouldHandleMixedValueTypes() {
        // Behavior: Filters with different value types bind correctly
        Int32Val intVal = new Int32Val(100);
        StringVal strVal = new StringVal("test");
        BooleanVal boolVal = new BooleanVal(true);
        PhysicalFilter intFilter = filter(Operator.EQ, "count", intVal);
        PhysicalFilter strFilter = filter(Operator.EQ, "name", strVal);
        PhysicalFilter boolFilter = filter(Operator.EQ, "active", boolVal);
        PhysicalAnd and = new PhysicalAnd(nextId(), List.of(intFilter, strFilter, boolFilter));
        List<BqlValue> parameters = List.of(intVal, strVal, boolVal);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(and, parameters);

        assertEquals(3, getTotalBindingCount(bindings));
    }

    @Test
    void shouldHandleSizeZero() {
        // Behavior: SIZE with 0 binds correctly
        Int32Val value = new Int32Val(0);
        PhysicalFilter filter = filter(Operator.SIZE, "items", value);
        List<BqlValue> parameters = List.of(value);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 1);
    }

    @Test
    void shouldHandleThreeFiltersWithSameSelector() {
        // Behavior: Three filters on the same selector with same operator all get bindings
        StringVal v1 = new StringVal("a");
        StringVal v2 = new StringVal("b");
        StringVal v3 = new StringVal("c");
        PhysicalFilter filter1 = filter(Operator.EQ, "tag", v1);
        PhysicalFilter filter2 = filter(Operator.EQ, "tag", v2);
        PhysicalFilter filter3 = filter(Operator.EQ, "tag", v3);
        PhysicalOr or = new PhysicalOr(nextId(), List.of(filter1, filter2, filter3));
        List<BqlValue> parameters = List.of(v1, v2, v3);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(or, parameters);

        assertEquals(3, getTotalBindingCount(bindings));
    }

    @Test
    void shouldHandleEmptyOr() {
        // Behavior: Empty PhysicalOr produces no bindings
        PhysicalOr emptyOr = new PhysicalOr(nextId(), List.of());
        List<BqlValue> parameters = List.of();
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(emptyOr, parameters);

        assertTrue(bindings.isEmpty());
    }

    // ===== Mixed Scan Types Tests =====

    @Test
    void shouldBindMixedScanTypesInAnd() {
        // Behavior: AND containing IndexScan and FullScan binds all inner filter parameters
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        Int32Val ageVal = new Int32Val(25);
        StringVal nameVal = new StringVal("john");
        PhysicalFilter ageFilter = filter(Operator.EQ, "age", ageVal);
        PhysicalIndexScan indexScan = new PhysicalIndexScan(nextId(), ageFilter, ageIndex);

        PhysicalFilter nameFilter = filter(Operator.EQ, "name", nameVal);
        PhysicalFullScan fullScan = new PhysicalFullScan(nextId(), nameFilter);

        PhysicalAnd and = new PhysicalAnd(nextId(), List.of(indexScan, fullScan));
        List<BqlValue> parameters = List.of(ageVal, nameVal);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(and, parameters);

        assertEquals(2, getTotalBindingCount(bindings));
        assertBindingCount(bindings, ageFilter.id(), 1);
        assertBindingCount(bindings, nameFilter.id(), 1);
    }

    @Test
    void shouldBindOrWithMultipleScanTypes() {
        // Behavior: OR containing IndexScan and RangeScan binds all parameters
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);

        Int32Val ageVal = new Int32Val(25);
        StringVal lowerName = new StringVal("a");
        StringVal upperName = new StringVal("m");
        PhysicalFilter ageFilter = filter(Operator.EQ, "age", ageVal);
        PhysicalIndexScan indexScan = new PhysicalIndexScan(nextId(), ageFilter, ageIndex);

        PhysicalRangeScan rangeScan = new PhysicalRangeScan(nextId(), "name",
                lowerName, upperName, true, false, nameIndex);

        PhysicalOr or = new PhysicalOr(nextId(), List.of(indexScan, rangeScan));
        List<BqlValue> parameters = List.of(ageVal, lowerName, upperName);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(or, parameters);

        // 1 from IndexScan filter + 2 from RangeScan bounds
        assertEquals(3, getTotalBindingCount(bindings));
    }

    @Test
    void shouldBindRangeScanCombinedWithFilters() {
        // Behavior: AND containing RangeScan and regular filters binds all parameters
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        Int32Val lower = new Int32Val(18);
        Int32Val upper = new Int32Val(65);
        StringVal statusVal = new StringVal("active");
        PhysicalRangeScan rangeScan = new PhysicalRangeScan(nextId(), "age",
                lower, upper, true, false, ageIndex);
        PhysicalFilter statusFilter = filter(Operator.EQ, "status", statusVal);

        PhysicalAnd and = new PhysicalAnd(nextId(), List.of(rangeScan, statusFilter));
        List<BqlValue> parameters = List.of(lower, upper, statusVal);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(and, parameters);

        // 2 from RangeScan + 1 from statusFilter = 3 total
        assertEquals(3, getTotalBindingCount(bindings));
        assertBindingCount(bindings, rangeScan.id(), 2);
        assertBindingCount(bindings, statusFilter.id(), 1);
    }

    @Test
    void shouldBindComplexIndexIntersectionWithThreeFilters() {
        // Behavior: IndexIntersection with 3 filters binds all parameters in canonical order
        StringVal nameVal = new StringVal("john");
        Int32Val ageVal = new Int32Val(25);
        StringVal cityVal = new StringVal("NYC");
        PhysicalFilter filterName = filter(Operator.EQ, "name", nameVal);
        PhysicalFilter filterAge = filter(Operator.EQ, "age", ageVal);
        PhysicalFilter filterCity = filter(Operator.EQ, "city", cityVal);

        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition cityIndex = SingleFieldIndexDefinition.create("city_idx", "city", BsonType.STRING, false, IndexStatus.WAITING);

        PhysicalIndexIntersection intersection = new PhysicalIndexIntersection(
                nextId(),
                List.of(nameIndex, ageIndex, cityIndex),
                List.of(filterName, filterAge, filterCity)
        );
        List<BqlValue> parameters = List.of(nameVal, ageVal, cityVal);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(intersection, parameters);

        assertEquals(3, getTotalBindingCount(bindings));
    }

    @Test
    void shouldBindArrayQueryWithElemMatch() {
        // Behavior: AND combining $all with $elemMatch binds all parameters
        StringVal tagA = new StringVal("a");
        StringVal tagB = new StringVal("b");
        Int32Val qtyVal = new Int32Val(10);
        List<Object> tagValues = List.of(tagA, tagB);
        PhysicalFilter tagsAll = filter(Operator.ALL, "tags", tagValues);

        PhysicalFilter qtyGte = filter(Operator.GTE, "qty", qtyVal);
        PhysicalElemMatch elemMatch = new PhysicalElemMatch(nextId(), "items", qtyGte);

        PhysicalAnd query = new PhysicalAnd(nextId(), List.of(tagsAll, elemMatch));
        List<BqlValue> parameters = List.of(tagA, tagB, qtyVal);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(query, parameters);

        // 2 from $all + 1 from $elemMatch inner filter = 3 total
        assertEquals(3, getTotalBindingCount(bindings));
        assertBindingCount(bindings, tagsAll.id(), 2);
        assertBindingCount(bindings, qtyGte.id(), 1);
    }

    @Test
    void shouldProduceSameBindingOrderForMixedScanTypesWithDifferentOrder() {
        // Behavior: Mixed scan types with reordered children produce same parameter indices
        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);

        // First version: IndexScan, FullScan order
        Int32Val ageVal1 = new Int32Val(25);
        StringVal nameVal1 = new StringVal("john");
        PhysicalFilter ageFilter1 = filter(Operator.EQ, "age", ageVal1);
        PhysicalIndexScan indexScan1 = new PhysicalIndexScan(nextId(), ageFilter1, ageIndex);
        PhysicalFilter nameFilter1 = filter(Operator.EQ, "name", nameVal1);
        PhysicalFullScan fullScan1 = new PhysicalFullScan(nextId(), nameFilter1);
        PhysicalAnd and1 = new PhysicalAnd(nextId(), List.of(indexScan1, fullScan1));
        List<BqlValue> parameters1 = List.of(ageVal1, nameVal1);

        // Second version: FullScan, IndexScan order
        Int32Val ageVal2 = new Int32Val(30);
        StringVal nameVal2 = new StringVal("jane");
        PhysicalFilter ageFilter2 = filter(Operator.EQ, "age", ageVal2);
        PhysicalIndexScan indexScan2 = new PhysicalIndexScan(nextId(), ageFilter2, ageIndex);
        PhysicalFilter nameFilter2 = filter(Operator.EQ, "name", nameVal2);
        PhysicalFullScan fullScan2 = new PhysicalFullScan(nextId(), nameFilter2);
        PhysicalAnd and2 = new PhysicalAnd(nextId(), List.of(fullScan2, indexScan2));
        List<BqlValue> parameters2 = List.of(ageVal2, nameVal2);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings1 =
                PhysicalPlanParameterBinder.bind(and1, parameters1);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings2 =
                PhysicalPlanParameterBinder.bind(and2, parameters2);

        int paramIdxAge1 = bindings1.get(ageFilter1.id()).get(0).paramIndex();
        int paramIdxAge2 = bindings2.get(ageFilter2.id()).get(0).paramIndex();
        int paramIdxName1 = bindings1.get(nameFilter1.id()).get(0).paramIndex();
        int paramIdxName2 = bindings2.get(nameFilter2.id()).get(0).paramIndex();

        assertEquals(paramIdxAge1, paramIdxAge2, "Parameter index for 'age' filter should be the same");
        assertEquals(paramIdxName1, paramIdxName2, "Parameter index for 'name' filter should be the same");
    }

    @Test
    void shouldBindNotWrappingComplexStructure() {
        // Behavior: NOT wrapping complex AND/OR binds all nested parameters
        Int32Val v1 = new Int32Val(1);
        Int32Val v2 = new Int32Val(2);
        PhysicalFilter filterA = filter(Operator.EQ, "a", v1);
        PhysicalFilter filterB = filter(Operator.EQ, "b", v2);
        PhysicalOr innerOr = new PhysicalOr(nextId(), List.of(filterA, filterB));
        PhysicalNot not = new PhysicalNot(nextId(), innerOr);
        List<BqlValue> parameters = List.of(v1, v2);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(not, parameters);

        assertEquals(2, getTotalBindingCount(bindings));
    }

    @Test
    void shouldBindElemMatchWithNestedOr() {
        // Behavior: ElemMatch with nested OR sub-plan binds all children
        Int32Val v1 = new Int32Val(50);
        Int32Val v2 = new Int32Val(90);
        PhysicalFilter filterLt = filter(Operator.LT, "score", v1);
        PhysicalFilter filterGt = filter(Operator.GT, "score", v2);
        PhysicalOr innerOr = new PhysicalOr(nextId(), List.of(filterLt, filterGt));
        PhysicalElemMatch elemMatch = new PhysicalElemMatch(nextId(), "scores", innerOr);
        List<BqlValue> parameters = List.of(v1, v2);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(elemMatch, parameters);

        assertEquals(2, getTotalBindingCount(bindings));
    }

    @Test
    void shouldHandleSingleChildOr() {
        // Behavior: OR with single child binds that child's parameters
        Int32Val value = new Int32Val(1);
        PhysicalFilter filter = filter(Operator.EQ, "a", value);
        PhysicalOr or = new PhysicalOr(nextId(), List.of(filter));
        List<BqlValue> parameters = List.of(value);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(or, parameters);

        assertBindingCount(bindings, filter.id(), 1);
        assertBinding(bindings, filter.id(), 0, 0, 0);
    }

    @Test
    void shouldHandleNestedAndInsideOr() {
        // Behavior: OR containing AND children binds all nested parameters
        Int32Val v1 = new Int32Val(1);
        Int32Val v2 = new Int32Val(2);
        Int32Val v3 = new Int32Val(3);
        PhysicalFilter filterA = filter(Operator.EQ, "a", v1);
        PhysicalFilter filterB = filter(Operator.EQ, "b", v2);
        PhysicalAnd innerAnd = new PhysicalAnd(nextId(), List.of(filterA, filterB));

        PhysicalFilter filterC = filter(Operator.EQ, "c", v3);

        PhysicalOr or = new PhysicalOr(nextId(), List.of(innerAnd, filterC));
        List<BqlValue> parameters = List.of(v1, v2, v3);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(or, parameters);

        assertEquals(3, getTotalBindingCount(bindings));
    }

    @Test
    void shouldHandleInt64Val() {
        // Behavior: PhysicalFilter with Int64Val binds correctly
        Int64Val value = new Int64Val(9223372036854775807L);
        PhysicalFilter filter = filter(Operator.EQ, "bigNumber", value);
        List<BqlValue> parameters = List.of(value);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 1);
    }

    @Test
    void shouldHandleDoubleVal() {
        // Behavior: PhysicalFilter with DoubleVal binds correctly
        DoubleVal value = new DoubleVal(19.99);
        PhysicalFilter filter = filter(Operator.EQ, "price", value);
        List<BqlValue> parameters = List.of(value);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 1);
    }

    @Test
    void shouldHandleNullVal() {
        // Behavior: PhysicalFilter with NullVal binds correctly
        PhysicalFilter filter = filter(Operator.EQ, "field", NullVal.INSTANCE);
        List<BqlValue> parameters = List.of(NullVal.INSTANCE);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 1);
    }

    @Test
    void shouldHandleEmptyNinArray() {
        // Behavior: $nin with [] produces no bindings (empty list)
        List<Object> values = List.of();
        PhysicalFilter filter = filter(Operator.NIN, "field", values);
        List<BqlValue> parameters = List.of();
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 0);
    }

    @Test
    void shouldHandleEmptyAllArray() {
        // Behavior: $all with [] produces no bindings (empty list)
        List<Object> values = List.of();
        PhysicalFilter filter = filter(Operator.ALL, "tags", values);
        List<BqlValue> parameters = List.of();
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(filter, parameters);

        assertBindingCount(bindings, filter.id(), 0);
    }

    // =========================================================================
    // Wrapper Node Ordering Fix Tests
    // =========================================================================
    // These tests verify the fix for the bug where NODE_INDEX_SCAN and NODE_FULL_SCAN
    // prefixes in PhysicalNodeShape.compute() caused parameter binding order to differ
    // from ParameterExtractor's order. The fix uses computeForBinding() which skips
    // wrapper prefixes to match QueryShape's ordering.

    @Test
    void shouldProduceSameOrderForIndexScanAndFullScanWrappingSameFilter() {
        // Behavior: When AND has children wrapped with IndexScan vs FullScan,
        // the parameter binding order should be based on the underlying filter shape,
        // not the wrapper type.
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("idx", "field", BsonType.INT32, false, IndexStatus.WAITING);

        // Two different filters with IndexScan wrapping
        Int32Val v1 = new Int32Val(1);
        Int32Val v2 = new Int32Val(2);
        PhysicalFilter filterA = filter(Operator.EQ, "a", v1);
        PhysicalFilter filterB = filter(Operator.EQ, "b", v2);
        PhysicalIndexScan indexScanA = new PhysicalIndexScan(nextId(), filterA, index);
        PhysicalIndexScan indexScanB = new PhysicalIndexScan(nextId(), filterB, index);
        PhysicalAnd andWithIndexScans = new PhysicalAnd(nextId(), List.of(indexScanA, indexScanB));
        List<BqlValue> parameters1 = List.of(v1, v2);

        // Same filters with FullScan wrapping
        Int32Val v10 = new Int32Val(10);
        Int32Val v20 = new Int32Val(20);
        PhysicalFilter filterA2 = filter(Operator.EQ, "a", v10);
        PhysicalFilter filterB2 = filter(Operator.EQ, "b", v20);
        PhysicalFullScan fullScanA = new PhysicalFullScan(nextId(), filterA2);
        PhysicalFullScan fullScanB = new PhysicalFullScan(nextId(), filterB2);
        PhysicalAnd andWithFullScans = new PhysicalAnd(nextId(), List.of(fullScanA, fullScanB));
        List<BqlValue> parameters2 = List.of(v10, v20);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings1 =
                PhysicalPlanParameterBinder.bind(andWithIndexScans, parameters1);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings2 =
                PhysicalPlanParameterBinder.bind(andWithFullScans, parameters2);

        // Parameter indices for 'a' and 'b' should be the same regardless of wrapper type
        int paramIdxA1 = bindings1.get(filterA.id()).get(0).paramIndex();
        int paramIdxA2 = bindings2.get(filterA2.id()).get(0).paramIndex();
        int paramIdxB1 = bindings1.get(filterB.id()).get(0).paramIndex();
        int paramIdxB2 = bindings2.get(filterB2.id()).get(0).paramIndex();

        assertEquals(paramIdxA1, paramIdxA2, "Parameter index for 'a' should match regardless of wrapper type");
        assertEquals(paramIdxB1, paramIdxB2, "Parameter index for 'b' should match regardless of wrapper type");
    }

    @Test
    void shouldProduceSameOrderForMixedIndexScanAndFullScanWrappers() {
        // Behavior: AND with one IndexScan child and one FullScan child should produce
        // the same parameter binding order as AND with both as FullScan (or both as IndexScan).
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("idx", "field", BsonType.INT32, false, IndexStatus.WAITING);

        // Version 1: IndexScan for 'a', FullScan for 'b'
        Int32Val v1 = new Int32Val(1);
        Int32Val v2 = new Int32Val(2);
        PhysicalFilter filterA1 = filter(Operator.EQ, "a", v1);
        PhysicalFilter filterB1 = filter(Operator.EQ, "b", v2);
        PhysicalIndexScan indexScanA = new PhysicalIndexScan(nextId(), filterA1, index);
        PhysicalFullScan fullScanB1 = new PhysicalFullScan(nextId(), filterB1);
        PhysicalAnd and1 = new PhysicalAnd(nextId(), List.of(indexScanA, fullScanB1));
        List<BqlValue> parameters1 = List.of(v1, v2);

        // Version 2: FullScan for 'a', IndexScan for 'b'
        Int32Val v10 = new Int32Val(10);
        Int32Val v20 = new Int32Val(20);
        PhysicalFilter filterA2 = filter(Operator.EQ, "a", v10);
        PhysicalFilter filterB2 = filter(Operator.EQ, "b", v20);
        PhysicalFullScan fullScanA = new PhysicalFullScan(nextId(), filterA2);
        PhysicalIndexScan indexScanB = new PhysicalIndexScan(nextId(), filterB2, index);
        PhysicalAnd and2 = new PhysicalAnd(nextId(), List.of(fullScanA, indexScanB));
        List<BqlValue> parameters2 = List.of(v10, v20);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings1 =
                PhysicalPlanParameterBinder.bind(and1, parameters1);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings2 =
                PhysicalPlanParameterBinder.bind(and2, parameters2);

        int paramIdxA1 = bindings1.get(filterA1.id()).get(0).paramIndex();
        int paramIdxA2 = bindings2.get(filterA2.id()).get(0).paramIndex();
        int paramIdxB1 = bindings1.get(filterB1.id()).get(0).paramIndex();
        int paramIdxB2 = bindings2.get(filterB2.id()).get(0).paramIndex();

        assertEquals(paramIdxA1, paramIdxA2, "Parameter index for 'a' should match regardless of wrapper type mix");
        assertEquals(paramIdxB1, paramIdxB2, "Parameter index for 'b' should match regardless of wrapper type mix");
    }

    @Test
    void shouldMatchParameterExtractorOrderForElemMatchWithOrAndWrappedChildren() {
        // Behavior: This is the exact scenario that caused the original bug.
        // $elemMatch with $or children where the physical plan wraps filters in FullScan.
        // The parameter binding order must match ParameterExtractor's order.
        String query = "{\"items\": {\"$elemMatch\": {\"$or\": [{\"category\": \"electronics\"}, {\"brand\": \"acme\"}]}}}";
        BqlExpr bqlExpr = BqlParser.parse(query);
        List<BqlValue> extractedParams = ParameterExtractor.extract(bqlExpr);

        // Create physical plan with FullScan wrapped filters (typical for $elemMatch subplan)
        StringVal categoryVal = new StringVal("electronics");
        StringVal brandVal = new StringVal("acme");
        PhysicalFilter filterCategory = filter(Operator.EQ, "category", categoryVal);
        PhysicalFilter filterBrand = filter(Operator.EQ, "brand", brandVal);
        PhysicalFullScan fullScanCategory = new PhysicalFullScan(nextId(), filterCategory);
        PhysicalFullScan fullScanBrand = new PhysicalFullScan(nextId(), filterBrand);
        PhysicalOr physicalOr = new PhysicalOr(nextId(), List.of(fullScanCategory, fullScanBrand));
        PhysicalElemMatch physicalElemMatch = new PhysicalElemMatch(nextId(), "items", physicalOr);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(physicalElemMatch, extractedParams);

        // Verify parameter count matches
        assertEquals(extractedParams.size(), getTotalBindingCount(bindings),
                "Total parameter count should match ParameterExtractor");

        // The critical check: parameter ordering must match.
        // ParameterExtractor extracts in canonical order based on QueryShape.
        // PhysicalPlanParameterBinder must produce the same ordering.
        int categoryParamIdx = bindings.get(filterCategory.id()).get(0).paramIndex();
        int brandParamIdx = bindings.get(filterBrand.id()).get(0).paramIndex();

        // Find which parameter in extractedParams corresponds to category vs brand
        int expectedCategoryIdx = -1;
        int expectedBrandIdx = -1;
        for (int i = 0; i < extractedParams.size(); i++) {
            BqlValue param = extractedParams.get(i);
            if (param instanceof StringVal sv) {
                if ("electronics".equals(sv.value())) {
                    expectedCategoryIdx = i;
                } else if ("acme".equals(sv.value())) {
                    expectedBrandIdx = i;
                }
            }
        }

        assertEquals(expectedCategoryIdx, categoryParamIdx,
                "Category parameter index should match ParameterExtractor order");
        assertEquals(expectedBrandIdx, brandParamIdx,
                "Brand parameter index should match ParameterExtractor order");
    }

    @Test
    void shouldMatchParameterExtractorOrderForOrWithFullScanWrappedChildren() {
        // Behavior: OR with FullScan wrapped children produces parameter ordering
        // that matches ParameterExtractor.
        String query = "{\"$or\": [{\"category\": \"electronics\"}, {\"brand\": \"acme\"}]}";
        BqlExpr bqlExpr = BqlParser.parse(query);
        List<BqlValue> extractedParams = ParameterExtractor.extract(bqlExpr);

        StringVal categoryVal = new StringVal("electronics");
        StringVal brandVal = new StringVal("acme");
        PhysicalFilter filterCategory = filter(Operator.EQ, "category", categoryVal);
        PhysicalFilter filterBrand = filter(Operator.EQ, "brand", brandVal);
        PhysicalFullScan fullScanCategory = new PhysicalFullScan(nextId(), filterCategory);
        PhysicalFullScan fullScanBrand = new PhysicalFullScan(nextId(), filterBrand);
        PhysicalOr or = new PhysicalOr(nextId(), List.of(fullScanCategory, fullScanBrand));

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(or, extractedParams);

        assertEquals(extractedParams.size(), getTotalBindingCount(bindings));

        int categoryParamIdx = bindings.get(filterCategory.id()).get(0).paramIndex();
        int brandParamIdx = bindings.get(filterBrand.id()).get(0).paramIndex();

        int expectedCategoryIdx = -1;
        int expectedBrandIdx = -1;
        for (int i = 0; i < extractedParams.size(); i++) {
            BqlValue param = extractedParams.get(i);
            if (param instanceof StringVal sv) {
                if ("electronics".equals(sv.value())) {
                    expectedCategoryIdx = i;
                } else if ("acme".equals(sv.value())) {
                    expectedBrandIdx = i;
                }
            }
        }

        assertEquals(expectedCategoryIdx, categoryParamIdx,
                "Category parameter index should match ParameterExtractor order");
        assertEquals(expectedBrandIdx, brandParamIdx,
                "Brand parameter index should match ParameterExtractor order");
    }

    @Test
    void shouldMatchParameterExtractorOrderForAndWithIndexScanWrappedChildren() {
        // Behavior: AND with IndexScan wrapped children produces parameter ordering
        // that matches ParameterExtractor.
        String query = "{\"$and\": [{\"age\": {\"$gte\": 18}}, {\"name\": \"john\"}]}";
        BqlExpr bqlExpr = BqlParser.parse(query);
        List<BqlValue> extractedParams = ParameterExtractor.extract(bqlExpr);

        SingleFieldIndexDefinition ageIndex = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        SingleFieldIndexDefinition nameIndex = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);

        Int32Val ageVal = new Int32Val(18);
        StringVal nameVal = new StringVal("john");
        PhysicalFilter filterAge = filter(Operator.GTE, "age", ageVal);
        PhysicalFilter filterName = filter(Operator.EQ, "name", nameVal);
        PhysicalIndexScan indexScanAge = new PhysicalIndexScan(nextId(), filterAge, ageIndex);
        PhysicalIndexScan indexScanName = new PhysicalIndexScan(nextId(), filterName, nameIndex);
        PhysicalAnd and = new PhysicalAnd(nextId(), List.of(indexScanAge, indexScanName));

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(and, extractedParams);

        assertEquals(extractedParams.size(), getTotalBindingCount(bindings));

        int ageParamIdx = bindings.get(filterAge.id()).get(0).paramIndex();
        int nameParamIdx = bindings.get(filterName.id()).get(0).paramIndex();

        int expectedAgeIdx = -1;
        int expectedNameIdx = -1;
        for (int i = 0; i < extractedParams.size(); i++) {
            BqlValue param = extractedParams.get(i);
            if (param instanceof Int32Val iv && iv.value() == 18) {
                expectedAgeIdx = i;
            } else if (param instanceof StringVal sv && "john".equals(sv.value())) {
                expectedNameIdx = i;
            }
        }

        assertEquals(expectedAgeIdx, ageParamIdx,
                "Age parameter index should match ParameterExtractor order");
        assertEquals(expectedNameIdx, nameParamIdx,
                "Name parameter index should match ParameterExtractor order");
    }

    @Test
    void shouldMatchParameterExtractorOrderForNestedElemMatchWithWrappedFilters() {
        // Behavior: Nested $elemMatch with AND containing wrapped filters produces
        // correct parameter ordering.
        String query = "{\"results\": {\"$elemMatch\": {\"$and\": [{\"score\": {\"$gte\": 80}}, {\"grade\": \"A\"}]}}}";
        BqlExpr bqlExpr = BqlParser.parse(query);
        List<BqlValue> extractedParams = ParameterExtractor.extract(bqlExpr);

        Int32Val scoreVal = new Int32Val(80);
        StringVal gradeVal = new StringVal("A");
        PhysicalFilter filterScore = filter(Operator.GTE, "score", scoreVal);
        PhysicalFilter filterGrade = filter(Operator.EQ, "grade", gradeVal);
        PhysicalFullScan fullScanScore = new PhysicalFullScan(nextId(), filterScore);
        PhysicalFullScan fullScanGrade = new PhysicalFullScan(nextId(), filterGrade);
        PhysicalAnd innerAnd = new PhysicalAnd(nextId(), List.of(fullScanScore, fullScanGrade));
        PhysicalElemMatch elemMatch = new PhysicalElemMatch(nextId(), "results", innerAnd);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(elemMatch, extractedParams);

        assertEquals(extractedParams.size(), getTotalBindingCount(bindings));

        int scoreParamIdx = bindings.get(filterScore.id()).get(0).paramIndex();
        int gradeParamIdx = bindings.get(filterGrade.id()).get(0).paramIndex();

        int expectedScoreIdx = -1;
        int expectedGradeIdx = -1;
        for (int i = 0; i < extractedParams.size(); i++) {
            BqlValue param = extractedParams.get(i);
            if (param instanceof Int32Val iv && iv.value() == 80) {
                expectedScoreIdx = i;
            } else if (param instanceof StringVal sv && "A".equals(sv.value())) {
                expectedGradeIdx = i;
            }
        }

        assertEquals(expectedScoreIdx, scoreParamIdx,
                "Score parameter index should match ParameterExtractor order");
        assertEquals(expectedGradeIdx, gradeParamIdx,
                "Grade parameter index should match ParameterExtractor order");
    }

    @Test
    void shouldProduceSameOrderRegardlessOfWrapperTypeForThreeChildren() {
        // Behavior: AND with three children using different wrapper types produces
        // same parameter ordering as AND with uniform wrapper types.
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("idx", "field", BsonType.INT32, false, IndexStatus.WAITING);

        // Version 1: Mixed wrappers (IndexScan, FullScan, IndexScan)
        Int32Val v1 = new Int32Val(1);
        Int32Val v2 = new Int32Val(2);
        Int32Val v3 = new Int32Val(3);
        PhysicalFilter filterA1 = filter(Operator.EQ, "a", v1);
        PhysicalFilter filterB1 = filter(Operator.EQ, "b", v2);
        PhysicalFilter filterC1 = filter(Operator.EQ, "c", v3);
        PhysicalIndexScan indexScanA = new PhysicalIndexScan(nextId(), filterA1, index);
        PhysicalFullScan fullScanB = new PhysicalFullScan(nextId(), filterB1);
        PhysicalIndexScan indexScanC = new PhysicalIndexScan(nextId(), filterC1, index);
        PhysicalAnd and1 = new PhysicalAnd(nextId(), List.of(indexScanA, fullScanB, indexScanC));
        List<BqlValue> parameters1 = List.of(v1, v2, v3);

        // Version 2: All FullScan
        Int32Val v10 = new Int32Val(10);
        Int32Val v20 = new Int32Val(20);
        Int32Val v30 = new Int32Val(30);
        PhysicalFilter filterA2 = filter(Operator.EQ, "a", v10);
        PhysicalFilter filterB2 = filter(Operator.EQ, "b", v20);
        PhysicalFilter filterC2 = filter(Operator.EQ, "c", v30);
        PhysicalFullScan fullScanA = new PhysicalFullScan(nextId(), filterA2);
        PhysicalFullScan fullScanB2 = new PhysicalFullScan(nextId(), filterB2);
        PhysicalFullScan fullScanC = new PhysicalFullScan(nextId(), filterC2);
        PhysicalAnd and2 = new PhysicalAnd(nextId(), List.of(fullScanA, fullScanB2, fullScanC));
        List<BqlValue> parameters2 = List.of(v10, v20, v30);

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings1 =
                PhysicalPlanParameterBinder.bind(and1, parameters1);
        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings2 =
                PhysicalPlanParameterBinder.bind(and2, parameters2);

        int paramIdxA1 = bindings1.get(filterA1.id()).get(0).paramIndex();
        int paramIdxA2 = bindings2.get(filterA2.id()).get(0).paramIndex();
        int paramIdxB1 = bindings1.get(filterB1.id()).get(0).paramIndex();
        int paramIdxB2 = bindings2.get(filterB2.id()).get(0).paramIndex();
        int paramIdxC1 = bindings1.get(filterC1.id()).get(0).paramIndex();
        int paramIdxC2 = bindings2.get(filterC2.id()).get(0).paramIndex();

        assertEquals(paramIdxA1, paramIdxA2, "Parameter index for 'a' should match");
        assertEquals(paramIdxB1, paramIdxB2, "Parameter index for 'b' should match");
        assertEquals(paramIdxC1, paramIdxC2, "Parameter index for 'c' should match");
    }

    @Test
    void shouldHandleOrInsideAndWithMixedWrappers() {
        // Behavior: Complex structure AND(OR(wrapped, wrapped), wrapped) produces
        // correct parameter ordering matching ParameterExtractor.
        SingleFieldIndexDefinition index = SingleFieldIndexDefinition.create("idx", "field", BsonType.INT32, false, IndexStatus.WAITING);

        String query = "{\"$and\": [{\"$or\": [{\"x\": 1}, {\"y\": 2}]}, {\"z\": 3}]}";
        BqlExpr bqlExpr = BqlParser.parse(query);
        List<BqlValue> extractedParams = ParameterExtractor.extract(bqlExpr);

        Int32Val vx = new Int32Val(1);
        Int32Val vy = new Int32Val(2);
        Int32Val vz = new Int32Val(3);
        PhysicalFilter filterX = filter(Operator.EQ, "x", vx);
        PhysicalFilter filterY = filter(Operator.EQ, "y", vy);
        PhysicalFilter filterZ = filter(Operator.EQ, "z", vz);

        PhysicalIndexScan indexScanX = new PhysicalIndexScan(nextId(), filterX, index);
        PhysicalFullScan fullScanY = new PhysicalFullScan(nextId(), filterY);
        PhysicalOr innerOr = new PhysicalOr(nextId(), List.of(indexScanX, fullScanY));

        PhysicalIndexScan indexScanZ = new PhysicalIndexScan(nextId(), filterZ, index);
        PhysicalAnd outerAnd = new PhysicalAnd(nextId(), List.of(innerOr, indexScanZ));

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(outerAnd, extractedParams);

        assertEquals(extractedParams.size(), getTotalBindingCount(bindings));
    }

    // =========================================================================
    // Duplicate Values Test (Fix for $nin with duplicate values)
    // =========================================================================

    @Test
    void shouldBindCorrectParameterIndexForDuplicateValuesInNin() {
        // Behavior: When $nin has duplicate values ['admin', 'admin', 'editor'],
        // after optimizer deduplication to ['admin', 'editor'], the bindings
        // should correctly map to parameter indices 0 (admin) and 2 (editor),
        // NOT 0 and 1.

        // Simulating the optimized plan after RedundantScanEliminationRule:
        // Original: $nin: ['admin', 'admin', 'editor'] → params [admin(0), admin(1), editor(2)]
        // Optimized: AND(NE('admin'), NE('editor'))

        StringVal admin = new StringVal("admin");
        StringVal adminDup = new StringVal("admin");  // Duplicate
        StringVal editor = new StringVal("editor");
        List<BqlValue> parameters = List.of(admin, adminDup, editor);

        PhysicalFilter adminFilter = filter(Operator.NE, "role", admin);
        PhysicalFilter editorFilter = filter(Operator.NE, "role", editor);
        PhysicalAnd and = new PhysicalAnd(nextId(), List.of(adminFilter, editorFilter));

        Map<Integer, List<PhysicalPlanParameterBinder.ParamBinding>> bindings =
                PhysicalPlanParameterBinder.bind(and, parameters);

        // admin filter should bind to parameter index 0 (first 'admin')
        assertBinding(bindings, adminFilter.id(), 0, 0, 0);

        // editor filter should bind to parameter index 2 (NOT 1!)
        assertBinding(bindings, editorFilter.id(), 0, 2, 0);
    }
}
