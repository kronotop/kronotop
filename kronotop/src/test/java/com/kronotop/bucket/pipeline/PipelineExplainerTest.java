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

import com.kronotop.bucket.PipelineExplainer;
import com.kronotop.bucket.bql.ast.Int32Val;
import com.kronotop.bucket.bql.ast.StringVal;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import org.bson.BsonType;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PipelineExplainerTest {

    private static @NonNull CompoundIndexScanNode getCompoundIndexScanNode() {
        CompoundIndexDefinition indexDef = new CompoundIndexDefinition(1, "age_score_idx",
                List.of(new CompoundIndexField("age", BsonType.INT32, false),
                        new CompoundIndexField("score", BsonType.INT32, false)),
                IndexStatus.READY, null);
        List<CompoundIndexScanNode.CompoundIndexScanFilter> filters = List.of(
                new CompoundIndexScanNode.CompoundIndexScanFilter("age", Operator.EQ, new Operand.Literal(new Int32Val(30)), BsonType.INT32),
                new CompoundIndexScanNode.CompoundIndexScanFilter("score", Operator.GT, new Operand.Literal(new Int32Val(80)), BsonType.INT32)
        );
        return new CompoundIndexScanNode(1, indexDef, filters);
    }

    @Test
    void shouldReturnPlannerVersionForNullNode() {
        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(null);
        assertEquals(1, result.size());
        assertTrue(containsKey(result, "planner_version"));
    }

    @Test
    void shouldExplainIndexScanNode() {
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "age", Operator.EQ, new Operand.Literal(new Int32Val(25)));
        IndexScanNode node = new IndexScanNode(1, indexDef, predicate);

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(node);

        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertTrue(containsKey(result, "nodeType"));
        assertTrue(containsKey(result, "scanType"));
        assertTrue(containsKey(result, "index"));
    }

    @Test
    void shouldExplainFullScanNode() {
        AlwaysTruePredicate predicate = new AlwaysTruePredicate();
        FullScanNode node = new TestFullScanNode(1, predicate);

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(node);

        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertTrue(containsKey(result, "nodeType"));
        assertTrue(containsKey(result, "scanType"));
    }

    @Test
    void shouldExplainRangeScanNode() {
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("age",
                new Operand.Literal(new Int32Val(18)),
                new Operand.Literal(new Int32Val(65)),
                true, false);
        RangeScanNode node = new TestRangeScanNode(1, indexDef, predicate);

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(node);

        assertNotNull(result);
        assertFalse(result.isEmpty());
        assertTrue(containsKey(result, "nodeType"));
        assertTrue(containsKey(result, "scanType"));
        assertTrue(containsKey(result, "lowerBound"));
        assertTrue(containsKey(result, "upperBound"));
    }

    @Test
    void shouldExplainUnionNode() {
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        IndexScanPredicate predicate1 = new IndexScanPredicate(1, "name", Operator.EQ, new Operand.Literal(new StringVal("Alice")));
        IndexScanPredicate predicate2 = new IndexScanPredicate(2, "name", Operator.EQ, new Operand.Literal(new StringVal("Bob")));
        IndexScanNode child1 = new IndexScanNode(1, indexDef, predicate1);
        IndexScanNode child2 = new IndexScanNode(2, indexDef, predicate2);
        UnionNode unionNode = new UnionNode(3, List.of(child1, child2));

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(unionNode);

        assertNotNull(result);
        assertTrue(containsKey(result, "operation"));
        assertTrue(containsKey(result, "children"));
    }

    @Test
    void shouldExplainTransformWithResidualPredicateNode() {
        ResidualPredicate residualPredicate = new ResidualPredicate(1, "status", Operator.EQ, new Operand.Literal(new StringVal("active")));
        TransformWithResidualPredicateNode transformNode = new TransformWithResidualPredicateNode(1, residualPredicate);

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(transformNode);

        assertNotNull(result);
        assertTrue(containsKey(result, "operation"));
        assertTrue(containsKey(result, "predicate"));
    }

    @Test
    void shouldExplainChainedNodes() {
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        IndexScanPredicate scanPredicate = new IndexScanPredicate(1, "age", Operator.GT, new Operand.Literal(new Int32Val(18)));
        IndexScanNode scanNode = new IndexScanNode(1, indexDef, scanPredicate);

        ResidualPredicate residualPredicate = new ResidualPredicate(2, "status", Operator.EQ, new Operand.Literal(new StringVal("active")));
        TransformWithResidualPredicateNode transformNode = new TransformWithResidualPredicateNode(2, residualPredicate);

        scanNode.connectNext(transformNode);

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(scanNode);

        assertNotNull(result);
        assertTrue(containsKey(result, "next"));
    }

    @Test
    void shouldReturnArrayRedisMessage() {
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "age", Operator.EQ, new Operand.Literal(new Int32Val(25)));
        IndexScanNode node = new IndexScanNode(1, indexDef, predicate);

        ArrayRedisMessage arrayMessage = PipelineExplainer.explainAsArrayMessage(node);

        assertNotNull(arrayMessage);
        assertFalse(arrayMessage.children().isEmpty());
    }

    @Test
    void shouldReturnMapRedisMessage() {
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "age", Operator.EQ, new Operand.Literal(new Int32Val(25)));
        IndexScanNode node = new IndexScanNode(1, indexDef, predicate);

        MapRedisMessage mapMessage = PipelineExplainer.explainAsMapMessage(node);

        assertNotNull(mapMessage);
        assertFalse(mapMessage.children().isEmpty());
    }

    @Test
    void shouldExplainResidualAndNode() {
        ResidualPredicate pred1 = new ResidualPredicate(1, "age", Operator.GT, new Operand.Literal(new Int32Val(18)));
        ResidualPredicate pred2 = new ResidualPredicate(2, "status", Operator.EQ, new Operand.Literal(new StringVal("active")));
        ResidualAndNode andNode = new ResidualAndNode(List.of(pred1, pred2));
        TransformWithResidualPredicateNode transformNode = new TransformWithResidualPredicateNode(3, andNode);

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(transformNode);

        assertNotNull(result);
        assertTrue(containsKey(result, "predicate"));
    }

    @Test
    void shouldExplainResidualOrNode() {
        ResidualPredicate pred1 = new ResidualPredicate(1, "status", Operator.EQ, new Operand.Literal(new StringVal("active")));
        ResidualPredicate pred2 = new ResidualPredicate(2, "status", Operator.EQ, new Operand.Literal(new StringVal("pending")));
        ResidualOrNode orNode = new ResidualOrNode(List.of(pred1, pred2));
        TransformWithResidualPredicateNode transformNode = new TransformWithResidualPredicateNode(3, orNode);

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(transformNode);

        assertNotNull(result);
        assertTrue(containsKey(result, "predicate"));
    }

    @Test
    void shouldExplainAlwaysTruePredicate() {
        AlwaysTruePredicate alwaysTrue = new AlwaysTruePredicate();
        TransformWithResidualPredicateNode transformNode = new TransformWithResidualPredicateNode(1, alwaysTrue);

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(transformNode);

        assertNotNull(result);
        assertTrue(containsKey(result, "predicate"));
    }

    @Test
    void shouldExplainOrderedConcatNode() {
        // Behavior: An ordered concat node with two EQ children should produce operation
        // ORDERED_CONCAT and a children array containing both child plans.
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        IndexScanPredicate predicate1 = new IndexScanPredicate(1, "age", Operator.EQ, new Operand.Literal(new Int32Val(10)));
        IndexScanPredicate predicate2 = new IndexScanPredicate(2, "age", Operator.EQ, new Operand.Literal(new Int32Val(30)));
        IndexScanNode child1 = new IndexScanNode(1, indexDef, predicate1);
        IndexScanNode child2 = new IndexScanNode(2, indexDef, predicate2);
        OrderedConcatNode orderedConcatNode = new OrderedConcatNode(3, List.of(child1, child2));

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(orderedConcatNode);

        assertNotNull(result);
        assertTrue(containsKey(result, "nodeType"));
        assertEquals("OrderedConcat", getStringValue(result, "nodeType"));
        assertTrue(containsKey(result, "operation"));
        assertEquals("ORDERED_CONCAT", getStringValue(result, "operation"));
        assertTrue(containsKey(result, "children"));

        RedisMessage childrenValue = getValueForKey(result, "children");
        assertInstanceOf(ArrayRedisMessage.class, childrenValue);
        ArrayRedisMessage childrenArray = (ArrayRedisMessage) childrenValue;
        assertEquals(2, childrenArray.children().size());
    }

    @Test
    void shouldExplainOrderedConcatNodeWithChainedNext() {
        // Behavior: An ordered concat node with a chained TransformWithResidualPredicateNode
        // should include both the children array and the next node in the output.
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        IndexScanPredicate predicate1 = new IndexScanPredicate(1, "age", Operator.EQ, new Operand.Literal(new Int32Val(10)));
        IndexScanNode child1 = new IndexScanNode(1, indexDef, predicate1);
        OrderedConcatNode orderedConcatNode = new OrderedConcatNode(2, List.of(child1));

        ResidualPredicate residualPredicate = new ResidualPredicate(3, "status", Operator.EQ, new Operand.Literal(new StringVal("active")));
        TransformWithResidualPredicateNode transformNode = new TransformWithResidualPredicateNode(3, residualPredicate);
        orderedConcatNode.connectNext(transformNode);

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(orderedConcatNode);

        assertNotNull(result);
        assertTrue(containsKey(result, "operation"));
        assertTrue(containsKey(result, "children"));
        assertTrue(containsKey(result, "next"));
    }

    @Test
    void shouldExplainCompoundIndexScanNode() {
        // Behavior: A compound index scan with two EQ filters should produce scanType, index, and filters keys.
        CompoundIndexDefinition indexDef = new CompoundIndexDefinition(1, "age_city_idx",
                List.of(new CompoundIndexField("age", BsonType.INT32, false),
                        new CompoundIndexField("city", BsonType.STRING, false)),
                IndexStatus.READY, null);
        List<CompoundIndexScanNode.CompoundIndexScanFilter> filters = List.of(
                new CompoundIndexScanNode.CompoundIndexScanFilter("age", Operator.EQ, new Operand.Literal(new Int32Val(25)), BsonType.INT32),
                new CompoundIndexScanNode.CompoundIndexScanFilter("city", Operator.EQ, new Operand.Literal(new StringVal("NYC")), BsonType.STRING)
        );
        CompoundIndexScanNode node = new CompoundIndexScanNode(1, indexDef, filters);

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(node);

        assertNotNull(result);
        assertTrue(containsKey(result, "scanType"));
        assertTrue(containsKey(result, "index"));
        assertTrue(containsKey(result, "filters"));
    }

    @Test
    void shouldExplainCompoundIndexScanNodeWithRangeFilter() {
        // Behavior: A compound index scan with EQ + GT filters should list both filters in the filters array.
        CompoundIndexScanNode node = getCompoundIndexScanNode();

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(node);

        assertNotNull(result);
        assertTrue(containsKey(result, "scanType"));
        assertTrue(containsKey(result, "index"));
        assertTrue(containsKey(result, "filters"));

        RedisMessage filtersValue = getValueForKey(result, "filters");
        assertInstanceOf(ArrayRedisMessage.class, filtersValue);
        ArrayRedisMessage filtersArray = (ArrayRedisMessage) filtersValue;
        assertEquals(2, filtersArray.children().size());

        // Verify first filter: age EQ 30
        assertInstanceOf(MapRedisMessage.class, filtersArray.children().get(0));
        Map<RedisMessage, RedisMessage> firstFilter = ((MapRedisMessage) filtersArray.children().get(0)).children();
        assertEquals("age", getStringValue(firstFilter, "selector"));
        assertEquals("EQ", getStringValue(firstFilter, "operator"));

        // Verify second filter: score GT 80
        assertInstanceOf(MapRedisMessage.class, filtersArray.children().get(1));
        Map<RedisMessage, RedisMessage> secondFilter = ((MapRedisMessage) filtersArray.children().get(1)).children();
        assertEquals("score", getStringValue(secondFilter, "selector"));
        assertEquals("GT", getStringValue(secondFilter, "operator"));
    }

    private boolean containsKey(Map<RedisMessage, RedisMessage> map, String keyName) {
        for (RedisMessage key : map.keySet()) {
            if (key instanceof FullBulkStringRedisMessage msg && msg.content().toString(StandardCharsets.UTF_8).equals(keyName)) {
                return true;
            }
        }
        return false;
    }

    private RedisMessage getValueForKey(Map<RedisMessage, RedisMessage> map, String keyName) {
        for (Map.Entry<RedisMessage, RedisMessage> entry : map.entrySet()) {
            if (entry.getKey() instanceof FullBulkStringRedisMessage msg && msg.content().toString(StandardCharsets.UTF_8).equals(keyName)) {
                return entry.getValue();
            }
        }
        return null;
    }

    private String getStringValue(Map<RedisMessage, RedisMessage> map, String keyName) {
        RedisMessage value = getValueForKey(map, keyName);
        if (value instanceof FullBulkStringRedisMessage msg) {
            return msg.content().toString(StandardCharsets.UTF_8);
        }
        return null;
    }

    // Test subclass to access the protected FullScanNode constructor
    private static class TestFullScanNode extends FullScanNode {
        TestFullScanNode(int id, ResidualPredicateNode predicate) {
            super(id, PrimaryIndex.createDefinition(), predicate);
        }
    }

    // Test subclass to access the protected RangeScanNode constructor
    private static class TestRangeScanNode extends RangeScanNode {
        TestRangeScanNode(int id, SingleFieldIndexDefinition index, RangeScanPredicate predicate) {
            super(id, index, predicate);
        }
    }
}
