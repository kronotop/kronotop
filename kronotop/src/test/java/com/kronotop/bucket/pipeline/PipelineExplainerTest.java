/*
 * Copyright (c) 2023-2025 Burak Sezer
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
import com.kronotop.bucket.index.IndexDefinition;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.server.resp3.*;
import org.bson.BsonType;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PipelineExplainerTest {

    // Test subclass to access protected FullScanNode constructor
    private static class TestFullScanNode extends FullScanNode {
        TestFullScanNode(int id, ResidualPredicateNode predicate) {
            super(id, predicate);
        }
    }

    // Test subclass to access protected RangeScanNode constructor
    private static class TestRangeScanNode extends RangeScanNode {
        TestRangeScanNode(int id, IndexDefinition index, RangeScanPredicate predicate) {
            super(id, index, predicate);
        }
    }

    @Test
    void shouldReturnPlannerVersionForNullNode() {
        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(null);
        assertEquals(1, result.size());
        assertTrue(containsKey(result, "planner_version"));
    }

    @Test
    void shouldExplainIndexScanNode() {
        IndexDefinition indexDef = IndexDefinition.create("age_idx", "age", BsonType.INT32);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "age", Operator.EQ, 25);
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
        IndexDefinition indexDef = IndexDefinition.create("age_idx", "age", BsonType.INT32);
        RangeScanPredicate predicate = new RangeScanPredicate("age", 18, 65, true, false);
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
        IndexDefinition indexDef = IndexDefinition.create("name_idx", "name", BsonType.STRING);
        IndexScanPredicate predicate1 = new IndexScanPredicate(1, "name", Operator.EQ, "Alice");
        IndexScanPredicate predicate2 = new IndexScanPredicate(2, "name", Operator.EQ, "Bob");
        IndexScanNode child1 = new IndexScanNode(1, indexDef, predicate1);
        IndexScanNode child2 = new IndexScanNode(2, indexDef, predicate2);
        UnionNode unionNode = new UnionNode(3, List.of(child1, child2));

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(unionNode);

        assertNotNull(result);
        assertTrue(containsKey(result, "operation"));
        assertTrue(containsKey(result, "children"));
    }

    @Test
    void shouldExplainIntersectionNode() {
        IndexDefinition indexDef1 = IndexDefinition.create("age_idx", "age", BsonType.INT32);
        IndexDefinition indexDef2 = IndexDefinition.create("city_idx", "city", BsonType.STRING);
        IndexScanPredicate predicate1 = new IndexScanPredicate(1, "age", Operator.GT, 18);
        IndexScanPredicate predicate2 = new IndexScanPredicate(2, "city", Operator.EQ, "NYC");
        IndexScanNode child1 = new IndexScanNode(1, indexDef1, predicate1);
        IndexScanNode child2 = new IndexScanNode(2, indexDef2, predicate2);
        IntersectionNode intersectionNode = new IntersectionNode(3, List.of(child1, child2));

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(intersectionNode);

        assertNotNull(result);
        assertTrue(containsKey(result, "operation"));
        assertTrue(containsKey(result, "children"));
    }

    @Test
    void shouldExplainTransformWithResidualPredicateNode() {
        ResidualPredicate residualPredicate = new ResidualPredicate(1, "status", Operator.EQ, "active");
        TransformWithResidualPredicateNode transformNode = new TransformWithResidualPredicateNode(1, residualPredicate);

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(transformNode);

        assertNotNull(result);
        assertTrue(containsKey(result, "operation"));
        assertTrue(containsKey(result, "predicate"));
    }

    @Test
    void shouldExplainChainedNodes() {
        IndexDefinition indexDef = IndexDefinition.create("age_idx", "age", BsonType.INT32);
        IndexScanPredicate scanPredicate = new IndexScanPredicate(1, "age", Operator.GT, 18);
        IndexScanNode scanNode = new IndexScanNode(1, indexDef, scanPredicate);

        ResidualPredicate residualPredicate = new ResidualPredicate(2, "status", Operator.EQ, "active");
        TransformWithResidualPredicateNode transformNode = new TransformWithResidualPredicateNode(2, residualPredicate);

        scanNode.connectNext(transformNode);

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(scanNode);

        assertNotNull(result);
        assertTrue(containsKey(result, "next"));
    }

    @Test
    void shouldReturnArrayRedisMessage() {
        IndexDefinition indexDef = IndexDefinition.create("age_idx", "age", BsonType.INT32);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "age", Operator.EQ, 25);
        IndexScanNode node = new IndexScanNode(1, indexDef, predicate);

        ArrayRedisMessage arrayMessage = PipelineExplainer.explainAsArrayMessage(node);

        assertNotNull(arrayMessage);
        assertFalse(arrayMessage.children().isEmpty());
    }

    @Test
    void shouldReturnMapRedisMessage() {
        IndexDefinition indexDef = IndexDefinition.create("age_idx", "age", BsonType.INT32);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "age", Operator.EQ, 25);
        IndexScanNode node = new IndexScanNode(1, indexDef, predicate);

        MapRedisMessage mapMessage = PipelineExplainer.explainAsMapMessage(node);

        assertNotNull(mapMessage);
        assertFalse(mapMessage.children().isEmpty());
    }

    @Test
    void shouldExplainResidualAndNode() {
        ResidualPredicate pred1 = new ResidualPredicate(1, "age", Operator.GT, 18);
        ResidualPredicate pred2 = new ResidualPredicate(2, "status", Operator.EQ, "active");
        ResidualAndNode andNode = new ResidualAndNode(List.of(pred1, pred2));
        TransformWithResidualPredicateNode transformNode = new TransformWithResidualPredicateNode(3, andNode);

        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(transformNode);

        assertNotNull(result);
        assertTrue(containsKey(result, "predicate"));
    }

    @Test
    void shouldExplainResidualOrNode() {
        ResidualPredicate pred1 = new ResidualPredicate(1, "status", Operator.EQ, "active");
        ResidualPredicate pred2 = new ResidualPredicate(2, "status", Operator.EQ, "pending");
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

    private boolean containsKey(Map<RedisMessage, RedisMessage> map, String keyName) {
        for (RedisMessage key : map.keySet()) {
            if (key instanceof SimpleStringRedisMessage msg && msg.content().equals(keyName)) {
                return true;
            }
        }
        return false;
    }
}
