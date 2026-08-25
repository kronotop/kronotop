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

import com.kronotop.bucket.Collation;
import com.kronotop.bucket.PipelineExplainer;
import com.kronotop.bucket.bql.ast.DoubleVal;
import com.kronotop.bucket.bql.ast.Int32Val;
import com.kronotop.bucket.bql.ast.StringVal;
import com.kronotop.bucket.index.*;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.server.RESPUtil;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.ArrayRedisMessage;
import com.kronotop.server.resp3.BooleanRedisMessage;
import com.kronotop.server.resp3.DoubleRedisMessage;
import com.kronotop.server.resp3.FullBulkStringRedisMessage;
import com.kronotop.server.resp3.IntegerRedisMessage;
import com.kronotop.server.resp3.MapRedisMessage;
import com.kronotop.server.resp3.NullRedisMessage;
import com.kronotop.server.resp3.RedisMessage;
import org.bson.BsonType;
import org.jspecify.annotations.NonNull;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class PipelineExplainerTest {

    private static @NonNull CompoundIndexScanNode getCompoundIndexScanNode() {
        CompoundIndexDefinition indexDef = new CompoundIndexDefinition(1, "age_score_idx",
                List.of(new CompoundIndexField("age", BsonType.INT32, false),
                        new CompoundIndexField("score", BsonType.INT32, false)),
                IndexStatus.READY, null, false);
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
    void shouldReturnArrayRedisMessageWhenRESP2() {
        // Behavior: RESP2 has no map type, so the rendered plan reaches the client as a flat array.
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        IndexScanPredicate predicate = new IndexScanPredicate(1, "age", Operator.EQ, new Operand.Literal(new Int32Val(25)));
        IndexScanNode node = new IndexScanNode(1, indexDef, predicate);

        List<RedisMessage> flattened = explainAsRESP2(node);

        assertFalse(flattened.isEmpty());
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
                IndexStatus.READY, null, false);
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

    @Test
    void shouldFormatNullOperandAsBulkStringNullWhenRESP2() {
        // Behavior: an unbounded range has a null operand, which a RESP2 client must receive as $-1.
        List<RedisMessage> flattened = explainAsRESP2(unboundedRangeScanNode());

        assertEquals(FullBulkStringRedisMessage.NULL_INSTANCE, valueAfterKey(flattened, "lowerBound"));
    }

    @Test
    void shouldFormatNullOperandAsNullTypeWhenRESP3() {
        // Behavior: the same null operand is rendered as the RESP3 null type on a RESP3 session.
        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(unboundedRangeScanNode());

        assertEquals(NullRedisMessage.INSTANCE, getValueForKey(result, "lowerBound"));
    }

    @Test
    void shouldRenderRangeBoundFlagsAsIntegersWhenRESP2() {
        // Behavior: RESP2 has no boolean type, so includeLower and includeUpper are rendered as 1 and 0.
        List<RedisMessage> flattened = explainAsRESP2(boundedRangeScanNode());

        assertEquals(1, intValueOf(valueAfterKey(flattened, "includeLower")));
        assertEquals(0, intValueOf(valueAfterKey(flattened, "includeUpper")));
    }

    @Test
    void shouldRenderRangeBoundFlagsAsBooleanTypeWhenRESP3() {
        // Behavior: the same bound flags are rendered as the RESP3 boolean type on a RESP3 session.
        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(boundedRangeScanNode());

        assertEquals(BooleanRedisMessage.TRUE, getValueForKey(result, "includeLower"));
        assertEquals(BooleanRedisMessage.FALSE, getValueForKey(result, "includeUpper"));
    }

    @Test
    void shouldRenderCollationFlagsAsIntegersWhenRESP2() {
        // Behavior: collation flags follow the same rule, a RESP2 client receives integers.
        RedisMessage result = RESPUtil.downgrade(PipelineExplainer.explainCollation(collationWithAllFlags()), RESPVersion.RESP2);

        assertInstanceOf(ArrayRedisMessage.class, result);
        List<RedisMessage> flattened = ((ArrayRedisMessage) result).children();
        assertEquals(1, intValueOf(valueAfterKey(flattened, "case_level")));
        assertEquals(1, intValueOf(valueAfterKey(flattened, "numeric_ordering")));
        assertEquals(1, intValueOf(valueAfterKey(flattened, "backwards")));
        assertEquals(1, intValueOf(valueAfterKey(flattened, "normalization")));
    }

    @Test
    void shouldRenderCollationFlagsAsBooleanTypeWhenRESP3() {
        // Behavior: the same collation flags are rendered as the RESP3 boolean type on a RESP3 session.
        MapRedisMessage result = PipelineExplainer.explainCollation(collationWithAllFlags());

        Map<RedisMessage, RedisMessage> map = result.children();
        assertEquals(BooleanRedisMessage.TRUE, getValueForKey(map, "case_level"));
        assertEquals(BooleanRedisMessage.TRUE, getValueForKey(map, "numeric_ordering"));
        assertEquals(BooleanRedisMessage.TRUE, getValueForKey(map, "backwards"));
        assertEquals(BooleanRedisMessage.TRUE, getValueForKey(map, "normalization"));
    }

    @Test
    void shouldRenderLiteralOperandAsItsUnwrappedValue() {
        // Behavior: an operand is unwrapped down to its value, not rendered as the wrapper's toString.
        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(
                indexScanNodeWith(new Operand.Literal(new Int32Val(25))));

        assertEquals(25, intValueOf(getValueForKey(result, "operand")));
    }

    @Test
    void shouldRenderStringLiteralOperandAsBulkString() {
        // Behavior: a string literal is rendered as a bulk string holding the raw value.
        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(
                indexScanNodeWith(new Operand.Literal(new StringVal("Alice"))));

        assertEquals("Alice", getStringValue(result, "operand"));
    }

    @Test
    void shouldRenderDoubleLiteralOperandAsBulkStringWhenRESP2() {
        // Behavior: RESP2 has no double type, so a double literal is rendered as a bulk string.
        List<RedisMessage> flattened = explainAsRESP2(indexScanNodeWith(new Operand.Literal(new DoubleVal(10.5))));

        RedisMessage operand = valueAfterKey(flattened, "operand");
        assertInstanceOf(FullBulkStringRedisMessage.class, operand);
    }

    @Test
    void shouldRenderDoubleLiteralOperandAsDoubleTypeWhenRESP3() {
        // Behavior: the same double literal is rendered as the RESP3 double type on a RESP3 session.
        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(
                indexScanNodeWith(new Operand.Literal(new DoubleVal(10.5))));

        RedisMessage operand = getValueForKey(result, "operand");
        assertInstanceOf(DoubleRedisMessage.class, operand);
        assertEquals(10.5, ((DoubleRedisMessage) operand).value());
    }

    @Test
    void shouldRenderParamOperandAsPlaceholder() {
        // Behavior: a cached plan holds no value for a parameter slot, so it renders as a placeholder.
        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(
                indexScanNodeWith(new Operand.Param(new ParamRef(3))));

        assertEquals("?3", getStringValue(result, "operand"));
    }

    @Test
    void shouldRenderLiteralListOperandAsArray() {
        // Behavior: an $in style operand renders as an array of its unwrapped values.
        Operand operand = new Operand.LiteralList(List.of(new Int32Val(1), new Int32Val(2)));
        Map<RedisMessage, RedisMessage> result = PipelineExplainer.explain(
                indexScanNodeWith(operand));

        RedisMessage rendered = getValueForKey(result, "operand");
        assertInstanceOf(ArrayRedisMessage.class, rendered);
        List<RedisMessage> items = ((ArrayRedisMessage) rendered).children();
        assertEquals(2, items.size());
        assertEquals(1, intValueOf(items.get(0)));
        assertEquals(2, intValueOf(items.get(1)));
    }

    @Test
    void shouldFlattenNestedMapsUnderFiltersWhenRESP2() {
        // Behavior: RESP2 has no map type, so the maps nested under filters are flattened too.
        List<RedisMessage> flattened = explainAsRESP2(getCompoundIndexScanNode());

        RedisMessage filters = valueAfterKey(flattened, "filters");
        assertInstanceOf(ArrayRedisMessage.class, filters);
        List<RedisMessage> filterMessages = ((ArrayRedisMessage) filters).children();
        assertEquals(2, filterMessages.size());

        assertInstanceOf(ArrayRedisMessage.class, filterMessages.get(0));
        assertInstanceOf(ArrayRedisMessage.class, filterMessages.get(1));
        assertEquals("age", getStringValue(pairsToMap((ArrayRedisMessage) filterMessages.get(0)), "selector"));
        assertEquals("score", getStringValue(pairsToMap((ArrayRedisMessage) filterMessages.get(1)), "selector"));
    }

    @Test
    void shouldFlattenNestedMapsUnderChildrenWhenRESP2() {
        // Behavior: union children are maps inside an array, they must be flattened for RESP2 as well.
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create("name_idx", "name", BsonType.STRING, false, IndexStatus.WAITING);
        IndexScanNode child1 = new IndexScanNode(1, indexDef, new IndexScanPredicate(1, "name", Operator.EQ, new Operand.Literal(new StringVal("Alice"))));
        IndexScanNode child2 = new IndexScanNode(2, indexDef, new IndexScanPredicate(2, "name", Operator.EQ, new Operand.Literal(new StringVal("Bob"))));

        List<RedisMessage> flattened = explainAsRESP2(new UnionNode(3, List.of(child1, child2)));

        RedisMessage children = valueAfterKey(flattened, "children");
        assertInstanceOf(ArrayRedisMessage.class, children);
        List<RedisMessage> childMessages = ((ArrayRedisMessage) children).children();
        assertEquals(2, childMessages.size());
        for (RedisMessage child : childMessages) {
            assertInstanceOf(ArrayRedisMessage.class, child);
        }
    }

    @Test
    void shouldKeepScalarArrayUnchangedWhenRESP2() {
        // Behavior: an array of scalars holds no map, so RESP2 keeps its shape and contents.
        Operand operand = new Operand.LiteralList(List.of(new Int32Val(1), new Int32Val(2)));
        List<RedisMessage> flattened = explainAsRESP2(indexScanNodeWith(operand));

        RedisMessage rendered = valueAfterKey(flattened, "operand");
        assertInstanceOf(ArrayRedisMessage.class, rendered);
        List<RedisMessage> items = ((ArrayRedisMessage) rendered).children();
        assertEquals(2, items.size());
        assertEquals(1, intValueOf(items.get(0)));
        assertEquals(2, intValueOf(items.get(1)));
    }

    /**
     * Renders a plan the way a RESP2 client receives it. The plan itself is always built with
     * RESP3 types, the response layer rewrites it, so the test walks the same path.
     */
    private List<RedisMessage> explainAsRESP2(PipelineNode node) {
        RedisMessage message = RESPUtil.downgrade(PipelineExplainer.explainAsMapMessage(node), RESPVersion.RESP2);
        assertInstanceOf(ArrayRedisMessage.class, message);
        return ((ArrayRedisMessage) message).children();
    }

    private Map<RedisMessage, RedisMessage> pairsToMap(ArrayRedisMessage flattened) {
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        List<RedisMessage> items = flattened.children();
        for (int i = 0; i + 1 < items.size(); i += 2) {
            map.put(items.get(i), items.get(i + 1));
        }
        return map;
    }

    private IndexScanNode indexScanNodeWith(Operand operand) {
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        return new IndexScanNode(1, indexDef, new IndexScanPredicate(1, "age", Operator.EQ, operand));
    }

    private Collation collationWithAllFlags() {
        return Collation.create("en", 3, true, "off", true, "non-ignorable", true, true, "punct");
    }

    private RangeScanNode boundedRangeScanNode() {
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("age",
                new Operand.Literal(new Int32Val(18)),
                new Operand.Literal(new Int32Val(65)),
                true, false);
        return new TestRangeScanNode(1, indexDef, predicate);
    }

    private long intValueOf(RedisMessage message) {
        assertInstanceOf(IntegerRedisMessage.class, message);
        return ((IntegerRedisMessage) message).value();
    }

    private RedisMessage valueAfterKey(List<RedisMessage> flattened, String keyName) {
        for (int i = 0; i < flattened.size() - 1; i++) {
            if (flattened.get(i) instanceof FullBulkStringRedisMessage msg
                    && msg.content().toString(StandardCharsets.UTF_8).equals(keyName)) {
                return flattened.get(i + 1);
            }
        }
        return null;
    }

    private RangeScanNode unboundedRangeScanNode() {
        SingleFieldIndexDefinition indexDef = SingleFieldIndexDefinition.create("age_idx", "age", BsonType.INT32, false, IndexStatus.WAITING);
        RangeScanPredicate predicate = new RangeScanPredicate("age", null, new Operand.Literal(new Int32Val(65)), false, false);
        return new TestRangeScanNode(1, indexDef, predicate);
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
