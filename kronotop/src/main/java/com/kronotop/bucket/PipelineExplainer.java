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

package com.kronotop.bucket;

import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.SingleFieldIndexDefinition;
import com.kronotop.bucket.pipeline.*;
import com.kronotop.server.resp3.*;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for converting pipeline execution plans to RESP messages. The plan is always
 * rendered with the richest RESP types, the response layer rewrites it for clients that speak
 * an older protocol version.
 */
public class PipelineExplainer {
    private static final int PLANNER_VERSION = 1;

    /**
     * Explains a pipeline node as a map structure.
     *
     * @param node the pipeline node to explain
     * @return a map of RedisMessage key-value pairs representing the plan
     */
    public static Map<RedisMessage, RedisMessage> explain(PipelineNode node) {
        Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
        result.put(key("planner_version"), intValue(PLANNER_VERSION));

        if (node == null) {
            return result;
        }

        result.put(key("nodeType"), value(getNodeTypeName(node)));
        result.put(key("id"), intValue(node.id()));

        switch (node) {
            case IndexScanNode scan -> explainIndexScan(result, scan);
            case FullScanNode scan -> explainFullScan(result, scan);
            case RangeScanNode scan -> explainRangeScan(result, scan);
            case CompoundIndexScanNode scan -> explainCompoundIndexScan(result, scan);
            case UnionNode union -> explainUnion(result, union);
            case OrderedConcatNode orderedConcat -> explainOrderedConcat(result, orderedConcat);
            case TransformWithResidualPredicateNode transform -> explainTransform(result, transform);
            default -> result.put(key("details"), value("Unknown node type"));
        }

        if (node.next() != null) {
            result.put(key("next"), new MapRedisMessage(explain(node.next())));
        }

        return result;
    }

    /**
     * Wraps the explanation in a MapRedisMessage for direct response.
     *
     * @param node the pipeline node to explain
     * @return a MapRedisMessage containing the plan
     */
    public static MapRedisMessage explainAsMapMessage(PipelineNode node) {
        return new MapRedisMessage(explain(node));
    }

    private static void explainIndexScan(Map<RedisMessage, RedisMessage> result, IndexScanNode scan) {
        result.put(key("scanType"), value("INDEX_SCAN"));
        result.put(key("index"), value(scan.getIndexDefinition().name()));
        result.put(key("selector"), value(scan.predicate().selector()));
        result.put(key("operator"), value(scan.predicate().op().name()));
        result.put(key("operand"), formatOperand(scan.predicate().operand()));
        addIndexCollation(result, scan.getIndexDefinition());
    }

    private static void explainFullScan(Map<RedisMessage, RedisMessage> result, FullScanNode scan) {
        result.put(key("scanType"), value("FULL_SCAN"));
        result.put(key("index"), value(scan.getIndexDefinition().name()));
        result.put(key("predicate"), explainPredicateAsMessage(scan.predicate()));
        if (scan.isCollationMismatch()) {
            result.put(key("collation_mismatch"), BooleanRedisMessage.TRUE);
            if (scan.getRejectedIndex() != null) {
                result.put(key("rejected_index"), value(scan.getRejectedIndex()));
            }
        }
    }

    private static void explainRangeScan(Map<RedisMessage, RedisMessage> result, RangeScanNode scan) {
        result.put(key("scanType"), value("RANGE_SCAN"));
        result.put(key("index"), value(scan.getIndexDefinition().name()));
        result.put(key("selector"), value(scan.predicate().selector()));
        result.put(key("lowerBound"), formatOperand(scan.predicate().lowerBound()));
        result.put(key("upperBound"), formatOperand(scan.predicate().upperBound()));
        result.put(key("includeLower"), boolValue(scan.predicate().includeLower()));
        result.put(key("includeUpper"), boolValue(scan.predicate().includeUpper()));
        addIndexCollation(result, scan.getIndexDefinition());
    }

    private static void explainCompoundIndexScan(Map<RedisMessage, RedisMessage> result, CompoundIndexScanNode scan) {
        result.put(key("scanType"), value("COMPOUND_INDEX_SCAN"));
        result.put(key("index"), value(scan.indexDefinition().name()));

        List<RedisMessage> filterMessages = new ArrayList<>();
        for (CompoundIndexScanNode.CompoundIndexScanFilter filter : scan.filters()) {
            Map<RedisMessage, RedisMessage> filterMap = new LinkedHashMap<>();
            filterMap.put(key("selector"), value(filter.selector()));
            filterMap.put(key("operator"), value(filter.op().name()));
            filterMap.put(key("operand"), formatOperand(filter.operand()));
            filterMessages.add(new MapRedisMessage(filterMap));
        }
        result.put(key("filters"), new ArrayRedisMessage(filterMessages));
        if (scan.indexDefinition().collation() != null) {
            result.put(key("index_collation"), explainCollation(scan.indexDefinition().collation()));
        }
    }

    private static void explainUnion(Map<RedisMessage, RedisMessage> result, UnionNode union) {
        result.put(key("operation"), value("UNION"));
        result.put(key("children"), explainChildrenAsMessage(union.children()));
    }

    private static void explainOrderedConcat(Map<RedisMessage, RedisMessage> result, OrderedConcatNode orderedConcat) {
        result.put(key("operation"), value("ORDERED_CONCAT"));
        result.put(key("children"), explainChildrenAsMessage(orderedConcat.children()));
    }

    private static void explainTransform(Map<RedisMessage, RedisMessage> result, TransformWithResidualPredicateNode transform) {
        result.put(key("operation"), value("FILTER"));
        result.put(key("predicate"), explainPredicateAsMessage(transform.predicate()));
    }

    private static ArrayRedisMessage explainChildrenAsMessage(List<PipelineNode> children) {
        List<RedisMessage> childMessages = new ArrayList<>();
        for (PipelineNode child : children) {
            childMessages.add(new MapRedisMessage(explain(child)));
        }
        return new ArrayRedisMessage(childMessages);
    }

    private static RedisMessage explainPredicateAsMessage(ResidualPredicateNode predicate) {
        return new MapRedisMessage(explainPredicate(predicate));
    }

    private static Map<RedisMessage, RedisMessage> explainPredicate(ResidualPredicateNode predicate) {
        Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();

        switch (predicate) {
            case ResidualPredicate p -> {
                result.put(key("type"), value("PREDICATE"));
                result.put(key("selector"), value(p.selector()));
                result.put(key("operator"), value(p.op().name()));
                result.put(key("operand"), formatOperand(p.operand()));
            }
            case ResidualAndNode andNode -> {
                result.put(key("type"), value("AND"));
                result.put(key("children"), explainPredicateChildrenAsMessage(andNode.children()));
            }
            case ResidualOrNode orNode -> {
                result.put(key("type"), value("OR"));
                result.put(key("children"), explainPredicateChildrenAsMessage(orNode.children()));
            }
            case AlwaysTruePredicate ignored -> result.put(key("type"), value("ALWAYS_TRUE"));
            default -> result.put(key("type"), value("UNKNOWN"));
        }

        return result;
    }

    private static ArrayRedisMessage explainPredicateChildrenAsMessage(List<ResidualPredicateNode> children) {
        List<RedisMessage> childMessages = new ArrayList<>();
        for (ResidualPredicateNode child : children) {
            childMessages.add(new MapRedisMessage(explainPredicate(child)));
        }
        return new ArrayRedisMessage(childMessages);
    }

    private static void addIndexCollation(Map<RedisMessage, RedisMessage> result, SingleFieldIndexDefinition definition) {
        if (definition.collation() != null) {
            result.put(key("index_collation"), explainCollation(definition.collation()));
        }
    }

    /**
     * Renders a collation as a map.
     *
     * @param collation the collation to render
     */
    public static MapRedisMessage explainCollation(Collation collation) {
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        map.put(key("locale"), value(collation.locale()));
        map.put(key("strength"), intValue(collation.strength()));
        if (collation.caseLevel()) {
            map.put(key("case_level"), BooleanRedisMessage.TRUE);
        }
        if (!"off".equals(collation.caseFirst())) {
            map.put(key("case_first"), value(collation.caseFirst()));
        }
        if (collation.numericOrdering()) {
            map.put(key("numeric_ordering"), BooleanRedisMessage.TRUE);
        }
        if (!"non-ignorable".equals(collation.alternate())) {
            map.put(key("alternate"), value(collation.alternate()));
        }
        if (collation.backwards()) {
            map.put(key("backwards"), BooleanRedisMessage.TRUE);
        }
        if (collation.normalization()) {
            map.put(key("normalization"), BooleanRedisMessage.TRUE);
        }
        return new MapRedisMessage(map);
    }

    private static String getNodeTypeName(PipelineNode node) {
        String className = node.getClass().getSimpleName();
        if (className.endsWith("Node")) {
            return className.substring(0, className.length() - 4);
        }
        return className;
    }

    private static FullBulkStringRedisMessage key(String key) {
        return new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(key.getBytes(StandardCharsets.UTF_8)));
    }

    private static FullBulkStringRedisMessage value(String value) {
        return new FullBulkStringRedisMessage(Unpooled.wrappedBuffer(value.getBytes(StandardCharsets.UTF_8)));
    }

    private static IntegerRedisMessage intValue(int value) {
        return new IntegerRedisMessage(value);
    }

    private static RedisMessage boolValue(boolean value) {
        return value ? BooleanRedisMessage.TRUE : BooleanRedisMessage.FALSE;
    }

    /**
     * Renders an operand. A literal is unwrapped down to its value, a parameter slot is
     * rendered as its placeholder because the plan holds no value for it.
     */
    private static RedisMessage formatOperand(Operand operand) {
        if (operand == null) {
            return NullRedisMessage.INSTANCE;
        }
        return switch (operand) {
            case Operand.Literal(BqlValue literal) -> formatValue(literal);
            case Operand.Param(ParamRef ref) -> value(placeholder(ref));
            case Operand.LiteralList(List<BqlValue> values) -> {
                List<RedisMessage> items = new ArrayList<>();
                for (BqlValue item : values) {
                    items.add(formatValue(item));
                }
                yield new ArrayRedisMessage(items);
            }
            case Operand.ParamList(List<ParamRef> refs) -> {
                List<RedisMessage> items = new ArrayList<>();
                for (ParamRef ref : refs) {
                    items.add(value(placeholder(ref)));
                }
                yield new ArrayRedisMessage(items);
            }
        };
    }

    /**
     * Renders a BQL value with the closest RESP type. Values with no matching RESP type fall
     * back to their JSON form.
     */
    private static RedisMessage formatValue(BqlValue bqlValue) {
        return switch (bqlValue) {
            case StringVal(String s) -> value(s);
            case Int32Val(int i) -> new IntegerRedisMessage(i);
            case Int64Val(long l) -> new IntegerRedisMessage(l);
            case DateTimeVal(long l) -> new IntegerRedisMessage(l);
            case TimestampVal(long l) -> new IntegerRedisMessage(l);
            case DoubleVal(double d) -> new DoubleRedisMessage(d);
            case BooleanVal(boolean b) -> boolValue(b);
            case NullVal ignored -> NullRedisMessage.INSTANCE;
            case BinaryVal(byte[] bytes) -> value("<binary:" + bytes.length + " bytes>");
            case ArrayVal(List<BqlValue> values) -> {
                List<RedisMessage> items = new ArrayList<>();
                for (BqlValue item : values) {
                    items.add(formatValue(item));
                }
                yield new ArrayRedisMessage(items);
            }
            default -> value(bqlValue.toJson());
        };
    }

    private static String placeholder(ParamRef ref) {
        return "?" + ref.index();
    }
}
