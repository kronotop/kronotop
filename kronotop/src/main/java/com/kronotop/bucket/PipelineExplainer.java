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
import com.kronotop.server.RESPUtil;
import com.kronotop.server.RESPVersion;
import com.kronotop.server.resp3.*;
import io.netty.buffer.Unpooled;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility class for converting pipeline execution plans to RESP protocol formats.
 * Supports both RESP3 (maps) and RESP2 (arrays) output formats for query plan explanation.
 */
public class PipelineExplainer {
    private static final int PLANNER_VERSION = 1;

    /**
     * Explains a pipeline node as a map structure.
     *
     * @param node    the pipeline node to explain
     * @param version the protocol version the plan is rendered for
     * @return a map of RedisMessage key-value pairs representing the plan
     */
    public static Map<RedisMessage, RedisMessage> explain(PipelineNode node, RESPVersion version) {
        Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();
        result.put(key("planner_version"), intValue(PLANNER_VERSION));

        if (node == null) {
            return result;
        }

        result.put(key("nodeType"), value(getNodeTypeName(node)));
        result.put(key("id"), intValue(node.id()));

        switch (node) {
            case IndexScanNode scan -> explainIndexScan(result, scan, version);
            case FullScanNode scan -> explainFullScan(result, scan, version);
            case RangeScanNode scan -> explainRangeScan(result, scan, version);
            case CompoundIndexScanNode scan -> explainCompoundIndexScan(result, scan, version);
            case UnionNode union -> explainUnion(result, union, version);
            case OrderedConcatNode orderedConcat -> explainOrderedConcat(result, orderedConcat, version);
            case TransformWithResidualPredicateNode transform -> explainTransform(result, transform, version);
            default -> result.put(key("details"), value("Unknown node type"));
        }

        if (node.next() != null) {
            result.put(key("next"), new MapRedisMessage(explain(node.next(), version)));
        }

        return result;
    }

    /**
     * Explains a pipeline node as a RESP2 array (flattened key-value pairs).
     *
     * @param node the pipeline node to explain
     * @return a list of RedisMessage representing the plan as flattened key-value pairs
     */
    public static List<RedisMessage> explainAsArray(PipelineNode node) {
        return flattenMap(explain(node, RESPVersion.RESP2));
    }

    /**
     * Wraps the explanation in an ArrayRedisMessage for direct RESP2 response.
     *
     * @param node the pipeline node to explain
     * @return an ArrayRedisMessage containing the flattened plan
     */
    public static ArrayRedisMessage explainAsArrayMessage(PipelineNode node) {
        return new ArrayRedisMessage(explainAsArray(node));
    }

    /**
     * Wraps the explanation in a MapRedisMessage for direct RESP3 response.
     *
     * @param node the pipeline node to explain
     * @return a MapRedisMessage containing the plan
     */
    public static MapRedisMessage explainAsMapMessage(PipelineNode node) {
        return new MapRedisMessage(explain(node, RESPVersion.RESP3));
    }

    private static void explainIndexScan(Map<RedisMessage, RedisMessage> result, IndexScanNode scan, RESPVersion version) {
        result.put(key("scanType"), value("INDEX_SCAN"));
        result.put(key("index"), value(scan.getIndexDefinition().name()));
        result.put(key("selector"), value(scan.predicate().selector()));
        result.put(key("operator"), value(scan.predicate().op().name()));
        result.put(key("operand"), formatOperand(scan.predicate().operand(), version));
        addIndexCollation(result, scan.getIndexDefinition(), version);
    }

    private static void explainFullScan(Map<RedisMessage, RedisMessage> result, FullScanNode scan, RESPVersion version) {
        result.put(key("scanType"), value("FULL_SCAN"));
        result.put(key("index"), value(scan.getIndexDefinition().name()));
        result.put(key("predicate"), explainPredicateAsMessage(scan.predicate(), version));
        if (scan.isCollationMismatch()) {
            result.put(key("collation_mismatch"), boolValue(true, version));
            if (scan.getRejectedIndex() != null) {
                result.put(key("rejected_index"), value(scan.getRejectedIndex()));
            }
        }
    }

    private static void explainRangeScan(Map<RedisMessage, RedisMessage> result, RangeScanNode scan, RESPVersion version) {
        result.put(key("scanType"), value("RANGE_SCAN"));
        result.put(key("index"), value(scan.getIndexDefinition().name()));
        result.put(key("selector"), value(scan.predicate().selector()));
        result.put(key("lowerBound"), formatOperand(scan.predicate().lowerBound(), version));
        result.put(key("upperBound"), formatOperand(scan.predicate().upperBound(), version));
        result.put(key("includeLower"), boolValue(scan.predicate().includeLower(), version));
        result.put(key("includeUpper"), boolValue(scan.predicate().includeUpper(), version));
        addIndexCollation(result, scan.getIndexDefinition(), version);
    }

    private static void explainCompoundIndexScan(Map<RedisMessage, RedisMessage> result, CompoundIndexScanNode scan, RESPVersion version) {
        result.put(key("scanType"), value("COMPOUND_INDEX_SCAN"));
        result.put(key("index"), value(scan.indexDefinition().name()));

        List<RedisMessage> filterMessages = new ArrayList<>();
        for (CompoundIndexScanNode.CompoundIndexScanFilter filter : scan.filters()) {
            Map<RedisMessage, RedisMessage> filterMap = new LinkedHashMap<>();
            filterMap.put(key("selector"), value(filter.selector()));
            filterMap.put(key("operator"), value(filter.op().name()));
            filterMap.put(key("operand"), formatOperand(filter.operand(), version));
            filterMessages.add(new MapRedisMessage(filterMap));
        }
        result.put(key("filters"), new ArrayRedisMessage(filterMessages));
        if (scan.indexDefinition().collation() != null) {
            result.put(key("index_collation"), explainCollation(scan.indexDefinition().collation(), version));
        }
    }

    private static void explainUnion(Map<RedisMessage, RedisMessage> result, UnionNode union, RESPVersion version) {
        result.put(key("operation"), value("UNION"));
        result.put(key("children"), explainChildrenAsMessage(union.children(), version));
    }

    private static void explainOrderedConcat(Map<RedisMessage, RedisMessage> result, OrderedConcatNode orderedConcat, RESPVersion version) {
        result.put(key("operation"), value("ORDERED_CONCAT"));
        result.put(key("children"), explainChildrenAsMessage(orderedConcat.children(), version));
    }

    private static void explainTransform(Map<RedisMessage, RedisMessage> result, TransformWithResidualPredicateNode transform, RESPVersion version) {
        result.put(key("operation"), value("FILTER"));
        result.put(key("predicate"), explainPredicateAsMessage(transform.predicate(), version));
    }

    private static ArrayRedisMessage explainChildrenAsMessage(List<PipelineNode> children, RESPVersion version) {
        List<RedisMessage> childMessages = new ArrayList<>();
        for (PipelineNode child : children) {
            childMessages.add(new MapRedisMessage(explain(child, version)));
        }
        return new ArrayRedisMessage(childMessages);
    }

    private static RedisMessage explainPredicateAsMessage(ResidualPredicateNode predicate, RESPVersion version) {
        return new MapRedisMessage(explainPredicate(predicate, version));
    }

    private static Map<RedisMessage, RedisMessage> explainPredicate(ResidualPredicateNode predicate, RESPVersion version) {
        Map<RedisMessage, RedisMessage> result = new LinkedHashMap<>();

        switch (predicate) {
            case ResidualPredicate p -> {
                result.put(key("type"), value("PREDICATE"));
                result.put(key("selector"), value(p.selector()));
                result.put(key("operator"), value(p.op().name()));
                result.put(key("operand"), formatOperand(p.operand(), version));
            }
            case ResidualAndNode andNode -> {
                result.put(key("type"), value("AND"));
                result.put(key("children"), explainPredicateChildrenAsMessage(andNode.children(), version));
            }
            case ResidualOrNode orNode -> {
                result.put(key("type"), value("OR"));
                result.put(key("children"), explainPredicateChildrenAsMessage(orNode.children(), version));
            }
            case AlwaysTruePredicate ignored -> result.put(key("type"), value("ALWAYS_TRUE"));
            default -> result.put(key("type"), value("UNKNOWN"));
        }

        return result;
    }

    private static ArrayRedisMessage explainPredicateChildrenAsMessage(List<ResidualPredicateNode> children, RESPVersion version) {
        List<RedisMessage> childMessages = new ArrayList<>();
        for (ResidualPredicateNode child : children) {
            childMessages.add(new MapRedisMessage(explainPredicate(child, version)));
        }
        return new ArrayRedisMessage(childMessages);
    }

    private static void addIndexCollation(Map<RedisMessage, RedisMessage> result, SingleFieldIndexDefinition definition, RESPVersion version) {
        if (definition.collation() != null) {
            result.put(key("index_collation"), explainCollation(definition.collation(), version));
        }
    }

    /**
     * Renders a collation as a RESP3 map.
     *
     * @param collation the collation to render
     * @param version   the protocol version the collation is rendered for
     */
    public static MapRedisMessage explainCollation(Collation collation, RESPVersion version) {
        return new MapRedisMessage(buildCollationMap(collation, version));
    }

    /**
     * Renders a collation as a RESP2 array of flattened key-value pairs.
     *
     * @param collation the collation to render
     */
    public static ArrayRedisMessage explainCollationAsArrayMessage(Collation collation) {
        return new ArrayRedisMessage(flattenMap(buildCollationMap(collation, RESPVersion.RESP2)));
    }

    private static Map<RedisMessage, RedisMessage> buildCollationMap(Collation collation, RESPVersion version) {
        Map<RedisMessage, RedisMessage> map = new LinkedHashMap<>();
        map.put(key("locale"), value(collation.locale()));
        map.put(key("strength"), intValue(collation.strength()));
        if (collation.caseLevel()) {
            map.put(key("case_level"), boolValue(true, version));
        }
        if (!"off".equals(collation.caseFirst())) {
            map.put(key("case_first"), value(collation.caseFirst()));
        }
        if (collation.numericOrdering()) {
            map.put(key("numeric_ordering"), boolValue(true, version));
        }
        if (!"non-ignorable".equals(collation.alternate())) {
            map.put(key("alternate"), value(collation.alternate()));
        }
        if (collation.backwards()) {
            map.put(key("backwards"), boolValue(true, version));
        }
        if (collation.normalization()) {
            map.put(key("normalization"), boolValue(true, version));
        }
        return map;
    }

    private static String getNodeTypeName(PipelineNode node) {
        String className = node.getClass().getSimpleName();
        if (className.endsWith("Node")) {
            return className.substring(0, className.length() - 4);
        }
        return className;
    }

    private static List<RedisMessage> flattenMap(Map<RedisMessage, RedisMessage> map) {
        List<RedisMessage> result = new ArrayList<>();
        for (Map.Entry<RedisMessage, RedisMessage> entry : map.entrySet()) {
            result.add(entry.getKey());
            result.add(toRESP2Value(entry.getValue()));
        }
        return result;
    }

    /**
     * Converts a rendered value to its RESP2 form. A map becomes a flat array of key-value
     * pairs. An array keeps its shape and is only rebuilt when it holds a value that changes.
     */
    private static RedisMessage toRESP2Value(RedisMessage message) {
        if (message instanceof MapRedisMessage mapMessage) {
            return new ArrayRedisMessage(flattenMap(mapMessage.children()));
        }
        if (message instanceof ArrayRedisMessage arrayMessage) {
            List<RedisMessage> children = arrayMessage.children();
            List<RedisMessage> converted = null;
            for (int i = 0; i < children.size(); i++) {
                RedisMessage item = children.get(i);
                RedisMessage value = toRESP2Value(item);
                if (value != item && converted == null) {
                    converted = new ArrayList<>(children.subList(0, i));
                }
                if (converted != null) {
                    converted.add(value);
                }
            }
            return converted == null ? arrayMessage : new ArrayRedisMessage(converted);
        }
        return message;
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

    private static RedisMessage boolValue(boolean value, RESPVersion version) {
        return RESPUtil.booleanMessage(value, version);
    }

    /**
     * Renders an operand. A literal is unwrapped down to its value, a parameter slot is
     * rendered as its placeholder because the plan holds no value for it.
     */
    private static RedisMessage formatOperand(Operand operand, RESPVersion version) {
        if (operand == null) {
            return RESPUtil.nullMessage(version);
        }
        return switch (operand) {
            case Operand.Literal(BqlValue literal) -> formatValue(literal, version);
            case Operand.Param(ParamRef ref) -> value(placeholder(ref));
            case Operand.LiteralList(List<BqlValue> values) -> {
                List<RedisMessage> items = new ArrayList<>();
                for (BqlValue item : values) {
                    items.add(formatValue(item, version));
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
     * Renders a BQL value with the closest RESP type the negotiated protocol version offers.
     * Values with no matching RESP type fall back to their JSON form.
     */
    private static RedisMessage formatValue(BqlValue bqlValue, RESPVersion version) {
        return switch (bqlValue) {
            case StringVal(String s) -> value(s);
            case Int32Val(int i) -> new IntegerRedisMessage(i);
            case Int64Val(long l) -> new IntegerRedisMessage(l);
            case DateTimeVal(long l) -> new IntegerRedisMessage(l);
            case TimestampVal(long l) -> new IntegerRedisMessage(l);
            case DoubleVal(double d) -> RESPUtil.doubleMessage(d, version);
            case BooleanVal(boolean b) -> RESPUtil.booleanMessage(b, version);
            case NullVal ignored -> RESPUtil.nullMessage(version);
            case BinaryVal(byte[] bytes) -> value("<binary:" + bytes.length + " bytes>");
            case ArrayVal(List<BqlValue> values) -> {
                List<RedisMessage> items = new ArrayList<>();
                for (BqlValue item : values) {
                    items.add(formatValue(item, version));
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
