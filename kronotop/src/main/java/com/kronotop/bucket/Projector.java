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

import com.kronotop.bucket.bql.ast.BqlExpr;
import com.kronotop.bucket.index.SelectorMatcher;
import com.kronotop.bucket.pipeline.PositionalMatchFinder;
import com.kronotop.internal.StringUtil;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonValue;

import java.util.*;

/**
 * Applies field-level projection to BSON documents.
 * Supports inclusion/exclusion modes and the {@code $} positional operator
 * which returns the first array element that matches the query condition.
 */
public final class Projector {

    private static final String ID_FIELD = ReservedFieldName.ID.getValue();
    private static final String POSITIONAL_SUFFIX = ".$";
    private static final String SLICE_OPERATOR = "$slice";

    private Projector() {
    }

    /**
     * Projects a list of documents according to the given projection spec.
     * The spec is parsed once and applied to every document.
     *
     * @param documents      the source documents
     * @param projectionSpec the projection specification (e.g. {@code {"name": 1, "grades.$": 1}})
     * @param parsedQuery    the parsed query used to determine which array element the {@code $} operator matched;
     *                       required when the projection spec contains {@code $}, may be null otherwise
     * @return projected documents
     */
    public static List<BsonDocument> project(List<BsonDocument> documents, BsonDocument projectionSpec, BqlExpr parsedQuery) {
        if (projectionSpec.isEmpty()) {
            return documents;
        }

        ParsedProjection parsed = parse(projectionSpec);

        if (parsed.positionalArrayPath != null && parsedQuery == null) {
            throw new IllegalArgumentException(
                    "The positional $ operator requires a parsed query to determine the matching array element");
        }

        List<BsonDocument> results = new ArrayList<>(documents.size());
        for (BsonDocument document : documents) {
            results.add(applyProjection(document, parsed, parsedQuery));
        }
        return results;
    }

    /**
     * Projects a list of documents according to the given projection spec.
     * Use this overload when the projection spec does not contain the {@code $} operator.
     *
     * @param documents      the source documents
     * @param projectionSpec the projection specification (e.g. {@code {"name": 1}})
     * @return projected documents
     */
    public static List<BsonDocument> project(List<BsonDocument> documents, BsonDocument projectionSpec) {
        return project(documents, projectionSpec, null);
    }

    private static ParsedProjection parse(BsonDocument spec) {
        boolean hasInclusion = false;
        boolean hasExclusion = false;
        boolean excludeId = false;

        Set<String> fields = new LinkedHashSet<>();
        String positionalArrayPath = null;
        List<SliceOp> slices = new ArrayList<>();

        for (Map.Entry<String, BsonValue> entry : spec.entrySet()) {
            String field = entry.getKey();
            BsonValue raw = entry.getValue();

            // A document value is an operator spec. $slice is the only supported operator;
            // anything else is rejected here instead of failing later on asNumber().
            // Slices are mode-neutral: they do not flip hasInclusion/hasExclusion.
            if (raw.isDocument()) {
                BsonDocument opDoc = raw.asDocument();
                if (!opDoc.containsKey(SLICE_OPERATOR)) {
                    throw new IllegalArgumentException(
                            "Unsupported projection operator for field '" + field + "': only $slice is supported");
                }
                slices.add(parseSliceOperator(field, opDoc));
                continue;
            }

            int value = raw.asNumber().intValue();

            if (field.equals(ID_FIELD)) {
                if (value == 0) {
                    excludeId = true;
                }
                continue;
            }

            if (value == 1) {
                hasInclusion = true;
            } else if (value == 0) {
                hasExclusion = true;
            }

            // Detect $ positional operator
            if (field.endsWith(POSITIONAL_SUFFIX)) {
                // Validate: $ must be at the end
                String withoutSuffix = field.substring(0, field.length() - POSITIONAL_SUFFIX.length());
                String[] segments = StringUtil.split(withoutSuffix);
                // After removing the trailing .$, no segment should be $
                for (String segment : segments) {
                    if (segment.equals("$")) {
                        throw new IllegalArgumentException(
                                "The positional operator $ can only appear at the end of the field path: " + field);
                    }
                }

                if (positionalArrayPath != null) {
                    throw new IllegalArgumentException(
                            "Only one positional $ operator is allowed in the projection spec");
                }
                positionalArrayPath = withoutSuffix;
                fields.add(field);
            } else {
                // Validate: no $ in the middle
                String[] segments = StringUtil.split(field);
                for (int i = 0; i < segments.length - 1; i++) {
                    if (segments[i].equals("$")) {
                        throw new IllegalArgumentException(
                                "The positional operator $ can only appear at the end of the field path: " + field);
                    }
                }
                fields.add(field);
            }
        }

        if (hasInclusion && hasExclusion) {
            throw new IllegalArgumentException(
                    "Cannot mix inclusion and exclusion in projection spec (except _id: 0)");
        }

        if (positionalArrayPath != null && !slices.isEmpty()) {
            throw new IllegalArgumentException(
                    "$slice cannot be combined with the positional $ operator in the same projection spec");
        }

        for (SliceOp op : slices) {
            for (String f : fields) {
                if (pathCollides(op.path, f)) {
                    throw new IllegalArgumentException(
                            "$slice on '" + op.path + "' collides with projecting field '" + f + "'");
                }
            }
            for (SliceOp other : slices) {
                if (other != op && pathCollides(op.path, other.path)) {
                    throw new IllegalArgumentException(
                            "$slice on '" + op.path + "' collides with $slice on '" + other.path + "'");
                }
            }
        }

        // $slice alone (no inclusion fields) behaves as an exclusion: all other fields pass through.
        boolean inclusionMode = hasInclusion || (!hasExclusion && !excludeId && slices.isEmpty());
        return new ParsedProjection(inclusionMode, excludeId, fields, positionalArrayPath, slices);
    }

    private static SliceOp parseSliceOperator(String field, BsonDocument opDoc) {
        if (opDoc.size() != 1 || !opDoc.containsKey(SLICE_OPERATOR)) {
            throw new IllegalArgumentException(
                    "Unsupported projection operator for field '" + field + "': only $slice is supported");
        }

        BsonValue arg = opDoc.get(SLICE_OPERATOR);
        if (arg.isInt32() || arg.isInt64()) {
            return new SliceOp(field, requireIntArg(arg, field), null);
        }
        if (arg.isArray()) {
            BsonArray a = arg.asArray();
            if (a.size() != 2) {
                throw new IllegalArgumentException(
                        "$slice on '" + field + "' must be a number or [skip, limit]");
            }
            int skip = requireIntArg(a.get(0), field);
            int limit = requireIntArg(a.get(1), field);
            if (limit <= 0) {
                throw new IllegalArgumentException("$slice limit must be positive: " + field);
            }
            return new SliceOp(field, skip, limit);
        }
        throw new IllegalArgumentException(
                "$slice on '" + field + "' must be a number or [skip, limit]");
    }

    /**
     * Requires an integer-typed slice argument. Accepts {@code Int32}, or {@code Int64} that
     * fits in an {@code int}; rejects fractional, decimal, or out-of-range values rather than
     * silently truncating them.
     */
    private static int requireIntArg(BsonValue value, String field) {
        if (value.isInt32()) {
            return value.asInt32().getValue();
        }
        if (value.isInt64()) {
            long v = value.asInt64().getValue();
            if (v < Integer.MIN_VALUE || v > Integer.MAX_VALUE) {
                throw new IllegalArgumentException(
                        "$slice on '" + field + "' is out of the supported integer range: " + v);
            }
            return (int) v;
        }
        throw new IllegalArgumentException(
                "$slice on '" + field + "' must be an integer, not " + value.getBsonType());
    }

    /**
     * Two paths collide when they are equal or one is a prefix of the other in either
     * direction. This rejects projecting a field embedded in a sliced array, slicing inside
     * a fully projected parent, and overlapping slice paths.
     */
    private static boolean pathCollides(String a, String b) {
        return a.equals(b) || a.startsWith(b + ".") || b.startsWith(a + ".");
    }

    private static BsonDocument applyProjection(BsonDocument document, ParsedProjection parsed, BqlExpr parsedQuery) {
        BsonDocument result = new BsonDocument();

        if (parsed.inclusionMode) {
            // Include only specified fields (plus _id by default)
            if (!parsed.excludeId && document.containsKey(ID_FIELD)) {
                result.put(ID_FIELD, document.get(ID_FIELD));
            }

            for (String field : parsed.fields) {
                if (field.endsWith(POSITIONAL_SUFFIX)) {
                    applyPositionalProjection(document, result, parsed.positionalArrayPath, parsedQuery);
                } else {
                    includeField(document, result, field);
                }
            }

            // Sliced arrays are included like any other inclusion field; slicing happens at the leaf.
            for (SliceOp op : parsed.slices) {
                includeSlicedField(document, result, StringUtil.split(op.path), 0, op);
            }
        } else {
            // Separate top-level and dot-notation exclusions
            Set<String> topLevel = new HashSet<>();
            List<String> nested = new ArrayList<>();
            for (String field : parsed.fields) {
                if (field.contains(".")) {
                    nested.add(field);
                } else {
                    topLevel.add(field);
                }
            }

            for (Map.Entry<String, BsonValue> entry : document.entrySet()) {
                String key = entry.getKey();
                if (key.equals(ID_FIELD) && parsed.excludeId) {
                    continue;
                }
                if (topLevel.contains(key)) {
                    continue;
                }
                result.put(key, entry.getValue());
            }

            for (String path : nested) {
                removeNestedField(result, path);
            }

            // Replace each sliced array in place, preserving sibling fields; non-array values stay untouched.
            for (SliceOp op : parsed.slices) {
                replaceSlicedField(result, StringUtil.split(op.path), 0, op);
            }
        }

        return result;
    }

    private static void applyPositionalProjection(BsonDocument document, BsonDocument result,
                                                  String arrayPath, BqlExpr parsedQuery) {
        BsonValue value = SelectorMatcher.match(arrayPath, document);
        if (value == null || !value.isArray()) {
            return;
        }

        BsonArray array = value.asArray();
        BsonArray projected = new BsonArray();
        if (!array.isEmpty()) {
            Map<String, Integer> matchedPositions = PositionalMatchFinder.findMatchedPositions(
                    document, parsedQuery, Set.of(arrayPath), 0);
            Integer matchedIndex = matchedPositions.get(arrayPath);
            if (matchedIndex != null && matchedIndex < array.size()) {
                projected.add(array.get(matchedIndex));
            }
        }

        setNestedField(result, arrayPath, projected);
    }

    private static void includeField(BsonDocument document, BsonDocument result, String field) {
        if (!field.contains(".")) {
            // Top-level field
            if (document.containsKey(field)) {
                result.put(field, document.get(field));
            }
        } else {
            String[] parts = StringUtil.split(field);
            includeNestedField(document, result, parts, 0);
        }
    }

    private static void includeNestedField(BsonDocument source, BsonDocument target, String[] parts, int index) {
        includeProjectedField(source, target, parts, index, (value, leafTarget, key) -> leafTarget.put(key, value));
    }

    /**
     * Walks a dot-notation path from {@code source} into {@code target}, building fresh intermediate
     * documents and recursing through arrays of documents, then applies {@code leaf} to the value at
     * the end of the path. Document elements are mapped to {@code target} by their order among
     * documents, not by their source index, so a non-document element interleaved in an array does
     * not shift later elements. This keeps multiple projection fields that cross the same array
     * (for example a sibling inclusion field and a {@code $slice}) aligned on the same elements.
     */
    private static void includeProjectedField(BsonDocument source, BsonDocument target, String[] parts, int index, LeafProjector leaf) {
        String part = parts[index];
        if (!source.containsKey(part)) {
            return;
        }

        BsonValue sourceValue = source.get(part);

        if (index == parts.length - 1) {
            leaf.project(sourceValue, target, part);
            return;
        }

        if (sourceValue.isDocument()) {
            BsonDocument targetNested;
            if (target.containsKey(part) && target.get(part).isDocument()) {
                targetNested = target.get(part).asDocument();
            } else {
                targetNested = new BsonDocument();
                target.put(part, targetNested);
            }
            includeProjectedField(sourceValue.asDocument(), targetNested, parts, index + 1, leaf);
        } else if (sourceValue.isArray()) {
            BsonArray sourceArray = sourceValue.asArray();
            BsonArray targetArray;
            if (target.containsKey(part) && target.get(part).isArray()) {
                targetArray = target.get(part).asArray();
            } else {
                targetArray = new BsonArray();
                target.put(part, targetArray);
            }

            int targetIndex = 0;
            for (BsonValue element : sourceArray) {
                if (!element.isDocument()) {
                    continue;
                }
                BsonDocument targetElement;
                if (targetIndex < targetArray.size() && targetArray.get(targetIndex).isDocument()) {
                    targetElement = targetArray.get(targetIndex).asDocument();
                } else {
                    targetElement = new BsonDocument();
                    targetArray.add(targetElement);
                }
                includeProjectedField(element.asDocument(), targetElement, parts, index + 1, leaf);
                targetIndex++;
            }
        }
    }

    private static void removeNestedField(BsonDocument doc, String path) {
        String[] parts = StringUtil.split(path);
        removeNestedFieldRecursive(doc, parts, 0);
    }

    private static void removeNestedFieldRecursive(BsonDocument current, String[] parts, int index) {
        String part = parts[index];
        if (!current.containsKey(part)) {
            return;
        }

        if (index == parts.length - 1) {
            current.remove(part);
            return;
        }

        BsonValue value = current.get(part);
        if (value.isDocument()) {
            BsonDocument cloned = new BsonDocument();
            cloned.putAll(value.asDocument());
            current.put(part, cloned);
            removeNestedFieldRecursive(cloned, parts, index + 1);
        } else if (value.isArray()) {
            BsonArray clonedArray = new BsonArray();
            for (BsonValue element : value.asArray()) {
                if (element.isDocument()) {
                    BsonDocument clonedElement = new BsonDocument();
                    clonedElement.putAll(element.asDocument());
                    removeNestedFieldRecursive(clonedElement, parts, index + 1);
                    clonedArray.add(clonedElement);
                } else {
                    clonedArray.add(element);
                }
            }
            current.put(part, clonedArray);
        }
    }

    private static void setNestedField(BsonDocument doc, String path, BsonValue value) {
        String[] parts = StringUtil.split(path);
        BsonDocument current = doc;

        for (int i = 0; i < parts.length - 1; i++) {
            String part = parts[i];
            if (!current.containsKey(part) || !current.get(part).isDocument()) {
                BsonDocument nested = new BsonDocument();
                current.put(part, nested);
                current = nested;
            } else {
                current = current.get(part).asDocument();
            }
        }

        current.put(parts[parts.length - 1], value);
    }

    /**
     * Returns the slice of {@code array} described by {@code op} as a new array.
     * Count form ({@code limit == null}) returns the first/last n elements; range form
     * skips and caps. Out-of-range starts clamp to the array bounds.
     */
    private static BsonArray sliceArray(BsonArray array, SliceOp op) {
        int len = array.size();
        int start;
        int end;

        if (op.limit == null) {
            int n = op.skip;
            if (n >= 0) {
                start = 0;
                end = Math.min(n, len);
            } else {
                start = Math.max(0, len + n);
                end = len;
            }
        } else {
            start = op.skip >= 0 ? Math.min(op.skip, len) : Math.max(0, len + op.skip);
            // len - start is non-negative and small, so this cannot overflow even for a huge limit.
            end = start + Math.min(op.limit, len - start);
        }

        BsonArray result = new BsonArray();
        for (int i = start; i < end; i++) {
            result.add(array.get(i));
        }
        return result;
    }

    /**
     * Includes a sliced array reached through a dot-notation path. Shares the traversal of
     * {@link #includeNestedField} but, at the leaf, slices an array value and passes a non-array
     * value through unchanged.
     */
    private static void includeSlicedField(BsonDocument source, BsonDocument target, String[] parts, int index, SliceOp op) {
        includeProjectedField(source, target, parts, index, (value, leafTarget, key) ->
                leafTarget.put(key, value.isArray() ? sliceArray(value.asArray(), op) : value));
    }

    /**
     * Replaces a sliced array reached through a dot-notation path while cloning intermediate
     * documents and arrays, so the source document is never mutated. Mirrors
     * {@link #removeNestedFieldRecursive}; a non-array or absent leaf is left untouched.
     */
    private static void replaceSlicedField(BsonDocument current, String[] parts, int index, SliceOp op) {
        String part = parts[index];
        if (!current.containsKey(part)) {
            return;
        }

        BsonValue value = current.get(part);

        if (index == parts.length - 1) {
            if (value.isArray()) {
                current.put(part, sliceArray(value.asArray(), op));
            }
            return;
        }

        if (value.isDocument()) {
            BsonDocument cloned = new BsonDocument();
            cloned.putAll(value.asDocument());
            current.put(part, cloned);
            replaceSlicedField(cloned, parts, index + 1, op);
        } else if (value.isArray()) {
            BsonArray clonedArray = new BsonArray();
            for (BsonValue element : value.asArray()) {
                if (element.isDocument()) {
                    BsonDocument clonedElement = new BsonDocument();
                    clonedElement.putAll(element.asDocument());
                    replaceSlicedField(clonedElement, parts, index + 1, op);
                    clonedArray.add(clonedElement);
                } else {
                    clonedArray.add(element);
                }
            }
            current.put(part, clonedArray);
        }
    }

    /**
     * Applies a projection field's leaf operation: store the value verbatim for a plain inclusion,
     * or store the sliced array for a {@code $slice}.
     */
    @FunctionalInterface
    private interface LeafProjector {
        void project(BsonValue sourceValue, BsonDocument target, String key);
    }

    private record SliceOp(String path, int skip, Integer limit) {
    }

    private record ParsedProjection(
            boolean inclusionMode,
            boolean excludeId,
            Set<String> fields,
            String positionalArrayPath,
            List<SliceOp> slices
    ) {
    }
}
