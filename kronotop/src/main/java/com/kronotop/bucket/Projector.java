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

        for (Map.Entry<String, BsonValue> entry : spec.entrySet()) {
            String field = entry.getKey();
            int value = entry.getValue().asNumber().intValue();

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

        boolean inclusionMode = hasInclusion || (!hasExclusion && !excludeId);
        return new ParsedProjection(inclusionMode, excludeId, fields, positionalArrayPath);
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
        String part = parts[index];
        if (!source.containsKey(part)) {
            return;
        }

        BsonValue sourceValue = source.get(part);

        if (index == parts.length - 1) {
            target.put(part, sourceValue);
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
            includeNestedField(sourceValue.asDocument(), targetNested, parts, index + 1);
        } else if (sourceValue.isArray()) {
            BsonArray sourceArray = sourceValue.asArray();
            BsonArray targetArray;
            if (target.containsKey(part) && target.get(part).isArray()) {
                targetArray = target.get(part).asArray();
            } else {
                targetArray = new BsonArray();
                target.put(part, targetArray);
            }

            for (int i = 0; i < sourceArray.size(); i++) {
                BsonValue element = sourceArray.get(i);
                if (element.isDocument()) {
                    BsonDocument targetElement;
                    if (i < targetArray.size() && targetArray.get(i).isDocument()) {
                        targetElement = targetArray.get(i).asDocument();
                    } else {
                        targetElement = new BsonDocument();
                        if (i < targetArray.size()) {
                            targetArray.set(i, targetElement);
                        } else {
                            targetArray.add(targetElement);
                        }
                    }
                    includeNestedField(element.asDocument(), targetElement, parts, index + 1);
                }
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

    private record ParsedProjection(
            boolean inclusionMode,
            boolean excludeId,
            Set<String> fields,
            String positionalArrayPath
    ) {
    }
}
