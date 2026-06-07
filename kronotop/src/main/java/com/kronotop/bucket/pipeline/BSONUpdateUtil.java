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

import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.bql.ast.BqlValue;
import com.kronotop.bucket.index.SelectorMatcher;
import com.kronotop.bucket.planner.Operator;
import com.kronotop.internal.StringUtil;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonNull;
import org.bson.BsonValue;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class BSONUpdateUtil {
    // Pattern to match $[identifier] where identifier is alphanumeric
    private static final Pattern ARRAY_FILTER_PATTERN = Pattern.compile("^\\$\\[([a-zA-Z_][a-zA-Z0-9_]*)]$");

    private BSONUpdateUtil() {
        // Utility class
    }

    /**
     * Validates and returns the matched index for the $ positional operator.
     *
     * @param pathSegments     the path segments array
     * @param currentIndex     the current index in pathSegments (pointing to "$")
     * @param arraySize        the size of the array being operated on
     * @param matchedPositions map of array paths to their matched indices
     * @return the validated matched index
     * @throws IllegalArgumentException if the array path is not in matchedPositions
     * @throws IllegalStateException    if the matched index is negative or out of bounds
     */
    private static int getValidatedMatchedIndex(String[] pathSegments, int currentIndex, int arraySize,
                                                Map<String, Integer> matchedPositions) {
        String arrayPath = buildPathUpTo(pathSegments, currentIndex - 1);
        Integer matchedIndex = matchedPositions.get(arrayPath);
        if (matchedIndex == null) {
            throw new IllegalArgumentException(
                    "Positional $ requires array field '" + arrayPath + "' in query");
        }
        if (matchedIndex < 0) {
            throw new IllegalStateException(
                    "Matched index cannot be negative for array " + arrayPath);
        }
        if (matchedIndex >= arraySize) {
            throw new IllegalStateException(
                    "Matched index " + matchedIndex + " out of bounds for array " + arrayPath);
        }
        return matchedIndex;
    }

    /**
     * Validates a positional operator segment and throws appropriate errors for invalid or unsupported syntax.
     *
     * @param segment      the path segment to validate
     * @param fullPath     the full path for error messages
     * @param arrayFilters the map of array filters, may be null or empty
     * @throws IllegalArgumentException if the segment is a malformed or unsupported positional operator
     */
    private static void validatePositionalOperator(String segment, String fullPath, Map<String, ArrayFilter> arrayFilters) {
        if (!segment.startsWith("$[")) {
            return;
        }

        if (segment.equals("$[]")) {
            return; // Valid positional all operator
        }

        // Check for array filter pattern $[identifier]
        Matcher matcher = ARRAY_FILTER_PATTERN.matcher(segment);
        if (matcher.matches()) {
            String identifier = matcher.group(1);
            if (arrayFilters == null || !arrayFilters.containsKey(identifier)) {
                throw new IllegalArgumentException(
                        String.format("No array filter found for identifier '%s' in path '%s'", identifier, fullPath));
            }
            return; // Valid filtered positional operator
        }

        // Malformed positional operator
        throw new IllegalArgumentException(
                String.format("Invalid positional operator '%s' in path '%s'", segment, fullPath));
    }

    /**
     * Checks whether a path segment is a positional operator ({@code $}, {@code $[]} or {@code $[identifier]}).
     *
     * @param segment the path segment
     * @return true if the segment is a positional operator
     */
    private static boolean isPositionalSegment(String segment) {
        return segment.equals("$") || segment.startsWith("$[");
    }

    /**
     * Extracts the filter identifier from a segment like $[identifier].
     *
     * @param segment the path segment
     * @return the identifier if a segment is $[identifier], null otherwise
     */
    private static String extractFilterIdentifier(String segment) {
        Matcher matcher = ARRAY_FILTER_PATTERN.matcher(segment);
        if (matcher.matches()) {
            return matcher.group(1);
        }
        return null;
    }

    /**
     * Checks if a BsonValue element matches the given array filter.
     * Supports nested paths in filter identifiers (e.g., "expensive.price" extracts element.price).
     */
    private static boolean matchesFilter(BsonValue element, ArrayFilter filter) {
        BsonValue valueToCompare = element;
        boolean pathExists = true;

        // If the filter identifier contains a dot (nested path) and the element is a document,
        // extract the nested value using SelectorMatcher
        if (element.isDocument()) {
            String identifier = filter.identifier();
            int dotIndex = identifier.indexOf('.');
            if (dotIndex > 0) {
                // identifier like "expensive.price" means compare element.price
                String fieldPath = identifier.substring(dotIndex + 1);
                valueToCompare = SelectorMatcher.match(fieldPath, element.asDocument());
                if (valueToCompare == null) {
                    pathExists = false;
                }
            }
        }

        // Handle EXISTS operator specially
        if (filter.op() == Operator.EXISTS) {
            Object operand = filter.operand();
            boolean existsValue = operand instanceof Boolean b ? b : false;
            return existsValue == pathExists;
        }

        // Path doesn't exist - can't compare
        if (!pathExists) {
            return false;
        }

        Object operand = filter.operand();

        // Handle SIZE operator specially - pass raw Integer
        if (filter.op() == Operator.SIZE) {
            if (operand instanceof Integer intOperand) {
                return PredicateEvaluator.evaluateBsonValue(valueToCompare, filter.op(), intOperand);
            }
            return false;
        }

        // For IN/NIN/ALL operators, convert List operand to List<BqlValue>
        if (operand instanceof List<?> list) {
            List<BqlValue> bqlList = new ArrayList<>();
            for (Object item : list) {
                bqlList.add(BSONUtil.convertObjectToBqlValue(item));
            }
            return PredicateEvaluator.evaluateBsonValue(valueToCompare, filter.op(), bqlList);
        }

        BqlValue bqlOperand = BSONUtil.convertObjectToBqlValue(operand);
        return PredicateEvaluator.evaluateBsonValue(valueToCompare, filter.op(), bqlOperand);
    }

    /**
     * Normalizes a selector path by removing positional operators such as {@code $},
     * {@code $[]}, and {@code $[identifier]}.
     * <p>
     * This is required to match index field names which do not contain positional
     * operators. For example:
     * <ul>
     * <li>"tags.$[].name" -> "tags.name"</li>
     * <li>"tags.$[elem].name" -> "tags.name"</li>
     * <li>"items.$.price" -> "items.price"</li>
     * </ul>
     *
     * @param selector the raw selector path containing positional operators
     * @return the normalized path suitable for index matching, or the original
     * selector if no operators are found.
     */
    static String normalizeSelector(String selector) {
        // Fast path: return immediately if no positional operator symbol is present
        if (selector == null || selector.indexOf('$') == -1) {
            return selector;
        }

        int len = selector.length();
        StringBuilder sb = new StringBuilder(len);

        for (int i = 0; i < len; i++) {
            char c = selector.charAt(i);

            // Detect the start of a positional operator sequence: ".$"
            if (c == '.' && i + 1 < len && selector.charAt(i + 1) == '$') {
                i++; // Skip '.'
                i++; // Skip '$'

                // Check for the array filter pattern: "$[identifier]" or "$[]"
                if (i < len && selector.charAt(i) == '[') {
                    // Skip everything until the closing bracket ']'
                    while (i < len && selector.charAt(i) != ']') {
                        i++;
                    }
                }
                // After skipping the operator, we 'continue' to avoid appending
                // the last character of the operator to the result.
                continue;
            }
            sb.append(c);
        }

        return sb.toString();
    }

    /**
     * Checks if a field selector contains a positional operator ($, $[] or $[identifier]).
     */
    private static boolean hasPositionalOperator(String selector) {
        if (selector.contains("$[")) {
            return true;
        }
        // Check for standalone $ operator
        String[] segments = selector.split("\\.");
        for (String segment : segments) {
            if (segment.equals("$")) {
                return true;
            }
        }
        return false;
    }

    /**
     * Updates a BSON document by applying the set operations from UpdateOptions.
     * This method traverses the setOps map and updates or adds the corresponding fields in the document.
     *
     * @param document     The original BSON document as ByteBuffer
     * @param setOps       Map of field names to new values
     * @param unsetOps     Set of field names to unset
     * @param arrayFilters Map of array filter identifiers to filters, may be null
     * @return DocumentUpdateResult containing the updated document and modified fields
     */
    public static DocumentUpdateResult applyUpdateOperations(ByteBuffer document, Map<String, BsonValue> setOps, Set<String> unsetOps, Map<String, ArrayFilter> arrayFilters) {
        return applyUpdateOperations(document, setOps, unsetOps, arrayFilters, Map.of());
    }

    /**
     * Updates a BSON document by applying the set operations from UpdateOptions.
     * This overload is for backward compatibility without array_filters.
     *
     * @param document The original BSON document as ByteBuffer
     * @param setOps   Map of field names to new values
     * @param unsetOps Set of field names to unset
     * @return DocumentUpdateResult containing the updated document and modified fields
     */
    public static DocumentUpdateResult applyUpdateOperations(ByteBuffer document, Map<String, BsonValue> setOps, Set<String> unsetOps) {
        return applyUpdateOperations(document, setOps, unsetOps, null, Map.of());
    }

    /**
     * Updates a BSON document by applying the set operations from UpdateOptions.
     * This overload supports both array_filters and positional $ operator.
     *
     * @param document         The original BSON document as ByteBuffer
     * @param setOps           Map of field names to new values
     * @param unsetOps         Set of field names to unset
     * @param arrayFilters     Map of array filter identifiers to filters, may be null
     * @param matchedPositions Map of the array path to matched index for $ operator
     * @return DocumentUpdateResult containing the updated document and modified fields
     */
    public static DocumentUpdateResult applyUpdateOperations(
            ByteBuffer document,
            Map<String, BsonValue> setOps,
            Set<String> unsetOps,
            Map<String, ArrayFilter> arrayFilters,
            Map<String, Integer> matchedPositions) {
        if (setOps.isEmpty() && unsetOps.isEmpty()) {
            return new DocumentUpdateResult(document, Map.of(), Set.of(), false);
        }

        document.rewind();
        BsonDocument bsonDoc = BSONUtil.fromBson(document);

        // Track normalized selectors that need actual value extraction after modifications
        Set<String> positionalNormalizedSelectors = new HashSet<>();

        Set<String> droppedSelectors = new HashSet<>();
        for (String selector : unsetOps) {
            if (unsetField(bsonDoc, selector, arrayFilters, matchedPositions)) {
                String normalized = normalizeSelector(selector);
                if (hasPositionalOperator(selector)) {
                    positionalNormalizedSelectors.add(normalized);
                } else {
                    droppedSelectors.add(normalized);
                }
            }
        }

        Map<String, BsonValue> newValues = new HashMap<>();

        // Apply set operations
        for (Map.Entry<String, BsonValue> setOp : setOps.entrySet()) {
            String fieldName = setOp.getKey();
            BsonValue bsonValue = setOp.getValue();

            setField(bsonDoc, fieldName, bsonValue, arrayFilters, matchedPositions);

            String normalizedSelector = normalizeSelector(fieldName);

            if (hasPositionalOperator(fieldName)) {
                positionalNormalizedSelectors.add(normalizedSelector);
            } else {
                newValues.put(normalizedSelector, bsonValue);
            }
        }

        // Extract actual values for positional operations from modified document
        for (String normalizedSelector : positionalNormalizedSelectors) {
            BsonValue actualValue = SelectorMatcher.match(normalizedSelector, bsonDoc);
            if (actualValue != null) {
                newValues.put(normalizedSelector, actualValue);
                droppedSelectors.remove(normalizedSelector);
            } else {
                // Field no longer exists after unset - keep in droppedSelectors
                droppedSelectors.add(normalizedSelector);
            }
        }

        ByteBuffer updatedDocument = BSONUtil.toByteBuffer(bsonDoc);
        boolean changed = !bsonEquals(document, updatedDocument);
        return new DocumentUpdateResult(updatedDocument, newValues, droppedSelectors, changed);
    }

    /**
     * Compares two BSON documents for byte-identity using their self-describing length prefix.
     * Reads are absolute-indexed so neither buffer's position or limit is modified.
     */
    private static boolean bsonEquals(ByteBuffer a, ByteBuffer b) {
        int lenA = a.order(ByteOrder.LITTLE_ENDIAN).getInt(0);
        int lenB = b.order(ByteOrder.LITTLE_ENDIAN).getInt(0);
        if (lenA != lenB) {
            return false;
        }
        for (int i = 0; i < lenA; i++) {
            if (a.get(i) != b.get(i)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Unsets a field from a document, supporting dot-path notation for nested fields.
     * When the path traverses an array, the field is removed from each document element in the array.
     *
     * @param doc              the document to modify
     * @param selector         the dot-path selector (e.g., "tags.$[].name" to remove "name" from each element in the "tags" array)
     * @param arrayFilters     the map of array filters, may be null
     * @param matchedPositions map of array path to matched index for $ operator
     * @return true if any field was removed, false otherwise
     */
    private static boolean unsetField(BsonDocument doc, String selector, Map<String, ArrayFilter> arrayFilters, Map<String, Integer> matchedPositions) {
        String[] pathSegments = StringUtil.split(selector);

        if (pathSegments.length == 1) {
            // Root level field
            if (doc.containsKey(selector)) {
                doc.remove(selector);
                return true;
            }
            return false;
        }

        // Navigate to parent, handling arrays
        return unsetNestedField(doc, pathSegments, 0, selector, arrayFilters, matchedPositions);
    }

    private static boolean unsetNestedField(Object current, String[] pathSegments, int index, String fullPath, Map<String, ArrayFilter> arrayFilters, Map<String, Integer> matchedPositions) {
        if (current == null) {
            return false;
        }

        String segment = pathSegments[index];
        boolean isLastSegment = index == pathSegments.length - 1;

        switch (current) {
            case BsonDocument bsonDoc -> {
                if (!bsonDoc.containsKey(segment)) {
                    return false;
                }

                if (isLastSegment) {
                    bsonDoc.remove(segment);
                    return true;
                }

                BsonValue next = bsonDoc.get(segment);
                return unsetNestedField(next, pathSegments, index + 1, fullPath, arrayFilters, matchedPositions);
            }
            case BsonArray bsonArray -> {
                validatePositionalOperator(segment, fullPath, arrayFilters);

                // Handle $ positional operator
                if (segment.equals("$")) {
                    int matchedIndex = getValidatedMatchedIndex(pathSegments, index, bsonArray.size(), matchedPositions);

                    if (isLastSegment) {
                        // $unset with $: set to null to maintain array stability
                        bsonArray.set(matchedIndex, BsonNull.VALUE);
                        return true;
                    }
                    // Continue traversal into matched element
                    return unsetNestedField(bsonArray.get(matchedIndex), pathSegments, index + 1, fullPath, arrayFilters, matchedPositions);
                }

                if (segment.equals("$[]")) {
                    if (isLastSegment) {
                        // $[] is the last segment: clear all elements from the array
                        if (bsonArray.isEmpty()) {
                            return false;
                        }
                        bsonArray.clear();
                        return true;
                    }
                    // Positional all operator: apply to each element
                    boolean anyRemoved = false;
                    for (BsonValue element : bsonArray) {
                        if (unsetNestedField(element, pathSegments, index + 1, fullPath, arrayFilters, matchedPositions)) {
                            anyRemoved = true;
                        }
                    }
                    return anyRemoved;
                }

                // Check for filtered positional operator $[identifier]
                String identifier = extractFilterIdentifier(segment);
                if (identifier != null && arrayFilters != null) {
                    ArrayFilter filter = arrayFilters.get(identifier);
                    if (filter != null) {
                        if (isLastSegment) {
                            // Remove matching elements from the array (reverse iteration to avoid index shifting)
                            boolean anyRemoved = false;
                            for (int i = bsonArray.size() - 1; i >= 0; i--) {
                                if (matchesFilter(bsonArray.get(i), filter)) {
                                    bsonArray.remove(i);
                                    anyRemoved = true;
                                }
                            }
                            return anyRemoved;
                        }
                        // Apply to matching elements only
                        boolean anyRemoved = false;
                        for (BsonValue element : bsonArray) {
                            if (matchesFilter(element, filter)) {
                                if (unsetNestedField(element, pathSegments, index + 1, fullPath, arrayFilters, matchedPositions)) {
                                    anyRemoved = true;
                                }
                            }
                        }
                        return anyRemoved;
                    }
                }

                // Numeric index handling
                Integer arrayIndex = parseArrayIndex(segment);
                if (arrayIndex != null && arrayIndex < bsonArray.size()) {
                    // Numeric index: access a specific element
                    if (isLastSegment) {
                        bsonArray.remove(arrayIndex.intValue());
                        return true;
                    } else {
                        return unsetNestedField(bsonArray.get(arrayIndex), pathSegments, index + 1, fullPath, arrayFilters, matchedPositions);
                    }
                }
                // Non-numeric, non-$[] key: the path doesn't exist
                return false;
            }
            default -> {
                return false;
            }
        }
    }

    /**
     * Sets a field in a document, supporting dot-path notation for nested fields.
     *
     * @param doc              the document to modify
     * @param selector         the dot-path selector (e.g., "items.0.price" to set price in the first array element)
     * @param value            the value to set
     * @param arrayFilters     the map of the array filters, may be null
     * @param matchedPositions map of the array path to matched index for $ operator
     */
    private static void setField(BsonDocument doc, String selector, BsonValue value, Map<String, ArrayFilter> arrayFilters, Map<String, Integer> matchedPositions) {
        String[] pathSegments = StringUtil.split(selector);

        if (pathSegments.length == 1) {
            doc.put(selector, value);
            return;
        }

        setNestedField(doc, pathSegments, 0, value, selector, arrayFilters, matchedPositions);
    }

    private static void setNestedField(Object current, String[] pathSegments, int index, BsonValue value, String fullPath, Map<String, ArrayFilter> arrayFilters, Map<String, Integer> matchedPositions) {
        if (current == null) {
            return;
        }

        String segment = pathSegments[index];
        boolean isLastSegment = index == pathSegments.length - 1;

        switch (current) {
            case BsonDocument bsonDoc -> {
                if (isLastSegment) {
                    bsonDoc.put(segment, value);
                    return;
                }

                String nextSegment = pathSegments[index + 1];
                if (isPositionalSegment(nextSegment)) {
                    // Positional operators target array elements. Validate the segment for
                    // consistent error reporting, then no-op unless the field holds an array.
                    validatePositionalOperator(nextSegment, fullPath, arrayFilters);
                    BsonValue next = bsonDoc.get(segment);
                    if (next == null || !next.isArray()) {
                        return;
                    }
                    setNestedField(next, pathSegments, index + 1, value, fullPath, arrayFilters, matchedPositions);
                    return;
                }

                // Check if the next segment is numeric (array index)
                Integer arrayIndex = parseArrayIndex(nextSegment);

                if (!bsonDoc.containsKey(segment)) {
                    // Create intermediate structure
                    bsonDoc.put(segment, arrayIndex != null ? new BsonArray() : new BsonDocument());
                }

                BsonValue next = bsonDoc.get(segment);
                setNestedField(next, pathSegments, index + 1, value, fullPath, arrayFilters, matchedPositions);
            }
            case BsonArray bsonArray -> {
                validatePositionalOperator(segment, fullPath, arrayFilters);

                // Handle $ positional operator
                if (segment.equals("$")) {
                    int matchedIndex = getValidatedMatchedIndex(pathSegments, index, bsonArray.size(), matchedPositions);

                    if (isLastSegment) {
                        // $set with $: replace the element
                        bsonArray.set(matchedIndex, value);
                    } else {
                        // Continue traversal into matched element
                        BsonValue element = bsonArray.get(matchedIndex);
                        setNestedField(element, pathSegments, index + 1, value, fullPath, arrayFilters, matchedPositions);
                    }
                    return;
                }

                if (segment.equals("$[]")) {
                    // Positional all operator: apply to each element
                    if (isLastSegment) {
                        // Replace all array elements with the value
                        for (int i = 0; i < bsonArray.size(); i++) {
                            bsonArray.set(i, value);
                        }
                    } else {
                        for (BsonValue element : bsonArray) {
                            setNestedField(element, pathSegments, index + 1, value, fullPath, arrayFilters, matchedPositions);
                        }
                    }
                    return;
                }

                // Check for filtered positional operator $[identifier]
                String identifier = extractFilterIdentifier(segment);
                if (identifier != null && arrayFilters != null) {
                    ArrayFilter filter = arrayFilters.get(identifier);
                    if (filter != null) {
                        if (isLastSegment) {
                            // Replace matching array elements with the value
                            for (int i = 0; i < bsonArray.size(); i++) {
                                if (matchesFilter(bsonArray.get(i), filter)) {
                                    bsonArray.set(i, value);
                                }
                            }
                        } else {
                            // Apply to matching elements only
                            for (BsonValue element : bsonArray) {
                                if (matchesFilter(element, filter)) {
                                    setNestedField(element, pathSegments, index + 1, value, fullPath, arrayFilters, matchedPositions);
                                }
                            }
                        }
                        return;
                    }
                }

                // Numeric index handling
                Integer arrayIndex = parseArrayIndex(segment);
                if (arrayIndex != null && arrayIndex < bsonArray.size()) {
                    // Numeric index: access specific array element
                    if (isLastSegment) {
                        bsonArray.set(arrayIndex, value);
                    } else {
                        BsonValue element = bsonArray.get(arrayIndex);
                        setNestedField(element, pathSegments, index + 1, value, fullPath, arrayFilters, matchedPositions);
                    }
                }
                // Non-numeric, non-$[] key: do nothing (path doesn't exist)
            }
            default -> {
            }
        }
    }

    private static Integer parseArrayIndex(String segment) {
        try {
            return Integer.parseInt(segment);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * Builds a path from path segments up to (but not including) the given index.
     */
    private static String buildPathUpTo(String[] pathSegments, int endIndex) {
        if (endIndex < 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i <= endIndex; i++) {
            if (!sb.isEmpty()) {
                sb.append(".");
            }
            sb.append(pathSegments[i]);
        }
        return sb.toString();
    }

    /**
     * Record to hold the result of a BSON document update operation.
     *
     * @param document  The updated BSON document as ByteBuffer
     * @param newValues Map of field names to their new BSON values that were set (both new and updated fields)
     * @param changed   Whether the update operations actually modified the document
     */
    public record DocumentUpdateResult(ByteBuffer document, Map<String, BsonValue> newValues,
                                       Set<String> droppedSelectors, boolean changed) {
    }
}