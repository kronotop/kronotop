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
import com.kronotop.bucket.NumericWidening;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.SelectorMatcher;
import org.bson.BsonArray;
import org.bson.BsonDocument;
import org.bson.BsonType;
import org.bson.BsonValue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Utility class for finding array indices that match query conditions for the $ positional operator.
 * When an update uses the $ operator (e.g., "items.$.price"), this class determines which array
 * element matched the query condition.
 */
public final class PositionalMatchFinder {

    private PositionalMatchFinder() {
    }

    /**
     * Finds the first matching array index for fields using the $ operator.
     * Throws if the array field is not referenced in the query.
     *
     * @param bsonDoc          the document to search
     * @param parsedQuery      pre-parsed BqlExpr
     * @param positionalFields array paths using $ operator (e.g., ["items", "orders.items"])
     * @return map of an array field path to matched index
     * @throws IllegalArgumentException if a positional field is not referenced in the query
     * @throws IllegalStateException    if an array is empty or does not exist
     */
    public static Map<String, Integer> findMatchedPositions(
            BsonDocument bsonDoc,
            BqlExpr parsedQuery,
            Set<String> positionalFields) {
        return findMatchedPositions(bsonDoc, parsedQuery, positionalFields, -1);
    }

    /**
     * Finds the first matching array index for fields using the $ operator.
     * When {@code defaultOnMissing} is non-negative and the array field is not referenced
     * in the query, uses that index as the default instead of throwing.
     *
     * @param bsonDoc          the document to search
     * @param parsedQuery      pre-parsed BqlExpr
     * @param positionalFields array paths using $ operator (e.g., ["items", "orders.items"])
     * @param defaultOnMissing index to use when the array is not referenced in the query;
     *                         negative values cause an {@link IllegalArgumentException} instead
     * @return map of an array field path to matched index
     * @throws IllegalArgumentException if a positional field is not referenced in the query and defaultOnMissing is negative
     * @throws IllegalStateException    if an array is empty or does not exist
     */
    public static Map<String, Integer> findMatchedPositions(
            BsonDocument bsonDoc,
            BqlExpr parsedQuery,
            Set<String> positionalFields,
            int defaultOnMissing) {

        if (positionalFields.isEmpty()) {
            return Map.of();
        }

        Map<String, Integer> result = new HashMap<>();

        for (String arrayPath : positionalFields) {
            // Find predicates that reference this array
            Set<ArrayPredicate> predicates = extractArrayPredicates(parsedQuery, arrayPath);

            if (predicates.isEmpty()) {
                if (defaultOnMissing < 0) {
                    throw new IllegalArgumentException(
                            "Positional operator $ requires array field '" + arrayPath + "' in query");
                }
                result.put(arrayPath, defaultOnMissing);
                continue;
            }

            // Get the array from the document
            BsonValue arrayValue = SelectorMatcher.match(arrayPath, bsonDoc);
            if (arrayValue == null || !arrayValue.isArray()) {
                throw new IllegalStateException(
                        "Field '" + arrayPath + "' is not an array or does not exist");
            }

            BsonArray array = arrayValue.asArray();
            if (array.isEmpty()) {
                throw new IllegalStateException(
                        "Array '" + arrayPath + "' is empty - no element to update");
            }

            // Find the first matching element
            int matchedIndex = findFirstMatchingIndex(array, predicates, arrayPath);
            if (matchedIndex < 0) {
                continue;
            }

            result.put(arrayPath, matchedIndex);
        }

        return result;
    }

    /**
     * Finds the first matching array index for fields using the $ operator.
     * Deserializes the ByteBuffer and delegates to {@link #findMatchedPositions(BsonDocument, BqlExpr, Set)}.
     *
     * @param document         the raw BSON document
     * @param parsedQuery      pre-parsed BqlExpr
     * @param positionalFields array paths using $ operator (e.g., ["items", "orders.items"])
     * @return map of an array field path to matched index
     * @throws IllegalStateException if an array is empty or no element matches
     */
    public static Map<String, Integer> findMatchedPositions(
            ByteBuffer document,
            BqlExpr parsedQuery,
            Set<String> positionalFields) {
        document.rewind();
        BsonDocument bsonDoc = BSONUtil.fromBson(document);
        return findMatchedPositions(bsonDoc, parsedQuery, positionalFields, -1);
    }

    /**
     * Finds the first matching array index for fields using the $ operator.
     * Deserializes the ByteBuffer and delegates to {@link #findMatchedPositions(BsonDocument, BqlExpr, Set, int)}.
     *
     * @param document         the raw BSON document
     * @param parsedQuery      pre-parsed BqlExpr
     * @param positionalFields array paths using $ operator (e.g., ["items", "orders.items"])
     * @param defaultOnMissing index to use when the array is not referenced in the query;
     *                         negative values cause an {@link IllegalArgumentException} instead
     * @return map of an array field path to matched index
     * @throws IllegalStateException if an array is empty or no element matches
     */
    public static Map<String, Integer> findMatchedPositions(
            ByteBuffer document,
            BqlExpr parsedQuery,
            Set<String> positionalFields,
            int defaultOnMissing) {
        document.rewind();
        BsonDocument bsonDoc = BSONUtil.fromBson(document);
        return findMatchedPositions(bsonDoc, parsedQuery, positionalFields, defaultOnMissing);
    }

    /**
     * Extracts predicates from the query that reference the given array path.
     */
    private static Set<ArrayPredicate> extractArrayPredicates(BqlExpr expr, String arrayPath) {
        Set<ArrayPredicate> predicates = new HashSet<>();
        collectPredicates(expr, arrayPath, predicates);
        return predicates;
    }

    private static void collectPredicates(BqlExpr expr, String arrayPath, Set<ArrayPredicate> predicates) {
        switch (expr) {
            case BqlElemMatch elemMatch -> {
                if (elemMatch.selector().equals(arrayPath)) {
                    predicates.add(new ElemMatchPredicate(elemMatch.expr()));
                }
            }
            case BqlEq eq -> addScalarPredicate(eq.selector(), eq.value(), arrayPath, predicates);
            case BqlGt gt -> addComparisonPredicate(gt.selector(), gt.value(), arrayPath, predicates, ComparisonOp.GT);
            case BqlGte gte ->
                    addComparisonPredicate(gte.selector(), gte.value(), arrayPath, predicates, ComparisonOp.GTE);
            case BqlLt lt -> addComparisonPredicate(lt.selector(), lt.value(), arrayPath, predicates, ComparisonOp.LT);
            case BqlLte lte ->
                    addComparisonPredicate(lte.selector(), lte.value(), arrayPath, predicates, ComparisonOp.LTE);
            case BqlNe ne -> addComparisonPredicate(ne.selector(), ne.value(), arrayPath, predicates, ComparisonOp.NE);
            case BqlIn in -> addInPredicate(in.selector(), in.values(), arrayPath, predicates);
            case BqlNin nin -> addNinPredicate(nin.selector(), nin.values(), arrayPath, predicates);
            case BqlExists exists -> addExistsPredicate(exists.selector(), exists.exists(), arrayPath, predicates);
            case BqlRegex regex -> addRegexPredicate(regex.selector(), regex.value(), arrayPath, predicates);
            case BqlAnd and -> {
                for (BqlExpr child : and.children()) {
                    collectPredicates(child, arrayPath, predicates);
                }
            }
            case BqlOr or -> {
                // For $or, we need to match if ANY branch matches
                List<Set<ArrayPredicate>> orBranches = new ArrayList<>();
                for (BqlExpr child : or.children()) {
                    Set<ArrayPredicate> branchPredicates = new HashSet<>();
                    collectPredicates(child, arrayPath, branchPredicates);
                    if (!branchPredicates.isEmpty()) {
                        orBranches.add(branchPredicates);
                    }
                }
                if (!orBranches.isEmpty()) {
                    predicates.add(new OrPredicate(orBranches));
                }
            }
            case BqlAll all -> addInPredicate(all.selector(), all.values(), arrayPath, predicates);
            case BqlSize ignored -> {
                // $size checks array length, not individual elements.
                // Cannot contribute to positional matching.
            }
            case BqlNot ignored -> {
                // $not at top level doesn't contribute to positional matching.
                // Negated conditions don't identify which specific element to update.
            }
            default -> {
                // Other expressions don't contribute to positional matching
            }
        }
    }

    private static void addScalarPredicate(String selector, BqlValue value, String arrayPath, Set<ArrayPredicate> predicates) {
        if (selector.equals(arrayPath)) {
            predicates.add(new ScalarPredicate("", value, ComparisonOp.EQ));
        } else {
            String prefix = arrayPath + ".";
            if (selector.startsWith(prefix)) {
                String subPath = selector.substring(prefix.length());
                predicates.add(new ScalarPredicate(subPath, value, ComparisonOp.EQ));
            }
        }
    }

    private static void addComparisonPredicate(String selector, BqlValue value, String arrayPath,
                                               Set<ArrayPredicate> predicates, ComparisonOp op) {
        if (selector.equals(arrayPath)) {
            predicates.add(new ScalarPredicate("", value, op));
        } else {
            String prefix = arrayPath + ".";
            if (selector.startsWith(prefix)) {
                String subPath = selector.substring(prefix.length());
                predicates.add(new ScalarPredicate(subPath, value, op));
            }
        }
    }

    private static void addInPredicate(String selector, List<BqlValue> values, String arrayPath, Set<ArrayPredicate> predicates) {
        if (selector.equals(arrayPath)) {
            predicates.add(new InPredicate("", values));
        } else {
            String prefix = arrayPath + ".";
            if (selector.startsWith(prefix)) {
                String subPath = selector.substring(prefix.length());
                predicates.add(new InPredicate(subPath, values));
            }
        }
    }

    private static void addNinPredicate(String selector, List<BqlValue> values, String arrayPath, Set<ArrayPredicate> predicates) {
        if (selector.equals(arrayPath)) {
            predicates.add(new NinPredicate("", values));
        } else {
            String prefix = arrayPath + ".";
            if (selector.startsWith(prefix)) {
                String subPath = selector.substring(prefix.length());
                predicates.add(new NinPredicate(subPath, values));
            }
        }
    }

    private static void addExistsPredicate(String selector, boolean shouldExist, String arrayPath, Set<ArrayPredicate> predicates) {
        if (selector.equals(arrayPath)) {
            predicates.add(new ExistsPredicate("", shouldExist));
        } else {
            String prefix = arrayPath + ".";
            if (selector.startsWith(prefix)) {
                String subPath = selector.substring(prefix.length());
                predicates.add(new ExistsPredicate(subPath, shouldExist));
            }
        }
    }

    private static void addRegexPredicate(String selector, RegexVal value, String arrayPath, Set<ArrayPredicate> predicates) {
        if (selector.equals(arrayPath)) {
            predicates.add(new RegexPredicate("", value));
        } else {
            String prefix = arrayPath + ".";
            if (selector.startsWith(prefix)) {
                String subPath = selector.substring(prefix.length());
                predicates.add(new RegexPredicate(subPath, value));
            }
        }
    }

    /**
     * Finds the first array element that matches all predicates.
     */
    private static int findFirstMatchingIndex(BsonArray array, Set<ArrayPredicate> predicates, String arrayPath) {
        for (int i = 0; i < array.size(); i++) {
            BsonValue element = array.get(i);
            if (allPredicatesMatch(element, predicates)) {
                return i;
            }
        }
        return -1;
    }

    private static boolean allPredicatesMatch(BsonValue element, Set<ArrayPredicate> predicates) {
        for (ArrayPredicate predicate : predicates) {
            if (!predicate.matches(element)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Matches a regex against a value, consistent with PredicateEvaluator semantics: only strings
     * match (no type coercion), and an array matches if any of its string elements matches.
     */
    private static boolean regexMatches(BsonValue actual, RegexVal regex) {
        if (actual.isString()) {
            return regex.compiled().matcher(actual.asString().getValue()).find();
        }
        if (actual.isArray()) {
            for (BsonValue element : actual.asArray()) {
                if (element.isString() && regex.compiled().matcher(element.asString().getValue()).find()) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Matches a single value from $in/$nin/$all against one list element. A regex element is a matcher
     * applied to the value; any other element uses equality.
     */
    private static boolean listElementMatches(BsonValue actual, BqlValue expected) {
        if (expected instanceof RegexVal regex) {
            return regexMatches(actual, regex);
        }
        return compareValues(actual, expected, ComparisonOp.EQ);
    }

    private static boolean compareValues(BsonValue actual, BqlValue expected, ComparisonOp op) {
        BqlValue actualBql = BSONUtil.convertBsonValueToBqlValue(actual);
        if (actualBql == null) {
            return op == ComparisonOp.NE;
        }

        int cmp = compareBqlValues(actualBql, expected);
        if (cmp == Integer.MIN_VALUE) {
            // Type mismatch
            return op == ComparisonOp.NE;
        }

        return switch (op) {
            case EQ -> cmp == 0;
            case NE -> cmp != 0;
            case GT -> cmp > 0;
            case GTE -> cmp >= 0;
            case LT -> cmp < 0;
            case LTE -> cmp <= 0;
        };
    }

    /**
     * Compares two BqlValue objects. Returns Integer.MIN_VALUE for type mismatch.
     */
    private static int compareBqlValues(BqlValue actual, BqlValue expected) {
        if (actual.getClass() == expected.getClass()) {
            return switch (actual) {
                case StringVal(String a) -> a.compareTo(((StringVal) expected).value());
                case Int32Val(int a) -> Integer.compare(a, ((Int32Val) expected).value());
                case Int64Val(long a) -> Long.compare(a, ((Int64Val) expected).value());
                case DoubleVal(double a) -> Double.compare(a, ((DoubleVal) expected).value());
                case Decimal128Val(BigDecimal a) -> a.compareTo(((Decimal128Val) expected).value());
                case BooleanVal(boolean a) -> Boolean.compare(a, ((BooleanVal) expected).value());
                case DateTimeVal(long a) -> Long.compare(a, ((DateTimeVal) expected).value());
                case TimestampVal(long a) -> Long.compare(a, ((TimestampVal) expected).value());
                case NullVal ignored -> 0;
                default -> Integer.MIN_VALUE;
            };
        }

        return numericBqlComparison(actual, expected);
    }

    @SuppressWarnings("unchecked")
    private static int numericBqlComparison(BqlValue actual, BqlValue expected) {
        BsonType t1 = bqlValueToBsonType(actual);
        BsonType t2 = bqlValueToBsonType(expected);
        if (t1 == null || t2 == null) {
            return Integer.MIN_VALUE;
        }
        BsonType common = NumericWidening.commonType(t1, t2);
        if (common == null) {
            return Integer.MIN_VALUE;
        }
        Object val1 = extractBqlNumericValue(actual);
        Object val2 = extractBqlNumericValue(expected);
        Comparable<Object> p1 = (Comparable<Object>) NumericWidening.promoteToComparable(val1, t1, common);
        Comparable<Object> p2 = (Comparable<Object>) NumericWidening.promoteToComparable(val2, t2, common);
        if (p1 == null || p2 == null) {
            return Integer.MIN_VALUE;
        }
        return p1.compareTo(p2);
    }

    private static BsonType bqlValueToBsonType(BqlValue value) {
        return switch (value) {
            case Int32Val ignored -> BsonType.INT32;
            case Int64Val ignored -> BsonType.INT64;
            case DoubleVal ignored -> BsonType.DOUBLE;
            case Decimal128Val ignored -> BsonType.DECIMAL128;
            default -> null;
        };
    }

    private static Object extractBqlNumericValue(BqlValue value) {
        return switch (value) {
            case Int32Val(int v) -> v;
            case Int64Val(long v) -> v;
            case DoubleVal(double v) -> v;
            case Decimal128Val(BigDecimal v) -> v;
            default -> null;
        };
    }

    private enum ComparisonOp {
        EQ, NE, GT, GTE, LT, LTE
    }

    private sealed interface ArrayPredicate permits ScalarPredicate, InPredicate, NinPredicate, ExistsPredicate, ElemMatchPredicate, OrPredicate, RegexPredicate {
        boolean matches(BsonValue element);
    }

    private record ScalarPredicate(String subPath, BqlValue expectedValue, ComparisonOp op) implements ArrayPredicate {
        @Override
        public boolean matches(BsonValue element) {
            BsonValue actualValue;
            if (subPath.isEmpty() || subPath.equals("$")) {
                actualValue = element;
            } else if (element.isDocument()) {
                actualValue = SelectorMatcher.match(subPath, element.asDocument());
            } else {
                return false;
            }

            if (actualValue == null) {
                return op == ComparisonOp.NE;
            }

            return compareValues(actualValue, expectedValue, op);
        }
    }

    private record RegexPredicate(String subPath, RegexVal regex) implements ArrayPredicate {
        @Override
        public boolean matches(BsonValue element) {
            BsonValue actualValue;
            if (subPath.isEmpty() || subPath.equals("$")) {
                actualValue = element;
            } else if (element.isDocument()) {
                actualValue = SelectorMatcher.match(subPath, element.asDocument());
            } else {
                return false;
            }

            if (actualValue == null) {
                return false;
            }

            return regexMatches(actualValue, regex);
        }
    }

    private record InPredicate(String subPath, List<BqlValue> expectedValues) implements ArrayPredicate {
        @Override
        public boolean matches(BsonValue element) {
            BsonValue actualValue;
            if (subPath.isEmpty() || subPath.equals("$")) {
                actualValue = element;
            } else if (element.isDocument()) {
                actualValue = SelectorMatcher.match(subPath, element.asDocument());
            } else {
                return false;
            }

            if (actualValue == null) {
                return false;
            }

            for (BqlValue expected : expectedValues) {
                if (listElementMatches(actualValue, expected)) {
                    return true;
                }
            }
            return false;
        }
    }

    private record NinPredicate(String subPath, List<BqlValue> excludedValues) implements ArrayPredicate {
        @Override
        public boolean matches(BsonValue element) {
            BsonValue actualValue;
            if (subPath.isEmpty() || subPath.equals("$")) {
                actualValue = element;
            } else if (element.isDocument()) {
                actualValue = SelectorMatcher.match(subPath, element.asDocument());
            } else {
                return true; // Field doesn't exist, so it's not in the excluded list
            }

            if (actualValue == null) {
                return true; // Null value is not in the excluded list
            }

            for (BqlValue excluded : excludedValues) {
                if (listElementMatches(actualValue, excluded)) {
                    return false;
                }
            }
            return true;
        }
    }

    private record ExistsPredicate(String subPath, boolean shouldExist) implements ArrayPredicate {
        @Override
        public boolean matches(BsonValue element) {
            if (subPath.isEmpty() || subPath.equals("$")) {
                // The element itself - always exists if we're here
                return shouldExist;
            }

            if (!element.isDocument()) {
                return !shouldExist;
            }

            BsonValue fieldValue = SelectorMatcher.match(subPath, element.asDocument());
            boolean exists = fieldValue != null;
            return exists == shouldExist;
        }
    }

    private record OrPredicate(List<Set<ArrayPredicate>> orBranches) implements ArrayPredicate {
        @Override
        public boolean matches(BsonValue element) {
            // Returns true if ANY branch matches (all predicates in that branch match)
            for (Set<ArrayPredicate> branch : orBranches) {
                if (allPredicatesMatch(element, branch)) {
                    return true;
                }
            }
            return false;
        }
    }

    private record ElemMatchPredicate(BqlExpr innerExpr) implements ArrayPredicate {
        @Override
        public boolean matches(BsonValue element) {
            return evaluateExpr(element, innerExpr);
        }

        private boolean evaluateExpr(BsonValue element, BqlExpr expr) {
            return switch (expr) {
                case BqlEq eq -> matchField(element, eq.selector(), eq.value(), ComparisonOp.EQ);
                case BqlGt gt -> matchField(element, gt.selector(), gt.value(), ComparisonOp.GT);
                case BqlGte gte -> matchField(element, gte.selector(), gte.value(), ComparisonOp.GTE);
                case BqlLt lt -> matchField(element, lt.selector(), lt.value(), ComparisonOp.LT);
                case BqlLte lte -> matchField(element, lte.selector(), lte.value(), ComparisonOp.LTE);
                case BqlNe ne -> matchField(element, ne.selector(), ne.value(), ComparisonOp.NE);
                case BqlIn in -> matchInField(element, in.selector(), in.values());
                case BqlNin nin -> matchNinField(element, nin.selector(), nin.values());
                case BqlExists exists -> matchExistsField(element, exists.selector(), exists.exists());
                case BqlRegex regex -> matchRegexField(element, regex.selector(), regex.value());
                case BqlNot not -> !evaluateExpr(element, not.expr());
                case BqlElemMatch nested -> evaluateNestedElemMatch(element, nested);
                case BqlAnd and -> {
                    for (BqlExpr child : and.children()) {
                        if (!evaluateExpr(element, child)) {
                            yield false;
                        }
                    }
                    yield true;
                }
                case BqlOr or -> {
                    for (BqlExpr child : or.children()) {
                        if (evaluateExpr(element, child)) {
                            yield true;
                        }
                    }
                    yield false;
                }
                default -> false;
            };
        }

        private boolean matchField(BsonValue element, String selector, BqlValue expected, ComparisonOp op) {
            BsonValue actual;
            if (selector.isEmpty()) {
                actual = element;
            } else if (element.isDocument()) {
                actual = SelectorMatcher.match(selector, element.asDocument());
            } else {
                return false;
            }

            if (actual == null) {
                return op == ComparisonOp.NE;
            }

            return compareValues(actual, expected, op);
        }

        private boolean matchInField(BsonValue element, String selector, List<BqlValue> values) {
            BsonValue actual;
            if (selector.isEmpty()) {
                actual = element;
            } else if (element.isDocument()) {
                actual = SelectorMatcher.match(selector, element.asDocument());
            } else {
                return false;
            }

            if (actual == null) {
                return false;
            }

            for (BqlValue value : values) {
                if (listElementMatches(actual, value)) {
                    return true;
                }
            }
            return false;
        }

        private boolean matchNinField(BsonValue element, String selector, List<BqlValue> values) {
            BsonValue actual;
            if (selector.isEmpty()) {
                actual = element;
            } else if (element.isDocument()) {
                actual = SelectorMatcher.match(selector, element.asDocument());
            } else {
                return true; // Field doesn't exist, so it's not in the excluded list
            }

            if (actual == null) {
                return true;
            }

            for (BqlValue value : values) {
                if (listElementMatches(actual, value)) {
                    return false;
                }
            }
            return true;
        }

        private boolean matchExistsField(BsonValue element, String selector, boolean shouldExist) {
            if (selector.isEmpty()) {
                return shouldExist;
            }

            if (!element.isDocument()) {
                return !shouldExist;
            }

            BsonValue fieldValue = SelectorMatcher.match(selector, element.asDocument());
            boolean exists = fieldValue != null;
            return exists == shouldExist;
        }

        private boolean matchRegexField(BsonValue element, String selector, RegexVal regex) {
            BsonValue actual;
            if (selector.isEmpty()) {
                actual = element;
            } else if (element.isDocument()) {
                actual = SelectorMatcher.match(selector, element.asDocument());
            } else {
                return false;
            }

            if (actual == null) {
                return false;
            }

            return regexMatches(actual, regex);
        }

        private boolean evaluateNestedElemMatch(BsonValue element, BqlElemMatch nested) {
            BsonValue arrayValue;
            if (nested.selector().isEmpty()) {
                arrayValue = element;
            } else if (element.isDocument()) {
                arrayValue = SelectorMatcher.match(nested.selector(), element.asDocument());
            } else {
                return false;
            }

            if (arrayValue == null || !arrayValue.isArray()) {
                return false;
            }

            BsonArray array = arrayValue.asArray();
            ElemMatchPredicate nestedPredicate = new ElemMatchPredicate(nested.expr());
            for (BsonValue arrayElement : array) {
                if (nestedPredicate.matches(arrayElement)) {
                    return true;
                }
            }
            return false;
        }
    }
}
