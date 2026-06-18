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

import com.apple.foundationdb.tuple.Versionstamp;
import com.ibm.icu.text.Collator;
import com.kronotop.KronotopException;
import com.kronotop.bucket.BSONUtil;
import com.kronotop.bucket.NumericWidening;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.SelectorMatcher;
import com.kronotop.bucket.planner.Operator;
import org.bson.BsonArray;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.types.ObjectId;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Evaluates predicates against BSON documents.
 * Provides unified comparison logic for all supported types.
 */
public class PredicateEvaluator {

    // ==================== String Collation Helpers ====================

    private static int compareStrings(String a, String b, Collator collator) {
        return collator != null ? collator.compare(a, b) : a.compareTo(b);
    }

    private static boolean stringsEqual(String a, String b, Collator collator) {
        return collator != null ? collator.equals(a, b) : a.equals(b);
    }

    // ==================== Entry Points ====================

    private static boolean resolveExistsOperand(Object resolvedOperand) {
        if (resolvedOperand instanceof Boolean b) {
            return b;
        } else if (resolvedOperand instanceof BooleanVal(boolean value)) {
            return value;
        }
        throw new KronotopException("EXISTS operator requires boolean operand");
    }

    /**
     * Tests if a document satisfies the conditions specified by a ResidualPredicate.
     *
     * @param filter     the predicate to test
     * @param view       the document view containing virtual fields and content
     * @param parameters the parameter list for resolving Param operands
     * @return true if the document matches the predicate
     */
    public static boolean testResidualPredicate(ResidualPredicate filter, DocumentView view, List<BqlValue> parameters) {
        return testResidualPredicate(filter, view, parameters, null);
    }

    /**
     * Tests if a document satisfies the conditions specified by a ResidualPredicate.
     *
     * @param filter     the predicate to test
     * @param view       the document view containing virtual fields and content
     * @param parameters the parameter list for resolving Param operands
     * @param collator   the collator for locale-aware string comparison, or null for binary
     * @return true if the document matches the predicate
     */
    public static boolean testResidualPredicate(ResidualPredicate filter, DocumentView view, List<BqlValue> parameters, Collator collator) {
        BsonValue bsonValue = SelectorMatcher.match(filter.selector(), view.getContent());

        // Resolve the operand
        Object resolvedOperand = filter.operand().resolveAny(parameters);

        // EXISTS performs a physical field presence check
        if (filter.op() == Operator.EXISTS) {
            return resolveExistsOperand(resolvedOperand) == (bsonValue != null);
        }

        boolean fieldMissingOrNull = (bsonValue == null || bsonValue.isNull());
        boolean operandIsNull = resolvedOperand instanceof NullVal;

        // Null is only meaningful in EQ / NE context
        if (operandIsNull) {
            if (fieldMissingOrNull) {
                return filter.op() == Operator.EQ;
            }
            return filter.op() == Operator.NE;
        }

        // Handle IN/NIN with null field: check if the list contains NullVal
        if (fieldMissingOrNull && resolvedOperand instanceof List<?> list) {
            boolean listContainsNull = list.stream().anyMatch(v -> v instanceof NullVal);
            if (filter.op() == Operator.IN) {
                return listContainsNull;
            }
            if (filter.op() == Operator.NIN) {
                return !listContainsNull;
            }
        }

        // Missing or null field never matches other comparison operators
        if (fieldMissingOrNull) {
            return filter.op() == Operator.NE;
        }

        // Both sides are non-null: evaluate normally
        return evaluateBsonValue(bsonValue, filter.op(), resolvedOperand, collator);
    }

    /**
     * Evaluates a BsonValue against an operand using the specified operator.
     * Uses binary string comparison (no collation).
     */
    public static boolean evaluateBsonValue(BsonValue bsonValue, Operator op, Object operand) {
        return evaluateBsonValue(bsonValue, op, operand, null);
    }

    private static boolean evaluateBsonValue(BsonValue bsonValue, Operator op, Object operand, Collator collator) {
        // Handle ALL with List<BqlValue> operand
        if (op == Operator.ALL && operand instanceof List<?> list) {
            if (!bsonValue.isArray()) {
                return false;
            }
            List<BqlValue> found = new ArrayList<>();
            for (BsonValue element : bsonValue.asArray()) {
                BqlValue value = BSONUtil.convertBsonValueToBqlValue(element);
                if (value != null) {
                    found.add(value);
                }
            }
            return list.stream()
                    .filter(v -> v instanceof BqlValue)
                    .map(v -> (BqlValue) v)
                    .allMatch(expected ->
                            found.stream().anyMatch(actual -> matchesListItem(actual, expected, collator))
                    );
        }

        // Handle SIZE with int operand
        if (op == Operator.SIZE) {
            int expectedSize;
            if (operand instanceof Integer i) {
                expectedSize = i;
            } else if (operand instanceof Int32Val(int value)) {
                expectedSize = value;
            } else {
                return false;
            }
            if (!bsonValue.isArray()) {
                return false;
            }
            return bsonValue.asArray().size() == expectedSize;
        }

        // Handle IN/NIN with List<BqlValue> operand
        if ((op == Operator.IN || op == Operator.NIN) && operand instanceof List<?> list) {
            // If a field is an array, check if any element matches any value in the list
            if (bsonValue.isArray()) {
                boolean matches = false;
                for (BsonValue element : bsonValue.asArray()) {
                    BqlValue elementValue = BSONUtil.convertBsonValueToBqlValue(element);
                    if (elementValue != null) {
                        for (Object item : list) {
                            if (item instanceof BqlValue expected && matchesListItem(elementValue, expected, collator)) {
                                matches = true;
                                break;
                            }
                        }
                        if (matches) break;
                    }
                }
                return (op == Operator.IN) == matches;
            }

            // Scalar field: check if the value matches any in the list
            BqlValue actual = BSONUtil.convertBsonValueToBqlValue(bsonValue);
            if (actual == null) {
                return op == Operator.NIN;
            }
            boolean matches = list.stream()
                    .filter(v -> v instanceof BqlValue)
                    .map(v -> (BqlValue) v)
                    .anyMatch(expected -> matchesListItem(actual, expected, collator));
            return (op == Operator.IN) == matches;
        }

        // $regex against an array field: match if any string element matches the pattern
        if (bsonValue.isArray() && operand instanceof RegexVal regex) {
            for (BsonValue element : bsonValue.asArray()) {
                if (element.isString() && regex.compiled().matcher(element.asString().getValue()).find()) {
                    return true;
                }
            }
            return false;
        }

        // Handle comparison operators when the field is an array and operand is a scalar BqlValue
        if (bsonValue.isArray() && operand instanceof BqlValue expectedValue) {
            if (op == Operator.EQ || op == Operator.NE || op == Operator.GT ||
                    op == Operator.GTE || op == Operator.LT || op == Operator.LTE) {
                boolean anyMatch = false;
                for (BsonValue element : bsonValue.asArray()) {
                    BqlValue elementValue = BSONUtil.convertBsonValueToBqlValue(element);
                    if (elementValue != null && evaluateSingleComparison(op, elementValue, expectedValue, collator)) {
                        anyMatch = true;
                        break;
                    }
                }
                // For NE: match if NO element equals the value
                // For all others: match if ANY element satisfies the condition
                return (op == Operator.NE) != anyMatch;
            }
        }

        // Convert BsonValue to comparable value and evaluate
        return switch (operand) {
            case StringVal(String expected) -> bsonValue.isString() &&
                    evaluateComparison(op, bsonValue.asString().getValue(), expected, collator);

            case RegexVal regex -> bsonValue.isString() &&
                    regex.compiled().matcher(bsonValue.asString().getValue()).find();

            case Int32Val(int expected) -> {
                if (bsonValue.isInt32()) {
                    yield evaluateComparison(op, bsonValue.asInt32().getValue(), expected, null);
                }
                yield numericCrossTypeCompare(bsonValue, BsonType.INT32, op, expected);
            }

            case Int64Val(long expected) -> {
                if (bsonValue.isInt64()) {
                    yield evaluateComparison(op, bsonValue.asInt64().getValue(), expected, null);
                }
                yield numericCrossTypeCompare(bsonValue, BsonType.INT64, op, expected);
            }

            case DoubleVal(double expected) -> {
                if (bsonValue.isDouble()) {
                    yield evaluateComparison(op, bsonValue.asDouble().getValue(), expected, null);
                }
                yield numericCrossTypeCompare(bsonValue, BsonType.DOUBLE, op, expected);
            }

            case Decimal128Val(BigDecimal expected) -> {
                if (bsonValue.isDecimal128()) {
                    yield evaluateComparison(op, bsonValue.asDecimal128().decimal128Value().bigDecimalValue(), expected, null);
                }
                yield numericCrossTypeCompare(bsonValue, BsonType.DECIMAL128, op, expected);
            }

            case BooleanVal(boolean expected) -> bsonValue.isBoolean() &&
                    evaluateBooleanComparison(op, bsonValue.asBoolean().getValue(), expected);

            case BinaryVal(byte[] expected) -> bsonValue.isBinary() &&
                    evaluateComparison(op, bsonValue.asBinary().getData(), expected, null);

            case VersionstampVal(Versionstamp expected) -> bsonValue.isBinary() &&
                    evaluateComparison(op, bsonValue.asBinary().getData(), expected.getBytes(), null);

            case TimestampVal(long expected) -> bsonValue.isTimestamp() &&
                    evaluateComparison(op, bsonValue.asTimestamp().getValue(), expected, null);

            case DateTimeVal(long expected) -> bsonValue.isDateTime() &&
                    evaluateComparison(op, bsonValue.asDateTime().getValue(), expected, null);

            case ObjectIdVal(ObjectId expected) -> bsonValue.isObjectId() &&
                    evaluateComparison(op, bsonValue.asObjectId().getValue(), expected, null);

            case ArrayVal(List<BqlValue> expectedValues) -> bsonValue.isArray() &&
                    evaluateArrayBsonValue(bsonValue.asArray(), op, expectedValues, collator);

            case NullVal ignored -> {
                if (op == Operator.EQ) yield bsonValue.isNull();
                if (op == Operator.NE) yield !bsonValue.isNull();
                yield false;
            }

            default -> false;
        };
    }

    private static boolean evaluateBooleanComparison(Operator op, boolean actual, boolean expected) {
        return switch (op) {
            case EQ -> actual == expected;
            case NE -> actual != expected;
            default -> false;
        };
    }

    private static boolean evaluateArrayBsonValue(BsonArray array, Operator op, List<BqlValue> expectedValues, Collator collator) {
        return switch (op) {
            case IN, NIN -> {
                boolean matches = false;
                for (BsonValue element : array) {
                    BqlValue value = BSONUtil.convertBsonValueToBqlValue(element);
                    if (value != null && !matches) {
                        matches = expectedValues.stream().anyMatch(expected -> valuesEqual(value, expected, collator));
                    }
                }
                yield (op == Operator.IN) == matches;
            }
            default -> false;
        };
    }

    // ==================== BqlValue Comparison ====================

    /**
     * Matches a single value from $in/$nin/$all against one list element. A regex element is a matcher:
     * it matches only string values, consistent with $regex. Any other element uses value equality.
     */
    private static boolean matchesListItem(BqlValue actual, BqlValue expected, Collator collator) {
        if (expected instanceof RegexVal regex) {
            return actual instanceof StringVal(String s) && regex.compiled().matcher(s).find();
        }
        return valuesEqual(actual, expected, collator);
    }

    private static boolean valuesEqual(BqlValue v1, BqlValue v2, Collator collator) {
        if (v1.getClass() != v2.getClass()) {
            return numericBqlSingleComparison(Operator.EQ, v1, v2);
        }
        return switch (v1) {
            case StringVal(String s1) -> stringsEqual(s1, ((StringVal) v2).value(), collator);
            case Int32Val(int i1) -> i1 == ((Int32Val) v2).value();
            case Int64Val(long l1) -> l1 == ((Int64Val) v2).value();
            case DoubleVal(double d1) -> Double.compare(d1, ((DoubleVal) v2).value()) == 0;
            case Decimal128Val(BigDecimal bd1) -> bd1.compareTo(((Decimal128Val) v2).value()) == 0;
            case BooleanVal(boolean b1) -> b1 == ((BooleanVal) v2).value();
            case NullVal ignored -> true;
            case TimestampVal(long t1) -> t1 == ((TimestampVal) v2).value();
            case DateTimeVal(long dt1) -> dt1 == ((DateTimeVal) v2).value();
            case BinaryVal(byte[] bytes1) -> Arrays.equals(bytes1, ((BinaryVal) v2).value());
            case VersionstampVal(Versionstamp vs1) -> vs1.equals(((VersionstampVal) v2).value());
            case ObjectIdVal(ObjectId oid1) -> oid1.equals(((ObjectIdVal) v2).value());
            case ArrayVal(List<BqlValue> list1) -> arrayValEquals(list1, ((ArrayVal) v2).values(), collator);
            default -> false;
        };
    }

    private static boolean arrayValEquals(List<BqlValue> list1, List<BqlValue> list2, Collator collator) {
        if (list1.size() != list2.size()) {
            return false;
        }
        for (int i = 0; i < list1.size(); i++) {
            if (!valuesEqual(list1.get(i), list2.get(i), collator)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Evaluates a single comparison between two BqlValue objects.
     * For NE, checks equality (caller inverts result).
     */
    private static boolean evaluateSingleComparison(Operator op, BqlValue actual, BqlValue expected, Collator collator) {
        // For NE, we check equality (the caller inverts the result)
        Operator effectiveOp = (op == Operator.NE) ? Operator.EQ : op;

        // Type must match for comparison, or both must be numeric for widening
        if (actual.getClass() != expected.getClass()) {
            return numericBqlSingleComparison(effectiveOp, actual, expected);
        }

        return switch (actual) {
            case Int32Val(int a) -> compareNumbers(effectiveOp, a, ((Int32Val) expected).value());
            case Int64Val(long a) -> compareNumbers(effectiveOp, a, ((Int64Val) expected).value());
            case DoubleVal(double a) -> compareDoubles(effectiveOp, a, ((DoubleVal) expected).value());
            case Decimal128Val(BigDecimal a) -> compareNumbers(effectiveOp, a, ((Decimal128Val) expected).value());
            case StringVal(String a) ->
                    compareByResult(effectiveOp, compareStrings(a, ((StringVal) expected).value(), collator));
            case TimestampVal(long a) -> compareNumbers(effectiveOp, a, ((TimestampVal) expected).value());
            case DateTimeVal(long a) -> compareNumbers(effectiveOp, a, ((DateTimeVal) expected).value());
            case BooleanVal(boolean a) -> effectiveOp == Operator.EQ && a == ((BooleanVal) expected).value();
            case BinaryVal(byte[] a) -> compareBinary(effectiveOp, a, ((BinaryVal) expected).value());
            case VersionstampVal(Versionstamp a) ->
                    compareBinary(effectiveOp, a.getBytes(), ((VersionstampVal) expected).value().getBytes());
            case ObjectIdVal(ObjectId a) -> compareNumbers(effectiveOp, a, ((ObjectIdVal) expected).value());
            default -> effectiveOp == Operator.EQ && valuesEqual(actual, expected, collator);
        };
    }

    private static boolean compareByResult(Operator op, int cmp) {
        return switch (op) {
            case EQ -> cmp == 0;
            case NE -> cmp != 0;
            case GT -> cmp > 0;
            case GTE -> cmp >= 0;
            case LT -> cmp < 0;
            case LTE -> cmp <= 0;
            default -> false;
        };
    }

    private static <T extends Comparable<T>> boolean compareNumbers(Operator op, T actual, T expected) {
        return compareByResult(op, actual.compareTo(expected));
    }

    private static boolean compareDoubles(Operator op, double actual, double expected) {
        return compareByResult(op, Double.compare(actual, expected));
    }

    private static boolean compareBinary(Operator op, byte[] actual, byte[] expected) {
        return compareByResult(op, Arrays.compare(actual, expected));
    }

    // ==================== Numeric Widening Helpers ====================

    @SuppressWarnings("unchecked")
    private static boolean numericCrossTypeCompare(BsonValue bsonValue, BsonType operandType, Operator op, Object operandJavaValue) {
        if (!NumericWidening.isNumericBsonType(bsonValue.getBsonType())) {
            return false;
        }
        BsonType commonType = NumericWidening.commonType(bsonValue.getBsonType(), operandType);
        if (commonType == null) {
            return false;
        }
        Object actualJavaValue = NumericWidening.extractNumericValue(bsonValue);
        Comparable<Object> promotedActual = (Comparable<Object>) NumericWidening.promoteToComparable(actualJavaValue, bsonValue.getBsonType(), commonType);
        Comparable<Object> promotedExpected = (Comparable<Object>) NumericWidening.promoteToComparable(operandJavaValue, operandType, commonType);
        if (promotedActual == null || promotedExpected == null) {
            return false;
        }
        return compareByResult(op, promotedActual.compareTo(promotedExpected));
    }

    @SuppressWarnings("unchecked")
    private static boolean numericBqlSingleComparison(Operator op, BqlValue actual, BqlValue expected) {
        BsonType t1 = bqlValueToBsonType(actual);
        BsonType t2 = bqlValueToBsonType(expected);
        if (t1 == null || t2 == null) {
            return false;
        }
        BsonType common = NumericWidening.commonType(t1, t2);
        if (common == null) {
            return false;
        }
        Object val1 = extractBqlNumericValue(actual);
        Object val2 = extractBqlNumericValue(expected);
        Comparable<Object> p1 = (Comparable<Object>) NumericWidening.promoteToComparable(val1, t1, common);
        Comparable<Object> p2 = (Comparable<Object>) NumericWidening.promoteToComparable(val2, t2, common);
        if (p1 == null || p2 == null) {
            return false;
        }
        return compareByResult(op, p1.compareTo(p2));
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

    // ==================== Generic Comparison Methods ====================

    /**
     * Evaluates a comparison between two values using the specified operator.
     * Uses binary string comparison (no collation).
     */
    public static <T> boolean evaluateComparison(Operator op, T actual, T expected) {
        return evaluateComparison(op, actual, expected, null);
    }

    static <T> boolean evaluateComparison(Operator op, T actual, T expected, Collator collator) {
        // IN/NIN have special handling - expected is a List
        if (op == Operator.IN) {
            return evaluateIn(actual, expected, collator);
        }
        if (op == Operator.NIN) {
            return !evaluateIn(actual, expected, collator);
        }

        // Type mismatch check (both must be non-null and the same type)
        if (actual != null && expected != null && !actual.getClass().equals(expected.getClass())) {
            return false;
        }

        return switch (op) {
            case EQ -> evaluateEquality(actual, expected, collator);
            case NE -> !evaluateEquality(actual, expected, collator);
            case GT, GTE, LT, LTE -> evaluateOrdering(op, actual, expected, collator);
            default -> throw new UnsupportedOperationException("Comparison not supported for operator: " + op);
        };
    }

    private static <T> boolean evaluateEquality(T actual, T expected, Collator collator) {
        if (actual == null && expected == null) {
            return true;
        }
        if (actual == null || expected == null) {
            return false;
        }
        // Handle arrays with content comparison (preserving order)
        if (actual.getClass().isArray()) {
            return arrayEquals(actual, expected);
        }
        // Handle Lists with content and order comparison
        if (actual instanceof List<?> actualList && expected instanceof List<?> expectedList) {
            return listEquals(actualList, expectedList, collator);
        }
        if (collator != null && actual instanceof String s1 && expected instanceof String s2) {
            return stringsEqual(s1, s2, collator);
        }
        return actual.equals(expected);
    }

    private static boolean arrayEquals(Object actual, Object expected) {
        if (actual instanceof byte[] a && expected instanceof byte[] b) {
            return Arrays.equals(a, b);
        }
        if (actual instanceof int[] a && expected instanceof int[] b) {
            return Arrays.equals(a, b);
        }
        if (actual instanceof long[] a && expected instanceof long[] b) {
            return Arrays.equals(a, b);
        }
        if (actual instanceof double[] a && expected instanceof double[] b) {
            return Arrays.equals(a, b);
        }
        if (actual instanceof float[] a && expected instanceof float[] b) {
            return Arrays.equals(a, b);
        }
        if (actual instanceof boolean[] a && expected instanceof boolean[] b) {
            return Arrays.equals(a, b);
        }
        if (actual instanceof short[] a && expected instanceof short[] b) {
            return Arrays.equals(a, b);
        }
        if (actual instanceof char[] a && expected instanceof char[] b) {
            return Arrays.equals(a, b);
        }
        if (actual instanceof Object[] a && expected instanceof Object[] b) {
            return Arrays.deepEquals(a, b);
        }
        return false;
    }

    private static boolean listEquals(List<?> actual, List<?> expected, Collator collator) {
        if (actual.size() != expected.size()) {
            return false;
        }
        for (int i = 0; i < actual.size(); i++) {
            Object a = actual.get(i);
            Object b = expected.get(i);
            if (!evaluateEquality(a, b, collator)) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private static <T> boolean evaluateOrdering(Operator op, T actual, T expected, Collator collator) {
        if (actual == null || expected == null) {
            return false;
        }

        int cmp;
        switch (actual) {
            case String s1 when collator != null && expected instanceof String s2 ->
                    cmp = compareStrings(s1, s2, collator);
            case byte[] actualBytes when expected instanceof byte[] expectedBytes ->
                    cmp = Arrays.compare(actualBytes, expectedBytes);
            case Comparable<?> ignored -> cmp = ((Comparable<T>) actual).compareTo(expected);
            default -> {
                // Non-comparable types - return false
                return false;
            }
        }

        return switch (op) {
            case GT -> cmp > 0;
            case GTE -> cmp >= 0;
            case LT -> cmp < 0;
            case LTE -> cmp <= 0;
            default -> false;
        };
    }

    private static <T> boolean evaluateIn(T actual, T expected, Collator collator) {
        if (!(expected instanceof List<?> expectedList)) {
            return false;
        }

        if (actual instanceof byte[] actualBytes) {
            for (Object item : expectedList) {
                if (item instanceof byte[] itemBytes && Arrays.equals(actualBytes, itemBytes)) {
                    return true;
                }
            }
            return false;
        }

        // Check if actual matches any item in the list with type-aware comparison
        for (Object item : expectedList) {
            if (item != null && actual != null && item.getClass().equals(actual.getClass())) {
                if (evaluateEquality(actual, item, collator)) {
                    return true;
                }
            } else if (item == null && actual == null) {
                return true;
            }
        }
        return false;
    }
}
