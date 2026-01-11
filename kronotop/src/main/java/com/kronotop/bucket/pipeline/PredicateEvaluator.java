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

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.KronotopException;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.SelectorMatcher;
import com.kronotop.bucket.planner.Operator;
import org.bson.BsonArray;
import org.bson.BsonValue;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Evaluates predicates against BSON documents.
 * Provides unified comparison logic for all supported types.
 */
public class PredicateEvaluator {

    /**
     * Tests if a BSON document satisfies the conditions specified by a ResidualPredicate.
     *
     * @param filter   the predicate to test
     * @param document the BSON document as a ByteBuffer
     * @return true if the document matches the predicate
     */
    public static boolean testResidualPredicate(ResidualPredicate filter, ByteBuffer document) {
        BsonValue bsonValue = SelectorMatcher.match(filter.selector(), document);

        // EXISTS performs a physical field presence check
        if (filter.op() == Operator.EXISTS) {
            if (!(filter.operand() instanceof Boolean existsOperand)) {
                throw new KronotopException("EXISTS operator requires boolean operand");
            }
            return existsOperand == (bsonValue != null);
        }

        boolean fieldMissingOrNull = (bsonValue == null || bsonValue.isNull());
        boolean operandIsNull = filter.operand() instanceof NullVal;

        // Null is only meaningful in EQ / NE context
        if (operandIsNull) {
            if (fieldMissingOrNull) {
                return filter.op() == Operator.EQ;
            }
            return filter.op() == Operator.NE;
        }

        // Handle IN/NIN with null field: check if the list contains NullVal
        if (fieldMissingOrNull && filter.operand() instanceof List<?> list) {
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
        return evaluateBsonValue(bsonValue, filter.op(), filter.operand());
    }

    /**
     * Evaluates a BsonValue against an operand using the specified operator.
     */
    private static boolean evaluateBsonValue(BsonValue bsonValue, Operator op, Object operand) {
        // Handle ALL with List<BqlValue> operand
        if (op == Operator.ALL && operand instanceof List<?> list) {
            if (!bsonValue.isArray()) {
                return false;
            }
            List<BqlValue> found = new ArrayList<>();
            for (BsonValue element : bsonValue.asArray()) {
                BqlValue value = convertBsonValueToBqlValue(element);
                if (value != null) {
                    found.add(value);
                }
            }
            return list.stream()
                    .filter(v -> v instanceof BqlValue)
                    .map(v -> (BqlValue) v)
                    .allMatch(expected -> found.stream().anyMatch(actual -> valuesEqual(actual, expected)));
        }

        // Handle SIZE with int operand
        if (op == Operator.SIZE && operand instanceof Integer expectedSize) {
            if (!bsonValue.isArray()) {
                return false;
            }
            return bsonValue.asArray().size() == expectedSize;
        }

        // Handle IN/NIN with List<BqlValue> operand
        if ((op == Operator.IN || op == Operator.NIN) && operand instanceof List<?> list) {
            // If field is an array, check if any element matches any value in the list
            if (bsonValue.isArray()) {
                boolean matches = false;
                for (BsonValue element : bsonValue.asArray()) {
                    BqlValue elementValue = convertBsonValueToBqlValue(element);
                    if (elementValue != null) {
                        for (Object item : list) {
                            if (item instanceof BqlValue expected && valuesEqual(elementValue, expected)) {
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
            BqlValue actual = convertBsonValueToBqlValue(bsonValue);
            if (actual == null) {
                return op == Operator.NIN;
            }
            boolean matches = list.stream()
                    .filter(v -> v instanceof BqlValue)
                    .map(v -> (BqlValue) v)
                    .anyMatch(expected -> valuesEqual(actual, expected));
            return (op == Operator.IN) == matches;
        }

        // Convert BsonValue to comparable value and evaluate
        return switch (operand) {
            case StringVal(String expected) -> bsonValue.isString() &&
                    evaluateComparison(op, bsonValue.asString().getValue(), expected);

            case Int32Val(int expected) -> bsonValue.isInt32() &&
                    evaluateComparison(op, bsonValue.asInt32().getValue(), expected);

            case Int64Val(long expected) -> bsonValue.isInt64() &&
                    evaluateComparison(op, bsonValue.asInt64().getValue(), expected);

            case DoubleVal(double expected) -> bsonValue.isDouble() &&
                    evaluateComparison(op, bsonValue.asDouble().getValue(), expected);

            case Decimal128Val(BigDecimal expected) -> bsonValue.isDecimal128() &&
                    evaluateComparison(op, bsonValue.asDecimal128().decimal128Value().bigDecimalValue(), expected);

            case BooleanVal(boolean expected) -> bsonValue.isBoolean() &&
                    evaluateBooleanComparison(op, bsonValue.asBoolean().getValue(), expected);

            case BinaryVal(byte[] expected) -> bsonValue.isBinary() &&
                    evaluateComparison(op, bsonValue.asBinary().getData(), expected);

            case TimestampVal(long expected) -> bsonValue.isTimestamp() &&
                    evaluateComparison(op, bsonValue.asTimestamp().getValue(), expected);

            case DateTimeVal(long expected) -> bsonValue.isDateTime() &&
                    evaluateComparison(op, bsonValue.asDateTime().getValue(), expected);

            case VersionstampVal(Versionstamp expected) -> bsonValue.isBinary() &&
                    evaluateComparison(op, Versionstamp.fromBytes(bsonValue.asBinary().getData()), expected);

            case ArrayVal(List<BqlValue> expectedValues) -> bsonValue.isArray() &&
                    evaluateArrayBsonValue(bsonValue.asArray(), op, expectedValues);

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

    private static boolean evaluateArrayBsonValue(BsonArray array, Operator op, List<BqlValue> expectedValues) {
        return switch (op) {
            case IN, NIN -> {
                boolean matches = false;
                for (BsonValue element : array) {
                    BqlValue value = convertBsonValueToBqlValue(element);
                    if (value != null && !matches) {
                        matches = expectedValues.stream().anyMatch(expected -> valuesEqual(value, expected));
                    }
                }
                yield (op == Operator.IN) == matches;
            }
            default -> false;
        };
    }

    private static BqlValue convertBsonValueToBqlValue(BsonValue bsonValue) {
        return switch (bsonValue.getBsonType()) {
            case STRING -> new StringVal(bsonValue.asString().getValue());
            case INT32 -> new Int32Val(bsonValue.asInt32().getValue());
            case INT64 -> new Int64Val(bsonValue.asInt64().getValue());
            case DOUBLE -> new DoubleVal(bsonValue.asDouble().getValue());
            case DECIMAL128 -> new Decimal128Val(bsonValue.asDecimal128().decimal128Value().bigDecimalValue());
            case BOOLEAN -> new BooleanVal(bsonValue.asBoolean().getValue());
            case NULL -> NullVal.INSTANCE;
            case DATE_TIME -> new DateTimeVal(bsonValue.asDateTime().getValue());
            case TIMESTAMP -> new TimestampVal(bsonValue.asTimestamp().getValue());
            case BINARY -> new BinaryVal(bsonValue.asBinary().getData());
            case ARRAY -> {
                List<BqlValue> elements = new ArrayList<>();
                for (BsonValue element : bsonValue.asArray()) {
                    BqlValue converted = convertBsonValueToBqlValue(element);
                    if (converted != null) {
                        elements.add(converted);
                    }
                }
                yield new ArrayVal(elements);
            }
            default -> null;
        };
    }

    /**
     * Compares two BqlValue objects for equality.
     */
    private static boolean valuesEqual(BqlValue v1, BqlValue v2) {
        if (v1.getClass() != v2.getClass()) {
            return false;
        }
        return switch (v1) {
            case StringVal(String s1) -> s1.equals(((StringVal) v2).value());
            case Int32Val(int i1) -> i1 == ((Int32Val) v2).value();
            case Int64Val(long l1) -> l1 == ((Int64Val) v2).value();
            case DoubleVal(double d1) -> Double.compare(d1, ((DoubleVal) v2).value()) == 0;
            case Decimal128Val(BigDecimal bd1) -> bd1.compareTo(((Decimal128Val) v2).value()) == 0;
            case BooleanVal(boolean b1) -> b1 == ((BooleanVal) v2).value();
            case NullVal ignored -> true;
            case TimestampVal(long t1) -> t1 == ((TimestampVal) v2).value();
            case DateTimeVal(long dt1) -> dt1 == ((DateTimeVal) v2).value();
            case BinaryVal(byte[] bytes1) -> Arrays.equals(bytes1, ((BinaryVal) v2).value());
            case VersionstampVal(Versionstamp vs1) -> vs1.compareTo(((VersionstampVal) v2).value()) == 0;
            case ArrayVal(List<BqlValue> list1) -> arrayValEquals(list1, ((ArrayVal) v2).values());
            default -> false;
        };
    }

    private static boolean arrayValEquals(List<BqlValue> list1, List<BqlValue> list2) {
        if (list1.size() != list2.size()) {
            return false;
        }
        for (int i = 0; i < list1.size(); i++) {
            if (!valuesEqual(list1.get(i), list2.get(i))) {
                return false;
            }
        }
        return true;
    }

    // ==================== Generic Comparison Methods ====================

    /**
     * Evaluates a comparison between two values using the specified operator.
     * Returns false if types don't match (except for IN/NIN which handle lists).
     */
    public static <T> boolean evaluateComparison(Operator op, T actual, T expected) {
        // IN/NIN have special handling - expected is a List
        if (op == Operator.IN) {
            return evaluateIn(actual, expected);
        }
        if (op == Operator.NIN) {
            return !evaluateIn(actual, expected);
        }

        // Type mismatch check (both must be non-null and same type)
        if (actual != null && expected != null && !actual.getClass().equals(expected.getClass())) {
            return false;
        }

        return switch (op) {
            case EQ -> evaluateEquality(actual, expected);
            case NE -> !evaluateEquality(actual, expected);
            case GT, GTE, LT, LTE -> evaluateOrdering(op, actual, expected);
            default -> throw new UnsupportedOperationException("Comparison not supported for operator: " + op);
        };
    }

    private static <T> boolean evaluateEquality(T actual, T expected) {
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
            return listEquals(actualList, expectedList);
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

    private static boolean listEquals(List<?> actual, List<?> expected) {
        if (actual.size() != expected.size()) {
            return false;
        }
        for (int i = 0; i < actual.size(); i++) {
            Object a = actual.get(i);
            Object b = expected.get(i);
            if (!evaluateEquality(a, b)) {
                return false;
            }
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private static <T> boolean evaluateOrdering(Operator op, T actual, T expected) {
        if (actual == null || expected == null) {
            return false;
        }

        int cmp;
        if (actual instanceof byte[] actualBytes && expected instanceof byte[] expectedBytes) {
            cmp = Arrays.compare(actualBytes, expectedBytes);
        } else if (actual instanceof Comparable) {
            cmp = ((Comparable<T>) actual).compareTo(expected);
        } else {
            // Non-comparable types - return false
            return false;
        }

        return switch (op) {
            case GT -> cmp > 0;
            case GTE -> cmp >= 0;
            case LT -> cmp < 0;
            case LTE -> cmp <= 0;
            default -> false;
        };
    }

    private static <T> boolean evaluateIn(T actual, T expected) {
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
                if (evaluateEquality(actual, item)) {
                    return true;
                }
            } else if (item == null && actual == null) {
                return true;
            }
        }
        return false;
    }
}
