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
import com.kronotop.bucket.planner.Operator;
import org.bson.BsonBinaryReader;
import org.bson.BsonReader;
import org.bson.BsonType;

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
        try (BsonReader reader = new BsonBinaryReader(document)) {
            reader.readStartDocument();

            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                String fieldName = reader.readName();

                if (!filter.selector().equals(fieldName)) {
                    reader.skipValue();
                    continue;
                }

                // Field found - evaluate the predicate
                return evaluateField(reader, filter.op(), filter.operand());
            }

            reader.readEndDocument();
        } catch (Exception e) {
            throw new KronotopException(e);
        }

        // Field not found - matches for null equality or null-inclusive comparisons
        if (filter.operand() instanceof NullVal) {
            return switch (filter.op()) {
                case EQ, GTE, LTE -> true;
                default -> false;
            };
        }
        return false;
    }

    /**
     * Evaluates a field value against an operand using the specified operator.
     */
    private static boolean evaluateField(BsonReader reader, Operator op, Object operand) {
        // Handle IN/NIN with List<BqlValue> operand
        if ((op == Operator.IN || op == Operator.NIN) && operand instanceof List<?> list) {
            boolean matches = matchesAnyInList(reader, list);
            return (op == Operator.IN) == matches;
        }

        return switch (operand) {
            case StringVal(String expected) -> evaluateTypedField(reader, BsonType.STRING, op,
                    reader::readString, expected);

            case Int32Val(int expected) -> evaluateTypedField(reader, BsonType.INT32, op,
                    reader::readInt32, expected);

            case Int64Val(long expected) -> evaluateTypedField(reader, BsonType.INT64, op,
                    reader::readInt64, expected);

            case DoubleVal(double expected) -> evaluateTypedField(reader, BsonType.DOUBLE, op,
                    reader::readDouble, expected);

            case Decimal128Val(BigDecimal expected) -> evaluateTypedField(reader, BsonType.DECIMAL128, op,
                    () -> reader.readDecimal128().bigDecimalValue(), expected);

            case BooleanVal(boolean expected) -> evaluateBooleanField(reader, op, expected);

            case NullVal ignored -> evaluateNullField(reader, op);

            case BinaryVal(byte[] expected) -> evaluateTypedField(reader, BsonType.BINARY, op,
                    () -> reader.readBinaryData().getData(), expected);

            case TimestampVal(long expected) -> evaluateTypedField(reader, BsonType.TIMESTAMP, op,
                    () -> reader.readTimestamp().getValue(), expected);

            case DateTimeVal(long expected) -> evaluateTypedField(reader, BsonType.DATE_TIME, op,
                    reader::readDateTime, expected);

            case VersionstampVal(Versionstamp expected) -> evaluateVersionstampField(reader, op, expected);

            case ArrayVal(List<BqlValue> expectedValues) -> evaluateArrayField(reader, op, expectedValues);

            default -> {
                reader.skipValue();
                yield false;
            }
        };
    }

    /**
     * Evaluates a typed field using the unified comparison logic.
     */
    private static <T> boolean evaluateTypedField(BsonReader reader, BsonType expectedType,
                                                  Operator op, ValueReader<T> valueReader, T expected) {
        if (reader.getCurrentBsonType() != expectedType) {
            reader.skipValue();
            return false;
        }
        T actual = valueReader.read();
        return evaluateComparison(op, actual, expected);
    }

    /**
     * Evaluates a boolean field. Booleans only support EQ and NE.
     */
    private static boolean evaluateBooleanField(BsonReader reader, Operator op, boolean expected) {
        if (reader.getCurrentBsonType() != BsonType.BOOLEAN) {
            reader.skipValue();
            return false;
        }
        boolean actual = reader.readBoolean();

        return switch (op) {
            case EQ -> actual == expected;
            case NE -> actual != expected;
            default -> false;
        };
    }

    /**
     * Evaluates a null field comparison. In BSON ordering, null is the lowest value.
     */
    private static boolean evaluateNullField(BsonReader reader, Operator op) {
        boolean isNull = reader.getCurrentBsonType() == BsonType.NULL;
        if (isNull) {
            reader.readNull();
        } else {
            reader.skipValue();
        }

        return switch (op) {
            case EQ, LTE -> isNull;
            case NE, EXISTS, GT -> !isNull;
            case GTE -> true;
            default -> false;
        };
    }

    /**
     * Evaluates a Versionstamp field. Versionstamp is stored as BINARY but compared using Comparable.
     */
    private static boolean evaluateVersionstampField(BsonReader reader, Operator op, Versionstamp expected) {
        if (reader.getCurrentBsonType() != BsonType.BINARY) {
            reader.skipValue();
            return false;
        }
        byte[] actualBytes = reader.readBinaryData().getData();
        Versionstamp actual = Versionstamp.fromBytes(actualBytes);
        return evaluateComparison(op, actual, expected);
    }

    /**
     * Evaluates array-specific operators: SIZE, ALL, IN.
     */
    private static boolean evaluateArrayField(BsonReader reader, Operator op, List<BqlValue> expectedValues) {
        if (reader.getCurrentBsonType() != BsonType.ARRAY) {
            reader.skipValue();
            return false;
        }

        return switch (op) {
            case SIZE -> {
                reader.readStartArray();
                int size = 0;
                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    reader.skipValue();
                    size++;
                }
                reader.readEndArray();

                if (!expectedValues.isEmpty() && expectedValues.getFirst() instanceof Int32Val(int expectedSize)) {
                    yield size == expectedSize;
                }
                yield false;
            }
            case ALL -> {
                reader.readStartArray();
                List<BqlValue> found = new ArrayList<>();
                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    BqlValue value = readBsonValue(reader);
                    if (value != null) {
                        found.add(value);
                    }
                }
                reader.readEndArray();

                yield expectedValues.stream().allMatch(expected ->
                        found.stream().anyMatch(actual -> valuesEqual(actual, expected)));
            }
            case IN, NIN -> {
                reader.readStartArray();
                boolean matches = false;
                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    BqlValue value = readBsonValue(reader);
                    if (value != null && !matches) {
                        matches = expectedValues.stream().anyMatch(expected -> valuesEqual(value, expected));
                    }
                }
                reader.readEndArray();
                yield (op == Operator.IN) == matches;
            }
            default -> {
                reader.skipValue();
                yield false;
            }
        };
    }

    /**
     * Checks if a field value matches any value in the list (for IN/NIN operators).
     */
    private static boolean matchesAnyInList(BsonReader reader, List<?> expectedValues) {
        BqlValue actual = readBsonValue(reader);
        if (actual == null) {
            return false;
        }
        return expectedValues.stream()
                .filter(v -> v instanceof BqlValue)
                .map(v -> (BqlValue) v)
                .anyMatch(expected -> valuesEqual(actual, expected));
    }

    /**
     * Reads a BSON value and converts it to a BqlValue.
     */
    private static BqlValue readBsonValue(BsonReader reader) {
        return switch (reader.getCurrentBsonType()) {
            case STRING -> new StringVal(reader.readString());
            case INT32 -> new Int32Val(reader.readInt32());
            case INT64 -> new Int64Val(reader.readInt64());
            case DOUBLE -> new DoubleVal(reader.readDouble());
            case DECIMAL128 -> new Decimal128Val(reader.readDecimal128().bigDecimalValue());
            case BOOLEAN -> new BooleanVal(reader.readBoolean());
            case NULL -> {
                reader.readNull();
                yield NullVal.INSTANCE;
            }
            case TIMESTAMP -> new TimestampVal(reader.readTimestamp().getValue());
            case DATE_TIME -> new DateTimeVal(reader.readDateTime());
            case BINARY -> new BinaryVal(reader.readBinaryData().getData());
            default -> {
                reader.skipValue();
                yield null;
            }
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

    @FunctionalInterface
    private interface ValueReader<T> {
        T read();
    }
}
