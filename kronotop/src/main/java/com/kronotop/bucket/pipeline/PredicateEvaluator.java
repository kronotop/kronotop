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
import com.kronotop.bucket.planner.physical.PhysicalAnd;
import org.bson.BsonBinaryReader;
import org.bson.BsonReader;
import org.bson.BsonType;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class PredicateEvaluator {
    /**
     * Generic comparison method for comparable values (strings, byte arrays, etc.).
     *
     * @param op       the comparison operator to be applied
     * @param actual   the actual value to be evaluated
     * @param expected the expected value to compare against
     * @param <T>      the type of values being compared
     * @return true if the comparison satisfies the operator's condition, otherwise false
     * @throws UnsupportedOperationException if the provided operator is not supported for this comparison type
     */
    public static <T> boolean evaluateComparison(Operator op, T actual, T expected) {
        return switch (op) {
            case EQ -> {
                if (actual instanceof byte[] actualBytes && expected instanceof byte[] expectedBytes) {
                    yield Arrays.equals(actualBytes, expectedBytes);
                } else {
                    yield actual.equals(expected);
                }
            }
            case NE -> {
                if (actual instanceof byte[] actualBytes && expected instanceof byte[] expectedBytes) {
                    yield !Arrays.equals(actualBytes, expectedBytes);
                } else {
                    yield !actual.equals(expected);
                }
            }
            case GT, GTE, LT, LTE -> {
                if (actual instanceof String actualStr && expected instanceof String expectedStr) {
                    int cmp = actualStr.compareTo(expectedStr);
                    yield switch (op) {
                        case GT -> cmp > 0;
                        case GTE -> cmp >= 0;
                        case LT -> cmp < 0;
                        case LTE -> cmp <= 0;
                        default -> false;
                    };
                } else if (actual instanceof byte[] actualBytes && expected instanceof byte[] expectedBytes) {
                    int cmp = Arrays.compare(actualBytes, expectedBytes);
                    yield switch (op) {
                        case GT -> cmp > 0;
                        case GTE -> cmp >= 0;
                        case LT -> cmp < 0;
                        case LTE -> cmp <= 0;
                        default -> false;
                    };
                } else {
                    throw new UnsupportedOperationException("Comparison operator " + op + " not supported for type: " + actual.getClass().getSimpleName());
                }
            }
            case IN -> {
                List<Object> expectedList = validateListOperand(expected, "IN");
                yield expectedList.contains(actual);
            }
            case NIN -> {
                List<Object> expectedList = validateListOperand(expected, "NIN");
                yield !expectedList.contains(actual);
            }
            default -> throw new UnsupportedOperationException("Comparison not supported for operator: " + op);
        };
    }

    /**
     * Applies a PhysicalAnd filter to a BSON document by applying all child filters.
     * All child filters must match for the document to pass.
     *
     * @param andFilter the {@link PhysicalAnd} containing multiple child filters
     * @param document  the {@link ByteBuffer} representing the BSON-encoded document to be evaluated
     * @return the original {@link ByteBuffer} if the document matches all filters; otherwise, null
     */
    /*ByteBuffer applyPhysicalAnd(PhysicalAnd andFilter, ByteBuffer document) {
        for (PhysicalNode child : andFilter.children()) {
            if (child instanceof PhysicalFilter filter) {
                ByteBuffer result = applyPhysicalFilter(filter, document.duplicate());
                if (result == null) {
                    return null; // One filter failed, document doesn't match
                }
            } else {
                // For now, only support PhysicalFilter children in AND
                throw new UnsupportedOperationException("Nested logical operators in PhysicalAnd not yet supported: " + child.getClass().getSimpleName());
            }
        }

        // All filters passed
        return document.rewind();
    }*/

    /**
     * Validates that the expected value is a list for IN/NIN operations.
     *
     * @param expected     the expected value to validate
     * @param operatorName the name of the operator (for error messages)
     * @return the list if valid
     * @throws UnsupportedOperationException if expected is not a list
     */
    @SuppressWarnings("unchecked")
    static List<Object> validateListOperand(Object expected, String operatorName) {
        if (expected instanceof List<?> expectedList) {
            return (List<Object>) expectedList;
        } else {
            throw new UnsupportedOperationException(operatorName + " operator requires a list of expected values");
        }
    }

    /**
     * Tests if a BSON document satisfies the conditions specified by a {@link ResidualPredicate}.
     * The method iterates through the fields in the provided BSON document and applies the predicate
     * filtering logic to determine whether the document matches the specified criteria.
     * <p>
     * Callers of this method MUST rewind the document.
     *
     * @param filter   the {@link ResidualPredicate} that defines the condition to be tested against the document
     * @param document the BSON document represented as a {@link ByteBuffer} that needs to be evaluated
     * @return true if the BSON document satisfies the conditions specified by the predicate, false otherwise
     */
    public static boolean testResidualPredicate(ResidualPredicate filter, ByteBuffer document) {
        try (BsonReader reader = new BsonBinaryReader(document)) {
            reader.readStartDocument();
            while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                String fieldName = reader.readName();
                // Skip fields that don't match the selector
                if (!filter.selector().equals(fieldName)) {
                    reader.skipValue();
                    continue;
                }

                // Handle IN and NIN operators specially since they have List<BqlValue> operands
                if (filter.op() == Operator.IN || filter.op() == Operator.NIN) {
                    if (filter.operand() instanceof List<?> operandList) {
                        boolean matches = evaluateInOperation(reader, operandList);
                        if (filter.op() == Operator.IN) {
                            return matches;
                        } else { // NIN
                            return !matches;
                        }
                    } else {
                        reader.skipValue();
                        return false;
                    }
                }

                // Apply the filter based on operand type and operator for non-IN/NIN operators
                // Document comparison is complex and typically not used in filters
                return switch (filter.operand()) {
                    case StringVal(String expectedValue) -> {
                        if (reader.getCurrentBsonType() != BsonType.STRING) {
                            reader.skipValue();
                            yield false;
                        }
                        String actualValue = reader.readString();
                        yield evaluateComparison(filter.op(), actualValue, expectedValue);
                    }
                    case Int32Val(int expectedValue) -> {
                        if (reader.getCurrentBsonType() != BsonType.INT32) {
                            reader.skipValue();
                            yield false;
                        }
                        int actualValue = reader.readInt32();
                        yield evaluateNumericComparison(filter.op(), actualValue, expectedValue);
                    }
                    case Int64Val(long expectedValue) -> {
                        if (reader.getCurrentBsonType() != BsonType.INT64) {
                            reader.skipValue();
                            yield false;
                        }
                        long actualValue = reader.readInt64();
                        yield evaluateNumericComparison(filter.op(), actualValue, expectedValue);
                    }
                    case DoubleVal(double expectedValue) -> {
                        if (reader.getCurrentBsonType() != BsonType.DOUBLE) {
                            reader.skipValue();
                            yield false;
                        }
                        double actualValue = reader.readDouble();
                        yield evaluateNumericComparison(filter.op(), actualValue, expectedValue);
                    }
                    case Decimal128Val(BigDecimal expectedValue) -> {
                        if (reader.getCurrentBsonType() != BsonType.DECIMAL128) {
                            reader.skipValue();
                            yield false;
                        }
                        BigDecimal actualValue = reader.readDecimal128().bigDecimalValue();
                        yield evaluateNumericComparison(filter.op(), actualValue, expectedValue);
                    }
                    case BooleanVal(boolean expectedValue) -> {
                        if (reader.getCurrentBsonType() != BsonType.BOOLEAN) {
                            reader.skipValue();
                            yield false;
                        }
                        boolean actualValue = reader.readBoolean();
                        yield evaluateBooleanComparison(filter.op(), actualValue, expectedValue);
                    }
                    case NullVal ignored -> {
                        boolean isNull = reader.getCurrentBsonType() == BsonType.NULL;
                        if (isNull) {
                            reader.readNull();
                        } else {
                            reader.skipValue();
                        }
                        yield evaluateNullComparison(filter.op(), isNull);
                    }
                    case BinaryVal(byte[] expectedValue) -> {
                        if (reader.getCurrentBsonType() != BsonType.BINARY) {
                            reader.skipValue();
                            yield false;
                        }
                        byte[] actualValue = reader.readBinaryData().getData();
                        yield evaluateComparison(filter.op(), actualValue, expectedValue);
                    }
                    case TimestampVal(long expectedValue) -> {
                        if (reader.getCurrentBsonType() != BsonType.TIMESTAMP) {
                            reader.skipValue();
                            yield false;
                        }
                        long actualValue = reader.readTimestamp().getValue();
                        yield evaluateNumericComparison(filter.op(), actualValue, expectedValue);
                    }
                    case DateTimeVal(long expectedValue) -> {
                        if (reader.getCurrentBsonType() != BsonType.DATE_TIME) {
                            reader.skipValue();
                            yield false;
                        }
                        long actualValue = reader.readDateTime();
                        yield evaluateNumericComparison(filter.op(), actualValue, expectedValue);
                    }
                    case VersionstampVal(Versionstamp expectedValue) -> {
                        if (reader.getCurrentBsonType() != BsonType.BINARY) {
                            reader.skipValue();
                            yield false;
                        }
                        byte[] actualValue = reader.readBinaryData().getData();
                        byte[] expectedBytes = expectedValue.getBytes();
                        yield evaluateComparison(filter.op(), actualValue, expectedBytes);
                    }
                    case ArrayVal(List<BqlValue> expectedValues) ->
                            evaluateArrayComparison(reader, filter.op(), expectedValues);
                    case DocumentVal ignored -> {
                        // Document comparison is complex and typically not used in filters
                        reader.skipValue();
                        yield false;
                    }
                    default -> {
                        reader.skipValue();
                        yield false;
                    }
                };
            }
            reader.readEndDocument();
        } catch (Exception e) {
            throw new KronotopException(e);
        }

        // Field not found in document - doesn't match the filter
        return filter.op().equals(Operator.EQ) && filter.operand() instanceof NullVal;
    }

    /**
     * Evaluates a numeric comparison between two given numbers using the specified operator.
     *
     * @param op       the comparison operator to be applied (e.g., EQ for equals, NE for not equals, GT for greater than, etc.)
     * @param actual   the actual numeric value to be evaluated
     * @param expected the expected numeric value to compare against
     * @return true if the comparison satisfies the operator's condition, otherwise false
     * @throws UnsupportedOperationException if the operator is not supported for numeric comparisons
     */
    private static boolean evaluateNumericComparison(Operator op, Number actual, Number expected) {
        return switch (op) {
            case EQ -> {
                if (actual instanceof BigDecimal actualBig && expected instanceof BigDecimal expectedBig) {
                    yield actualBig.compareTo(expectedBig) == 0;
                } else {
                    double actualDouble = actual.doubleValue();
                    double expectedDouble = expected.doubleValue();
                    yield Double.compare(actualDouble, expectedDouble) == 0;
                }
            }
            case NE -> {
                if (actual instanceof BigDecimal actualBig && expected instanceof BigDecimal expectedBig) {
                    yield actualBig.compareTo(expectedBig) != 0;
                } else {
                    double actualDouble = actual.doubleValue();
                    double expectedDouble = expected.doubleValue();
                    yield Double.compare(actualDouble, expectedDouble) != 0;
                }
            }
            case GT, GTE, LT, LTE -> {
                if (actual instanceof BigDecimal actualBig && expected instanceof BigDecimal expectedBig) {
                    int cmp = actualBig.compareTo(expectedBig);
                    yield switch (op) {
                        case GT -> cmp > 0;
                        case GTE -> cmp >= 0;
                        case LT -> cmp < 0;
                        case LTE -> cmp <= 0;
                        default -> false;
                    };
                } else {
                    double actualDouble = actual.doubleValue();
                    double expectedDouble = expected.doubleValue();
                    yield switch (op) {
                        case GT -> actualDouble > expectedDouble;
                        case GTE -> actualDouble >= expectedDouble;
                        case LT -> actualDouble < expectedDouble;
                        case LTE -> actualDouble <= expectedDouble;
                        default -> false;
                    };
                }
            }
            case IN -> {
                List<Object> expectedList = validateListOperand(expected, "IN");
                yield expectedList.stream().anyMatch(val -> {
                    if (val instanceof Number expectedNum) {
                        return evaluateNumericComparison(Operator.EQ, actual, expectedNum);
                    }
                    return false;
                });
            }
            case NIN -> {
                List<Object> expectedList = validateListOperand(expected, "NIN");
                yield expectedList.stream().noneMatch(val -> {
                    if (val instanceof Number expectedNum) {
                        return evaluateNumericComparison(Operator.EQ, actual, expectedNum);
                    }
                    return false;
                });
            }
            default -> throw new UnsupportedOperationException("Numeric comparison not supported for operator: " + op);
        };
    }

    /**
     * Evaluates a boolean comparison between two given boolean values using the specified operator.
     *
     * @param op       the comparison operator to be applied (e.g., EQ for equals, NE for not equals)
     * @param actual   the actual boolean value to be evaluated
     * @param expected the expected boolean value to compare against
     * @return true if the comparison satisfies the operator's condition, otherwise false
     * @throws UnsupportedOperationException if the provided operator is not supported for boolean comparisons
     */
    private static boolean evaluateBooleanComparison(Operator op, boolean actual, Object expected) {
        return switch (op) {
            case EQ -> {
                if (expected instanceof Boolean expectedBool) {
                    yield actual == expectedBool;
                } else {
                    yield false;
                }
            }
            case NE -> {
                if (expected instanceof Boolean expectedBool) {
                    yield actual != expectedBool;
                } else {
                    yield true;
                }
            }
            case IN -> {
                List<Object> expectedList = validateListOperand(expected, "IN");
                yield expectedList.contains(actual);
            }
            case NIN -> {
                List<Object> expectedList = validateListOperand(expected, "NIN");
                yield !expectedList.contains(actual);
            }
            default -> throw new UnsupportedOperationException("Boolean comparison not supported for operator: " + op);
        };
    }

    /**
     * Evaluates a null comparison using the specified operator.
     *
     * @param op     the comparison operator to be applied (e.g., EQ for equals, NE for not equals)
     * @param isNull a boolean value indicating whether the field being evaluated is null
     * @return true if the comparison satisfies the operator's condition, otherwise false
     * @throws UnsupportedOperationException if the operator is not supported for null comparisons
     */
    private static boolean evaluateNullComparison(Operator op, boolean isNull) {
        return switch (op) {
            case EQ -> isNull;  // Field is null
            case NE -> !isNull; // Field is not null
            case EXISTS -> !isNull; // Field exists (is not null)
            default -> throw new UnsupportedOperationException("Null comparison not supported for operator: " + op);
        };
    }

    /**
     * Reads a BSON value and converts it to the corresponding BqlValue.
     *
     * @param reader the BSON reader positioned at the field to read
     * @return the BqlValue representation of the BSON field, or null if unsupported type
     */
    private static BqlValue readBsonValue(BsonReader reader) {
        return switch (reader.getCurrentBsonType()) {
            case STRING -> new StringVal(reader.readString());
            case INT32 -> new Int32Val(reader.readInt32());
            case INT64 -> new Int64Val(reader.readInt64());
            case DOUBLE -> new DoubleVal(reader.readDouble());
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
     * Checks if a BSON value matches any of the expected values in a list.
     *
     * @param reader         the BSON reader positioned at the field to check
     * @param expectedValues the list of expected values to match against
     * @return true if the BSON value matches any expected value
     */
    private static boolean matchesAnyExpectedValue(BsonReader reader, List<?> expectedValues) {
        BqlValue actualValue = readBsonValue(reader);
        if (actualValue == null) {
            return false;
        }

        return expectedValues.stream().anyMatch(expected -> {
            if (expected instanceof BqlValue expectedBqlValue) {
                return compareValues(actualValue, expectedBqlValue);
            }
            return false;
        });
    }

    /**
     * Evaluates IN and NIN operations by comparing document field value against a list of expected values.
     *
     * @param reader         the BSON reader positioned at the field to evaluate
     * @param expectedValues the list of expected values to compare against
     * @return true if the field value satisfies the IN/NIN condition
     */
    private static boolean evaluateInOperation(BsonReader reader, List<?> expectedValues) {
        return matchesAnyExpectedValue(reader, expectedValues);
    }

    /**
     * Compares two BqlValue objects for equality, handling all supported types.
     *
     * @param value1 the first BqlValue to compare
     * @param value2 the second BqlValue to compare
     * @return true if the values are equal, false otherwise
     */
    private static boolean compareValues(BqlValue value1, BqlValue value2) {
        if (value1.getClass() != value2.getClass()) {
            return false;
        }

        return switch (value1) {
            case StringVal(String str1) -> str1.equals(((StringVal) value2).value());
            case Int32Val(int int1) -> int1 == ((Int32Val) value2).value();
            case Int64Val(long long1) -> long1 == ((Int64Val) value2).value();
            case DoubleVal(double double1) -> Double.compare(double1, ((DoubleVal) value2).value()) == 0;
            case BooleanVal(boolean bool1) -> bool1 == ((BooleanVal) value2).value();
            case NullVal ignored -> true; // Both are NullVal due to class check above
            case TimestampVal(long ts1) -> ts1 == ((TimestampVal) value2).value();
            case DateTimeVal(long dt1) -> dt1 == ((DateTimeVal) value2).value();
            case BinaryVal(byte[] binary1) -> Arrays.equals(binary1, ((BinaryVal) value2).value());
            case VersionstampVal(Versionstamp vs1) ->
                    Arrays.equals(vs1.getBytes(), ((VersionstampVal) value2).value().getBytes());
            case Decimal128Val(BigDecimal dec1) -> dec1.compareTo(((Decimal128Val) value2).value()) == 0;
            default -> false;
        };
    }

    /**
     * Evaluates array-based comparisons for operators like IN, NIN, ALL, and SIZE.
     *
     * @param reader         the BSON reader positioned at an array field
     * @param op             the comparison operator to be applied
     * @param expectedValues the expected array values for comparison
     * @return true if the array comparison satisfies the operator's condition, otherwise false
     */
    private static boolean evaluateArrayComparison(BsonReader reader, Operator op, List<BqlValue> expectedValues) {
        if (reader.getCurrentBsonType() != BsonType.ARRAY) {
            reader.skipValue();
            // Only arrays have size
            // ALL requires an array
            return false;
        }

        return switch (op) {
            case IN -> {
                // Check if any array element matches any expected value
                reader.readStartArray();
                boolean found = false;
                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT && !found) {
                    found = matchesAnyExpectedValue(reader, expectedValues);
                }
                reader.readEndArray();
                yield found;
            }
            case SIZE -> {
                // Check if array size matches expected size
                reader.readStartArray();
                int actualSize = 0;
                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    reader.skipValue();
                    actualSize++;
                }
                reader.readEndArray();

                if (!expectedValues.isEmpty() && expectedValues.getFirst() instanceof Int32Val(int expectedSize)) {
                    yield actualSize == expectedSize;
                } else {
                    yield false;
                }
            }
            case ALL -> {
                // Check if array contains all expected values
                reader.readStartArray();
                List<BqlValue> foundValues = new ArrayList<>();

                while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
                    BqlValue elementValue = readBsonValue(reader);
                    if (elementValue != null) {
                        foundValues.add(elementValue);
                    }
                }
                reader.readEndArray();

                // Check if all expected values are found in the array
                yield expectedValues.stream().allMatch(expectedValue ->
                        foundValues.stream().anyMatch(foundValue ->
                                compareValues(expectedValue, foundValue)
                        )
                );
            }
            default -> {
                reader.skipValue();
                yield false;
            }
        };
    }
}