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

import org.bson.BsonType;
import org.bson.BsonValue;

import java.math.BigDecimal;

/**
 * Lossless numeric widening rules for BSON types.
 * <p>
 * Allowed widening paths:
 * <ul>
 *   <li>INT32 to INT64</li>
 *   <li>INT32 to DOUBLE</li>
 *   <li>INT32 to DECIMAL128</li>
 *   <li>INT64 to DECIMAL128</li>
 *   <li>DOUBLE to DECIMAL128</li>
 * </ul>
 * <p>
 * INT64 to DOUBLE is explicitly forbidden because 64-bit integers
 * exceed double's 53-bit mantissa, causing silent precision loss.
 *
 * <h3>Common type resolution and cost</h3>
 * <p>
 * {@link #commonType} does NOT always promote to DECIMAL128. It returns the
 * cheapest lossless common type for each pair:
 * <pre>
 *   INT32 + INT32 -> INT32 (identity, no conversion)
 *   INT32 + INT64 -> INT64 (cheap cast)
 *   INT32 + DOUBLE -> DOUBLE (cheap cast)
 *   INT32 + DECIMAL128 -> DECIMAL128 (BigDecimal allocation)
 *   INT64 + INT64 -> INT64 (identity, no conversion)
 *   INT64 + DOUBLE -> DECIMAL128 (BigDecimal allocation — unavoidable, INT64→DOUBLE is lossy)
 *   DOUBLE and DOUBLE -> DOUBLE (identity, no conversion)
 *   DOUBLE + DECIMAL128→ DECIMAL128 (BigDecimal allocation)
 * </pre>
 * <p>
 * DECIMAL128 (BigDecimal) is only used when one side is already DECIMAL128,
 * or for INT64 vs DOUBLE where it is the only lossless common representation.
 * Callers in PredicateEvaluator and TypeBracketComparator check for same-type
 * first and use primitive comparison in that fast path, so widening is only
 * invoked for actual cross-type comparisons.
 */
public final class NumericWidening {

    private NumericWidening() {
    }

    /**
     * Returns true if the given BSON type is a numeric type (INT32, INT64, DOUBLE, DECIMAL128).
     */
    public static boolean isNumericBsonType(BsonType type) {
        return type == BsonType.INT32 || type == BsonType.INT64 ||
                type == BsonType.DOUBLE || type == BsonType.DECIMAL128;
    }

    /**
     * Returns true if a value of type {@code from} can be losslessly widened to type {@code to}.
     * Same-type pairs return true (identity widening). Non-numeric types always return false.
     */
    public static boolean canWiden(BsonType from, BsonType to) {
        if (from == to) {
            return true;
        }
        if (!isNumericBsonType(from) || !isNumericBsonType(to)) {
            return false;
        }
        return switch (from) {
            case INT32 -> to == BsonType.INT64 || to == BsonType.DOUBLE || to == BsonType.DECIMAL128;
            case INT64 -> to == BsonType.DECIMAL128; // INT64 -> DOUBLE is lossy, forbidden
            case DOUBLE -> to == BsonType.DECIMAL128;
            default -> false;
        };
    }

    /**
     * Widens a BsonValue to the target type and returns the Java object suitable for
     * FoundationDB Tuple packing. Returns null if widening is not allowed.
     */
    public static Object widenValue(BsonValue value, BsonType targetType) {
        BsonType sourceType = value.getBsonType();
        if (!canWiden(sourceType, targetType)) {
            return null;
        }
        if (sourceType == targetType) {
            return extractNumericValue(value);
        }
        return switch (sourceType) {
            case INT32 -> widenInt32(value.asInt32().getValue(), targetType);
            case INT64 -> widenInt64(value.asInt64().getValue(), targetType);
            case DOUBLE -> widenDouble(value.asDouble().getValue(), targetType);
            default -> null;
        };
    }

    private static Object widenInt32(int value, BsonType targetType) {
        return switch (targetType) {
            case INT64 -> (long) value;
            case DOUBLE -> (double) value;
            case DECIMAL128 -> BigDecimal.valueOf(value);
            default -> null;
        };
    }

    private static Object widenInt64(long value, BsonType targetType) {
        if (targetType == BsonType.DECIMAL128) {
            return BigDecimal.valueOf(value);
        }
        return null;
    }

    private static Object widenDouble(double value, BsonType targetType) {
        if (targetType == BsonType.DECIMAL128) {
            return BigDecimal.valueOf(value);
        }
        return null;
    }

    /**
     * Returns the common lossless comparison type for two numeric types.
     * Returns null if either type is non-numeric.
     */
    public static BsonType commonType(BsonType a, BsonType b) {
        if (!isNumericBsonType(a) || !isNumericBsonType(b)) {
            return null;
        }
        if (a == b) {
            return a;
        }
        // Order-independent: sort so that a <= b in the widening hierarchy
        BsonType lo = wideningOrder(a) <= wideningOrder(b) ? a : b;
        BsonType hi = (lo == a) ? b : a;

        return switch (lo) {
            case INT32 -> hi; // INT32 can widen to anything: INT64, DOUBLE, DECIMAL128
            case INT64 -> {
                // INT64 + DOUBLE -> DECIMAL128 (INT64->DOUBLE is lossy)
                if (hi == BsonType.DOUBLE) {
                    yield BsonType.DECIMAL128;
                }
                yield BsonType.DECIMAL128; // INT64 + DECIMAL128
            }
            case DOUBLE -> BsonType.DECIMAL128; // DOUBLE + DECIMAL128
            default -> null;
        };
    }

    /**
     * Promotes a Java numeric value to the target BSON type's Java representation.
     * Used for compare-time widening where both sides need to be promoted to a common type.
     *
     * @param value      the Java value (Integer, Long, Double, or BigDecimal)
     * @param sourceType the original BSON type of the value
     * @param targetType the target BSON type to promote to
     * @return the promoted Java value, or null if promotion is not possible
     */
    public static Comparable<?> promoteToComparable(Object value, BsonType sourceType, BsonType targetType) {
        if (sourceType == targetType) {
            return (Comparable<?>) value;
        }
        return switch (targetType) {
            case INT64 -> {
                if (sourceType == BsonType.INT32) {
                    yield ((Integer) value).longValue();
                }
                yield null;
            }
            case DOUBLE -> {
                if (sourceType == BsonType.INT32) {
                    yield ((Integer) value).doubleValue();
                }
                yield null;
            }
            case DECIMAL128 -> switch (sourceType) {
                case INT32 -> BigDecimal.valueOf((Integer) value);
                case INT64 -> BigDecimal.valueOf((Long) value);
                case DOUBLE -> BigDecimal.valueOf((Double) value);
                default -> null;
            };
            default -> null;
        };
    }

    /**
     * Extracts a Java numeric value from a BsonValue.
     */
    public static Object extractNumericValue(BsonValue value) {
        return switch (value.getBsonType()) {
            case INT32 -> value.asInt32().getValue();
            case INT64 -> value.asInt64().getValue();
            case DOUBLE -> value.asDouble().getValue();
            case DECIMAL128 -> value.asDecimal128().decimal128Value().bigDecimalValue();
            default -> null;
        };
    }

    private static int wideningOrder(BsonType type) {
        return switch (type) {
            case INT32 -> 0;
            case INT64 -> 1;
            case DOUBLE -> 2;
            case DECIMAL128 -> 3;
            default -> -1;
        };
    }
}
