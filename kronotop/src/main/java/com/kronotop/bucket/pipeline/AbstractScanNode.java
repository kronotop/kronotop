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
import com.kronotop.bucket.bql.ast.*;
import org.bson.BsonType;

import java.math.BigDecimal;
import java.util.Objects;

public abstract class AbstractScanNode extends AbstractPipelineNode implements ScanNode {

    protected AbstractScanNode(int id) {
        super(id);
    }

    /**
     * Converts a raw FoundationDB index value to a typed BqlValue for query processing.
     *
     * <p>Extracts the index value from the secondary index key structure
     * ({@code BqlValue | Versionstamp}) at tuple position 1, and wraps it in the appropriate
     * BqlValue subtype based on the BSON type metadata from the index definition.
     *
     * <p><b>Type Conversions:</b>
     * <ul>
     *   <li>{@code INT32}: FoundationDB stores as long, converted to int</li>
     *   <li>{@code DECIMAL128}: Stored as string, parsed to BigDecimal</li>
     *   <li>{@code BINARY}: Returns {@link VersionstampVal} for primary index (_id),
     *       {@link BinaryVal} for regular binary indexes</li>
     * </ul>
     *
     * <p>Used by scan nodes to construct checkpoint bounds and evaluate predicates
     * during index traversal.
     *
     * @param value    the raw index value extracted from tuple position 1
     * @param bsonType the BSON type from the index definition
     * @return the typed BqlValue wrapper for query operations
     * @throws IllegalArgumentException if the BSON type is not supported for indexing
     * @see RangeScanNode
     * @see IndexScanNode
     */
    BqlValue createBqlValueFromIndexValue(Object value, BsonType bsonType) {
        if (Objects.isNull(value)) {
            return new NullVal();
        }
        return switch (bsonType) {
            case STRING -> new StringVal((String) value);
            case INT32 -> new Int32Val(((Long) value).intValue()); // Index stores INT32 as long
            case INT64 -> new Int64Val((Long) value);
            case DOUBLE -> new DoubleVal((Double) value);
            case BOOLEAN -> new BooleanVal((Boolean) value);
            case DATE_TIME -> new DateTimeVal((Long) value);
            case TIMESTAMP -> new TimestampVal((Long) value);
            case DECIMAL128 -> new Decimal128Val(new BigDecimal((String) value));
            case BINARY -> {
                if (value instanceof Versionstamp) {
                    // For _id index, create a VersionstampVal instead of BinaryVal
                    yield new VersionstampVal((Versionstamp) value);
                } else {
                    // For regular binary indexes
                    yield new BinaryVal((byte[]) value);
                }
            }
            case NULL -> new NullVal();
            default -> throw new IllegalArgumentException("Unsupported BSON type for index value: " + bsonType);
        };
    }
}