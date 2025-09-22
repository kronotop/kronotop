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
     * Creates a BqlValue from a raw index value based on the BSON type.
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