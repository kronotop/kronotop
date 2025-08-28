package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.bql.ast.*;
import org.bson.BsonType;

import java.math.BigDecimal;
import java.util.Objects;

public abstract class AbstractTransactionAwareNode extends AbstractPipelineNode implements TransactionAwareNode {

    protected AbstractTransactionAwareNode(int id) {
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