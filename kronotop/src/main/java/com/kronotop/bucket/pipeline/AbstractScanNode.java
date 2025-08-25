package com.kronotop.bucket.pipeline;

import com.apple.foundationdb.tuple.Versionstamp;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.IndexDefinition;
import org.bson.BsonType;

import java.math.BigDecimal;
import java.util.List;

public abstract class AbstractScanNode extends AbstractPipelineNode implements ScanNode {
    private final List<IndexScanPredicate> children;
    private final IndexDefinition index;

    protected AbstractScanNode(int id, IndexDefinition index, List<IndexScanPredicate> children) {
        super(id);
        this.index = index;
        this.children = children;
    }

    @Override
    public List<IndexScanPredicate> predicates() {
        return children;
    }

    @Override
    public IndexDefinition index() {
        return index;
    }

    /**
     * Creates a BqlValue from a raw index value based on the BSON type.
     */
    BqlValue createBqlValueFromIndexValue(Object value, BsonType bsonType) {
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