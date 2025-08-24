package com.kronotop.bucket.executor;

import com.apple.foundationdb.Transaction;
import com.kronotop.bucket.DefaultIndexDefinition;
import com.kronotop.bucket.bql.ast.*;
import com.kronotop.bucket.index.IndexDefinition;

import java.util.List;

public abstract class AbstractDataFlowNode implements PipelineNode {
    private final List<PipelineNode> children;

    protected AbstractDataFlowNode(List<PipelineNode> children) {
        this.children = children;
    }

    @Override
    public List<PipelineNode> children() {
        return children;
    }

    @Override
    public final void execute(Transaction tr, PipelineContext ctx) {
        run(tr, ctx);
    }

    protected abstract void run(Transaction tr, PipelineContext ctx);

    void validateIndexOperandType(IndexDefinition definition, Object operand) {
        boolean typeMatches = switch (definition.bsonType()) {
            case STRING -> operand instanceof StringVal;
            case INT32 -> operand instanceof Int32Val;
            case INT64 -> operand instanceof Int64Val;
            case DOUBLE -> operand instanceof DoubleVal;
            case BOOLEAN -> operand instanceof BooleanVal;
            case BINARY -> {
                // Special case: _id index accepts VersionstampVal even though it's BINARY type
                if (operand instanceof VersionstampVal) {
                    yield DefaultIndexDefinition.ID.selector().equals(definition.selector());
                } else {
                    yield operand instanceof BinaryVal;
                }
            }
            case DATE_TIME -> operand instanceof DateTimeVal;
            case TIMESTAMP -> operand instanceof TimestampVal;
            case DECIMAL128 -> operand instanceof Decimal128Val;
            case NULL -> operand instanceof NullVal;
            default -> false;
        };
        if (!typeMatches) {
            throw new IllegalStateException("BSONType of the index (" + definition.bsonType() + ") doesn't match with PhysicalFilter's operand type (" + operand.getClass().getSimpleName() + ")");
        }
    }
}
